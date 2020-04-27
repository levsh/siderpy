__all__ = ['SiderPyError', 'RedisError', 'LOG', 'REDIS_PORT', 'CONNECT_TIMEOUT', 'POOL_SIZE', 'Redis', 'RedisPool']
__version__ = '0.1'

import asyncio
import collections
import contextlib
import functools
import logging
import numbers
import os
import ssl
import typing as tp


hiredis = None
if not os.environ.get('SIDERPY_DISABLE_HIREDIS'):
    try:
        import hiredis
    except ImportError:
        pass


LOG = logging.getLogger(__name__)
LOG.setLevel('INFO')

logging.basicConfig()

REDIS_PORT = 6379
CONNECT_TIMEOUT = 30
POOL_SIZE = 4


class SiderPyError(Exception):
    pass


class RedisError(SiderPyError):
    pass


class Protocol:

    __slots__ = ('_reader', '_unparsed', '_parser', '_ready', 'feed', 'gets', 'has_data')

    def __init__(self):
        if hiredis is None:
            self._reader = None
            self._unparsed = b''
            self._parser = self._parse()
            next(self._parser)
            self._ready = collections.deque()
            self.feed = self._feed
            self.gets = self._gets
            self.has_data = self._has_data
        else:
            self._reader = hiredis.Reader()
            self.feed = self._reader.feed
            self.gets = self._gets_hiredis
            self.has_data = self._reader.has_data

    def __str__(self):
        return '<{}.{} hiredis={} [{}]>'.format(
                self.__class__.__module__, self.__class__.__name__, bool(self._reader), hex(id(self)))

    def __repr__(self):
        return self.__str__()

    def reset(self):
        if self._reader:
            self._reader = hiredis.Reader()
        else:
            self._ready = []
            self._unparsed = None

    def make_cmd(self, cmd_name: str, cmd_args: tp.Union[tuple, list]) -> bytearray:
        buf = bytearray()
        buf.extend(b'*%d\r\n$%d\r\n%s\r\n' % (len(cmd_args) + 1, len(cmd_name), cmd_name.encode()))
        for arg in cmd_args:
            if isinstance(arg, (str, numbers.Number)):
                arg = str(arg).encode()
            buf.extend(b'$%d\r\n%s\r\n' % (len(arg), arg))
        return buf

    def _has_data(self) -> bool:
        return bool(self._ready) or bool(self._unparsed)

    def _parse(self):
        bytestring = b''
        sub_parser = None
        sub_parser_map = {
            b'+': self._parse_string(),
            b'-': self._parse_error(),
            b':': self._parse_integer(),
            b'$': self._parse_bulk_string(),
            b'*': self._parse_array()}
        for v in sub_parser_map.values():
            next(v)  # pylint: disable=stop-iteration-return
        data = None
        while True:
            bytestring = yield data, bytestring
            if sub_parser is None:
                sub_parser = sub_parser_map[bytestring[:1]]
            data, bytestring = sub_parser.send(bytestring)
            if data is not False:
                sub_parser = None

    def _feed(self, bytestring: bytes):
        data = None
        self._unparsed += bytestring
        while self._unparsed:
            data, self._unparsed = self._parser.send(self._unparsed)
            if data is False:
                break
            self._ready.append(data)

    def _gets(self):
        if self._ready:
            data = self._ready.popleft()
            if isinstance(data, Exception):
                raise data
            return data
        return False

    def _gets_hiredis(self):
        data = self._reader.gets()
        if isinstance(data, Exception):
            raise RedisError(data) from data
        return data

    def _parse_string(self):
        data = None
        while True:
            bytestring = yield data
            data = bytestring[1:].split(b'\r\n', 1)
            if len(data) != 2:
                data = False, bytestring

    def _parse_error(self):
        data = None
        while True:
            bytestring = yield data
            data = bytestring[1:].split(b'\r\n', 1)
            if len(data) != 2:
                data = False, bytestring
            else:
                data = RedisError(data[0].decode()), data[1]

    def _parse_integer(self):
        data = None
        while True:
            bytestring = yield data
            data = bytestring[1:].split(b'\r\n', 1)
            if len(data) != 2:
                data = False, bytestring
            else:
                data = int(data[0].decode()), data[1]

    def _parse_bulk_string(self):
        data = None
        while True:
            bytestring = yield data
            data = bytestring[1:].split(b'\r\n', 1)
            if len(data) != 2:
                data = False, bytestring
                continue
            strlen, remain = int(data[0].decode()), data[1]
            if strlen == -1:
                data = None, remain
                continue
            if len(remain) - 2 < strlen:
                data = False, bytestring
                continue
            data = remain[:strlen], remain[strlen + 2:]

    def _parse_array(self):
        data = None
        out = []
        while True:
            bytestring = yield data
            data = bytestring[1:].split(b'\r\n', 1)
            if len(data) != 2:
                data = False, bytestring
                continue
            number_of_elements, remain = int(data[0].decode()), data[1]
            if number_of_elements == -1:
                data = None, remain
                continue
            if number_of_elements == 0:
                data = [], remain
                continue
            sub_parser = self._parse()
            next(sub_parser)  # pylint: disable=stop-iteration-return
            while len(out) != number_of_elements:
                if not remain:
                    remain = yield False, remain
                    continue
                data, remain = sub_parser.send(remain)
                if data is False:
                    remain = yield False, remain
                    continue
                out.append(data)
            data = out, remain
            out = []


class _Pool:

    __slots__ = ('_factory', '_size', '_test', '_on_create', '_queue', '_used')

    def __init__(self,
                 factory: tp.Coroutine,
                 size: int = POOL_SIZE,
                 test: tp.Callable = None,
                 on_create: tp.Callable = None):
        self._factory = factory
        self._size = size
        self._test = test
        self._on_create = on_create
        self._queue = asyncio.LifoQueue(maxsize=self._size)
        self._used = set()
        for _ in range(self._size):
            self._queue.put_nowait(None)

    def __str__(self):
        return '<{}.{} size {}, available {} [{}]>'.format(
                self.__class__.__module__, self.__class__.__name__, self._size, self._queue.qsize(), hex(id(self)))

    def __repr__(self):
        return self.__str__()

    async def get(self, timeout: float = None):
        async def call():
            item = await self._queue.get()
            if item is None or self._test and not self._test(item):
                try:
                    item = await self._factory()
                    if callable(self._on_create):
                        self._on_create(item)
                except (asyncio.CancelledError, Exception) as e:
                    self._queue.put_nowait(None)
                    raise e
            self._used.add(item)
            LOG.debug('%s get %s', self, item)
            return item
        return await asyncio.wait_for(call(), timeout)

    def put(self, item):
        if item in self._used:
            LOG.debug('%s put %s', self, item)
            self._used.remove(item)
            self._queue.put_nowait(item)

    @contextlib.asynccontextmanager
    async def get_item(self, timeout: float = None):
        item = await self.get(timeout=timeout)
        try:
            yield item
        finally:
            self.put(item)

    def close(self, func: tp.Callable):
        for item in self._used:
            func(item)
        while self._queue.qsize():
            item = self._queue.get_nowait()
            if item:
                func(item)


class Redis:

    __slots__ = ('_host', '_port', '_connect_timeout', '_read_timeout', '_write_timeout', '_ssl_ctx',
                 '_pool', '_proto', '_pipeline', '_buf', '_subscriber', '_subscriber_cb', '_subscriber_channels')

    def __init__(self,
                 host: str,
                 port: int = REDIS_PORT,
                 connect_timeout: float = CONNECT_TIMEOUT,
                 timeout: tp.Union[float, tuple, list] = None,
                 ssl_ctx: ssl.SSLContext = None):
        self._host = host
        self._port = port
        self._connect_timeout = connect_timeout
        if isinstance(timeout, (tuple, list)):
            self._read_timeout, self._write_timeout = timeout
        else:
            self._read_timeout = timeout
            self._write_timeout = timeout
        self._ssl_ctx = ssl_ctx
        self._pool = _Pool(self._create_connection,
                           size=1,
                           test=lambda conn: not conn[1].is_closing(),
                           on_create=self._on_connection_create)
        self._proto = Protocol()
        self._pipeline = False
        self._buf = []
        self._subscriber = None
        self._subscriber_cb = None
        self._subscriber_channels = set()

    def __str__(self):
        return '<{}.{} ({}, {}) [{}]>'.format(
                self.__class__.__module__, self.__class__.__name__, self._host, self._port, hex(id(self)))

    def __repr__(self):
        return self.__str__()

    def close_connection(self):
        self._pool.close(lambda conn: conn[1].close())

    def pipeline_on(self):
        if self._subscriber:
            raise SiderPyError('Client in subscribe mode')
        self._pipeline = True

    def pipeline_off(self):
        self._pipeline = False

    @contextlib.contextmanager
    def pipeline(self):
        self._pipeline = True
        try:
            yield
        finally:
            self._pipeline = False

    async def pipeline_execute(self):
        if self._subscriber:
            raise SiderPyError('Client in subscribe mode')
        res = await self._execute_bytestring(b''.join(self._buf))
        self._buf = []
        return res

    def pipeline_clear(self):
        self._buf = []

    async def _create_connection(self) -> tp.Tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        handshake_timeout = None
        if self._ssl_ctx:
            handshake_timeout = self._connect_timeout
        return await asyncio.open_connection(host=self._host,
                                             port=self._port,
                                             ssl=self._ssl_ctx,
                                             ssl_handshake_timeout=handshake_timeout)

    # pylint: disable=unused-argument
    def _on_connection_create(self, conn: tp.Tuple[asyncio.StreamReader, asyncio.StreamWriter]):
        if self._subscriber:
            self._subscriber.cancel()
            self._subscriber = None
            self._subscriber_cb = None
            self._subscriber_channels = set()

    async def _read(self, r: asyncio.StreamReader) -> tp.List[bytes]:
        out = []
        proto = self._proto
        feed = proto.feed
        gets = proto.gets
        has_data = proto.has_data
        while True:
            raw = await r.read(1024)
            # pylint: disable=protected-access
            LOG.debug('%s fd=%s read %s', self, r._transport.get_extra_info('socket').fileno(), raw)
            if raw == b'' and r.at_eof():
                raise ConnectionError
            feed(raw)
            while True:
                data = gets()
                if data is False:
                    break
                out.append(data)
                if not has_data():
                    return out
            if not has_data():
                return out

    async def _execute_bytestring(self, bytestring: tp.Union[bytearray, bytes]):
        async with self._pool.get_item(timeout=self._connect_timeout) as (r, w):
            LOG.debug('%s fd=%s write %s', self, w.get_extra_info('socket').fileno(), bytestring)
            try:
                w.write(bytestring)
                await asyncio.wait_for(w.drain(), self._write_timeout)
                if self._subscriber:
                    return
                data = await asyncio.wait_for(self._read(r), self._read_timeout)
            except (asyncio.CancelledError, Exception) as e:
                LOG.warning('%s closing connection %s', self, w)
                w.close()
                await w.wait_closed()
                raise e
            if len(data) == 1:
                data = data[0]
            if isinstance(data, RedisError):
                raise data
            return data

    async def _execute(self, cmd_name: str, *args):
        bytestring = self._proto.make_cmd(cmd_name, args)
        if not self._pipeline:
            return await self._execute_bytestring(bytestring)
        self._buf.append(bytestring)

    async def _subscribe(self, cmd_name: str, callback: tp.Coroutine, *channels):
        if not callable(callback):
            raise TypeError(f"'{callback}' is not callable")

        self._subscriber_cb = callback

        if self._subscriber is None:
            conn = await self._pool.get()
            self._pool.put(conn)

            async def subscriber():
                while True:
                    messages = await self._read(conn[0])
                    for message in messages:
                        if message[0] in {b'subscribe', b'psubscribe'}:
                            self._subscriber_channels.add(message[1].decode())
                        elif message[0] in {b'unsubscribe', b'punsubscribe'}:
                            self._subscriber_channels.remove(message[1].decode())
                        else:
                            try:
                                await callback(message)
                            except Exception as e:
                                LOG.exception(e)
                    if not self._subscriber_channels:
                        self._subscriber = None
                        self._subscriber_cb = None
                        return

            self._subscriber = asyncio.create_task(subscriber())

        return await self._execute(cmd_name, *channels)

    async def _unsubscribe(self, cmd_name: str, *channels):
        await self._execute(cmd_name, *channels)

    def __getattr__(self, attr_name: str):
        cmd_name = {
            'delete': 'del',
            'execute': 'exec',
        }.get(attr_name, attr_name)
        if cmd_name in {'subscribe', 'psubscribe'}:
            return functools.partial(self._subscribe, cmd_name)
        elif cmd_name in {'unsubscribe', 'punsubscribe'}:
            return functools.partial(self._unsubscribe, cmd_name)
        return functools.partial(self._execute, cmd_name)


class RedisPool:

    __slots__ = ('_host', '_port', '_connect_timeout', '_read_timeout', '_write_timeout', '_ssl_ctx', '_pool')

    def __init__(self,
                 host: str,
                 port: int = REDIS_PORT,
                 connect_timeout: float = CONNECT_TIMEOUT,
                 timeout: tp.Union[float, tuple, list] = None,
                 size: int = None,
                 ssl_ctx: ssl.SSLContext = None):
        self._host = host
        self._port = port
        self._connect_timeout = connect_timeout
        if isinstance(timeout, (tuple, list)):
            self._read_timeout, self._write_timeout = timeout
        else:
            self._read_timeout = timeout
            self._write_timeout = timeout
        self._ssl_ctx = ssl_ctx
        self._pool = _Pool(self._factory, size=size)

    def __str__(self):
        return '<{}.{} ({}, {})) {} [{}]>'.format(
                self.__class__.__module__, self.__class__.__name__, self._host, self._port, self._pool, hex(id(self)))

    async def _factory(self):
        return Redis(self._host,
                     port=self._port,
                     connect_timeout=self._connect_timeout,
                     timeout=(self._read_timeout, self._write_timeout),
                     ssl_ctx=self._ssl_ctx)

    @contextlib.asynccontextmanager
    async def get_redis(self, timeout: float = None):
        redis = await self._pool.get(timeout=timeout)
        try:
            yield redis
            if redis._subscriber:  # pylint: disable=protected-access
                try:
                    # pylint: disable=protected-access
                    await asyncio.wait_for(asyncio.wait({redis._subscriber}), timeout)
                except asyncio.TimeoutError:
                    redis.close_connection()
                    raise SiderPyError('Closising Redis instance because active pub/sub')
        finally:
            self._pool.put(redis)

    def close_connections(self):
        self._pool.close(lambda redis: redis.close_connection())

    async def _execute(self, attr_name: str, *args):
        async with self._pool.get_item(timeout=self._connect_timeout) as redis:
            return await getattr(redis, attr_name)(*args)

    def __getattr__(self, attr_name: str):
        return functools.partial(self._execute, attr_name)
