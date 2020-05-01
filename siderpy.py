__all__ = ['SiderPyError', 'RedisError', 'LOG', 'REDIS_PORT', 'CONNECT_TIMEOUT', 'POOL_SIZE', 'Redis', 'RedisPool']
__version__ = '0.1'

import asyncio
import collections
import contextlib
import functools
import logging
import numbers
import ssl
import sys
import typing as tp

try:
    import hiredis
except ImportError:
    hiredis = None


log_frmt = logging.Formatter('%(asctime)s %(name)s %(levelname)s %(message)s')
log_hndl = logging.StreamHandler(stream=sys.stderr)
log_hndl.setFormatter(log_frmt)
LOG = logging.getLogger(__name__)
LOG.addHandler(log_hndl)
LOG.setLevel('INFO')


REDIS_PORT = 6379
CONNECT_TIMEOUT = None
POOL_SIZE = 4


class SiderPyError(Exception):
    """Base exception"""
    pass


class RedisError(SiderPyError):
    """Redis error Exception"""
    pass


class Protocol:

    __slots__ = ('_reader', '_unparsed', '_parser', '_ready', '_map', 'feed', 'gets', 'has_data')

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
            self.gets = self._reader.gets
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
            self._ready.clear()
            self._unparsed = b''

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
            bytestring = yield data
            if sub_parser is None:
                sub_parser = sub_parser_map[bytestring[:1]]
            data = sub_parser.send(bytestring)
            if data[0] is not False:
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
            return self._ready.popleft()
        return False

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


class Pool:

    __slots__ = ('_factory', '_size', '_item_test', '_on_create', '_queue', '_used')

    def __init__(self,
                 factory: tp.Coroutine,
                 size: int = POOL_SIZE,
                 item_test: tp.Callable = None,
                 on_create: tp.Callable = None):
        self._factory = factory
        self._size = size
        self._item_test = item_test
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

    async def get(self):
        item = await self._queue.get()
        if item is None or self._item_test and not self._item_test(item):
            try:
                item = await self._factory()
                if callable(self._on_create):
                    self._on_create(item)
            except (asyncio.CancelledError, Exception) as e:
                self._queue.put_nowait(None)
                raise e
        self._used.add(item)
        # LOG.debug('%s get %s', self, item)
        return item

    def put(self, item):
        if item in self._used:
            # LOG.debug('%s put %s', self, item)
            self._used.remove(item)
            self._queue.put_nowait(item)

    @contextlib.asynccontextmanager
    async def get_item(self, timeout: float = None):
        if timeout is not None:
            item = await asyncio.wait_for(self.get(), timeout)
        else:
            item = await self.get()
        try:
            yield item
        finally:
            self.put(item)

    def close(self, func: tp.Callable):
        for item in self._used:
            func(item)
        while self._queue.qsize():
            item = self._queue.get_nowait()
            if item is not None:
                func(item)


class Redis:
    """Class representing a single connection to a Redis server.
    Connection to the server is established automatically during the first request.

    Examples:

        >>> import siderpy
        >>> pool = siderpy.Redis('localhost', port=6379)
        >>> await redis.ping()
        >>> redis.close_connection()

    To select Redis logical database call select method:

        >>> await redis.select(0)

    MULTI/EXEC

        >>> await redis.multi()
        >>> await redis.set('key', 'value')
        >>> ...
        >>> await redis.execute()
    """

    __slots__ = ('_host', '_port', '_connect_timeout', '_read_timeout', '_write_timeout', '_ssl_ctx',
                 '_conn', '_conn_lock', '_proto', '_pipeline', '_buf', '_incomming_queue',
                 '_subscriber_task', '_subscriber_callbacks')

    def __init__(self,
                 host: str,
                 port: int = REDIS_PORT,
                 connect_timeout: float = CONNECT_TIMEOUT,
                 timeout: tp.Union[float, tuple, list] = None,
                 ssl_ctx: ssl.SSLContext = None):
        """
        Args:
            host (:obj:`str`): Redis server hostname or IP address.
            port (:obj:`int`, optional): Redis server port.
            connect_timeout (:obj:`float`, optional): Timeout used to get initialized :py:class:`Redis` instance
                and as a :obj:`ssl_handshake_timeout` argument for :obj:`asyncio.open_connection` call.
            timeout (:obj:`float`, optional): Timeout used for read and write operations.
                It is possibly to specify separately values for read and write.

                Example:

                    >>> Redis('localhost', timeout=(read_timeout, write_timeout))

                If common or read timeout is specified it will affect all
                Redis blocking read commands such as blpop, etc.
                For example, this code will raise :obj:`asyncio.TimeoutError` after one second
                though a timeout of zero for blpop command can be used to block indefinitely.

                    >>> redis = siderpy.Redis('localhost', timeout=1)
                    >>> await redis.blpop('empty_list', 0)  # asyncio.TimeoutError exception
                    >>>                                     # will occur here after 1 second

                To avoid this situation set read timeout to :obj:`None`.

                    >>> redis = siderpy.Redis('localhost', timeout=(None, 15))
                    >>> await redis.blpop('empty_list', 0)  # will block indefinitely

            ssl_ctx (:py:class:`ssl.SSLContext`, optional): SSL context object to enable SSL(TLS).
        """
        self._host = host
        self._port = port
        self._connect_timeout = connect_timeout
        if isinstance(timeout, (tuple, list)):
            self._read_timeout, self._write_timeout = timeout
        else:
            self._read_timeout = timeout
            self._write_timeout = timeout
        self._ssl_ctx = ssl_ctx
        self._conn = None
        self._conn_lock = asyncio.Lock()
        self._proto = Protocol()
        self._pipeline = False
        self._buf = []
        self._incomming_queue = asyncio.Queue()
        self._subscriber_task = None
        self._subscriber_callbacks = {b'message': {}, b'pmessage': {}}

    def __str__(self):
        return '<{}.{} ({}, {}) [{}]>'.format(
            self.__class__.__module__, self.__class__.__name__, self._host, self._port, hex(id(self)))

    def __repr__(self):
        return self.__str__()

    def close_connection(self):
        """Close established connection"""
        if self._conn is not None:
            self._conn[1].close()
            self._conn = None

    def pipeline_on(self):
        """Enable pipeline mode. In this mode, all commands are saved to the internal pipeline buffer
        and not executed until the pipe_execute method is invoked directly."""
        if self._subscriber_task:
            raise SiderPyError('Client in subscribe mode')
        self._pipeline = True

    def pipeline_off(self):
        """Disable pipeline mode"""
        self._pipeline = False

    @contextlib.contextmanager
    def pipeline(self):
        """Pipeline mode contextmanager

        Example:

            >>> with redis.pipeline():
            >>>     await redis.set('key1', 'value2')
            >>>     await redis.set('key2', 'value2')
            >>>     await redis.mget('key1', 'key2')
            >>> result = await redis.pipeline_execute()

        Also it's possible to resume or execute pipeline later, for example:

            >>> with redis.pipeline():
            >>>     await redis.set('key1', 'value2')
            >>> # pause pipeline, do other stuff
            >>> ...
            >>> # continue with pipeline
            >>> with redis.pipeline():
            >>>     await redis.set('key2', 'value2')
            >>> result = await redis.pipeline_execute()
        """
        self.pipeline_on()
        try:
            yield
        finally:
            self.pipeline_off()

    async def pipeline_execute(self):
        """Execute pipeline buffer"""
        if self._subscriber_task:
            raise SiderPyError('Client in subscribe mode')
        if self._buf:
            res = await self._execute_cmd_list(self._buf)
            self._buf = []
            return res

    def pipeline_clear(self):
        """Clear internal pipeline buffer"""
        self._buf = []

    async def _create_connection(self) -> tp.Tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        handshake_timeout = None
        if self._ssl_ctx:
            handshake_timeout = self._connect_timeout
        LOG.debug('%s create connection', self)
        aw = asyncio.open_connection(host=self._host,
                                     port=self._port,
                                     ssl=self._ssl_ctx,
                                     ssl_handshake_timeout=handshake_timeout)
        if self._connect_timeout is None:
            return await aw
        return await asyncio.wait_for(aw, self._connect_timeout)

    # pylint: disable=unused-argument
    def _on_connection_create(self, conn: tp.Tuple[asyncio.StreamReader, asyncio.StreamWriter]):
        if self._subscriber_task:
            self._subscriber_task.cancel()
            self._subscriber_task = None
            self._subscriber_callbacks[b'message'].clear()
            self._subscriber_callbacks[b'pmessage'].clear()

    async def _read(self, r: asyncio.StreamReader, count: int = 1) -> tp.List[bytes]:
        out = []
        proto = self._proto
        while len(out) < count:
            raw = await r.read(2048)
            # pylint: disable=protected-access
            LOG.debug('%s fd=%s read %s', self, r._transport.get_extra_info('socket').fileno(), raw)
            if raw == b'' and r.at_eof():
                raise ConnectionError
            proto.feed(raw)
            while True:
                data = proto.gets()
                if data is False:
                    break
                out.append(data)
                if not proto.has_data():
                    break
        return out

    async def _execute_cmd_list(self, cmd_list: list):
        async with self._conn_lock:
            if self._conn is None or self._conn[1].is_closing():
                self._conn = await self._create_connection()
            r, w = self._conn
            cmd_count = len(cmd_list)
            bytestring = b''.join(cmd_list)
            LOG.debug('%s fd=%s write %s', self, w.get_extra_info('socket').fileno(), bytestring)
            try:
                w.write(bytestring)
                if self._write_timeout is not None:
                    await asyncio.wait_for(w.drain(), self._write_timeout)
                else:
                    await w.drain()
                if self._subscriber_task is None:
                    aw = self._read(r, count=cmd_count)
                else:
                    aw = self._incomming_queue.get()
                if self._read_timeout is None:
                    data = await aw
                else:
                    data = await asyncio.wait_for(aw, self._read_timeout)
                if len(data) == 1:
                    data = data[0]
                if isinstance(data, Exception):
                    raise RedisError(data)
                return data
            except (asyncio.CancelledError, Exception) as e:
                LOG.warning('%s close connection %s', self, w)
                w.close()
                await w.wait_closed()
                self._conn = None
                raise e

    async def _execute(self, cmd_name: str, *args):
        cmd = self._proto.make_cmd(cmd_name, args)
        if not self._pipeline:
            return await self._execute_cmd_list([cmd])
        self._buf.append(cmd)

    def _callbacks_iter(self):
        for _, values in self._subscriber_callbacks.items():
            for _, callback in values.items():
                yield callback

    async def _listen(self):
        try:
            while True:
                try:
                    data = await asyncio.wait_for(self._read(self._conn[0]), self._read_timeout)
                except Exception as e:
                    LOG.warning('%s %s', e.__class__.__name__, e)
                    for callback in self._callbacks_iter():
                        try:
                            await callback(e)
                        except Exception as ee:
                            LOG.error('%s %s', ee.__class__.__name__, ee)
                    raise e
                if isinstance(data, Exception):
                    await self._incomming_queue.put(data)
                    continue
                incomming_data = []
                for message in data:
                    if message[0] in {b'message', b'pmessage'}:
                        try:
                            await self._subscriber_callbacks[message[0]][message[1]](message)
                        except Exception as e:
                            LOG.error('%s %s', e.__class__.__name__, e)
                        continue
                    if message[0] in {b'subscribe', b'psubscribe'} or message[1] == b'':
                        incomming_data.append(message)
                        continue
                    if message[0] == b'unsubscribe':
                        del self._subscriber_callbacks[b'message'][message[1]]
                        incomming_data.append(message)
                    elif message[0] == b'punsubscribe':
                        del self._subscriber_callbacks[b'pmessage'][message[1]]
                        incomming_data.append(message)
                    else:
                        raise ValueError('Unknown pubsub message type %s' % message[0])
                if incomming_data:
                    await self._incomming_queue.put(incomming_data)
                if not (self._subscriber_callbacks[b'message'] or self._subscriber_callbacks[b'pmessage']):
                    self._subscriber_task = None
                    return
        except Exception as e:
            LOG.exception(e)

    async def _subscribe(self, cmd_name: str, callback: tp.Callable, *channels):
        key = {'subscribe': b'message', 'psubscribe': b'pmessage'}[cmd_name]
        self._subscriber_callbacks[key].update(
            {channel.encode() if isinstance(channel, str) else channel: callback for channel in channels})
        if self._subscriber_task is None:
            async with self._conn_lock:
                if self._conn is None or self._conn[1].is_closing():
                    self._conn = await self._create_connection()
            self._subscriber_task = asyncio.create_task(self._listen())
        new_channels = tuple(channel for channel in channels
                             if channel not in set(self._subscriber_callbacks[key].values()))
        return await self._execute(cmd_name, *tuple(new_channels))

    async def _unsubscribe(self, cmd_name: str, *channels):
        return await self._execute(cmd_name, *channels)

    def __getattr__(self, attr_name: str):
        cmd_name = {
            'delete': 'del',
            'execute': 'exec',
        }.get(attr_name, attr_name)
        if cmd_name in {'subscribe', 'psubscribe'}:
            return functools.partial(self._subscribe, cmd_name)
        if cmd_name in {'unsubscribe', 'punsubscribe'}:
            return functools.partial(self._unsubscribe, cmd_name)
        return functools.partial(self._execute, cmd_name)


class RedisPool:
    """Class representing a pool of connections to a Redis server

        >>> import siderpy
        >>> pool = siderpy.RedisPool('localhost', port=6379, size=10)
        >>> await pool.ping()
        >>> pool.close_connections()
    """

    __slots__ = ('_host', '_port', '_connect_timeout', '_read_timeout', '_write_timeout', '_size', '_ssl_ctx', '_pool')

    def __init__(self,
                 host: str,
                 port: int = REDIS_PORT,
                 connect_timeout: float = CONNECT_TIMEOUT,
                 timeout: tp.Union[float, tuple, list] = None,
                 size: int = POOL_SIZE,
                 ssl_ctx: ssl.SSLContext = None):
        """
        Args:
            host (:obj:`str`): same as :obj:`host` argument for :py:class:`Redis`.
            port (:obj:`int`, optional): same as :obj:`port` argument for :py:class:`Redis`.
            connect_timeout (:obj:`float`, optional): same as :obj:`connect_timeout` argument for :obj:`Redis`.
            timeout (:obj:`float`, optional): same as :obj:`timeout` argument for :py:class:`Redis`.
            size (:obj:`int`, optional): Pool size.
            ssl_ctx (:py:class:`ssl.SSLContext`, optional): same as :obj:`ssl_ctx` argument for :py:class:`Redis`.
        """
        self._host = host
        self._port = port
        self._connect_timeout = connect_timeout
        if isinstance(timeout, (tuple, list)):
            self._read_timeout, self._write_timeout = timeout
        else:
            self._read_timeout = timeout
            self._write_timeout = timeout
        self._size = size
        self._ssl_ctx = ssl_ctx
        self._pool = Pool(self._factory, size=size)

    def __str__(self):
        return '<{}.{} ({}, {}) {} [{}]>'.format(
            self.__class__.__module__, self.__class__.__name__, self._host, self._port, self._pool, hex(id(self)))

    async def _factory(self):
        return Redis(self._host,
                     port=self._port,
                     connect_timeout=self._connect_timeout,
                     timeout=(self._read_timeout, self._write_timeout),
                     ssl_ctx=self._ssl_ctx)

    @contextlib.asynccontextmanager
    async def get_redis(self, timeout: float = None):
        """Context manager for getting Redis instance

        :param float timeout: Timeout to get initialized :py:class:`Redis` object

        >>> async with pool.get_redis() as redis:
        >>>     await redis.ping()
        """
        if timeout is not None:
            redis = await asyncio.wait_for(self._pool.get(), timeout)
        else:
            redis = await self._pool.get()
        try:
            yield redis
            if redis._subscriber_task:  # pylint: disable=protected-access
                try:
                    # pylint: disable=protected-access
                    await asyncio.wait_for(asyncio.wait({redis._subscriber_task}), timeout)
                except asyncio.TimeoutError:
                    redis.close_connection()
                    raise SiderPyError('Closising Redis instance because active pub/sub')
        finally:
            self._pool.put(redis)

    def close_connections(self):
        """Close all established connections"""
        self._pool.close(lambda redis: redis.close_connection())

    async def _execute(self, attr_name: str, *args):
        async with self._pool.get_item(timeout=self._connect_timeout) as redis:
            return await getattr(redis, attr_name)(*args)

    def __getattr__(self, attr_name: str):
        return functools.partial(self._execute, attr_name)
