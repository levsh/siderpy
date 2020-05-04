__all__ = ['SiderPyError', 'RedisError', 'LOG', 'CONNECT_TIMEOUT', 'TIMEOUT', 'POOL_SIZE', 'Redis', 'RedisPool']
__version__ = '0.1'

import asyncio
import collections
import contextlib
import functools
import logging
import numbers
import os
import ssl
import sys
import typing as tp
import urllib

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


CONNECT_TIMEOUT = None
TIMEOUT = None
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


class Redis:
    """Class representing a single connection to a Redis server.
    Connection to the server is established automatically during first request.

    Examples:

        >>> import siderpy
        >>> redis = siderpy.Redis('redis://username:password@localhost:6379?db=0')
        >>> await redis.ping()
        >>> ...
        >>> redis.close_connection()

    Manual auth and logical database selection

        >>> await redis.auth('password')
        >>> await redis.auth('user', 'password')  # when Redis ACLs are used
        >>> await redis.select(0)

    multi/exec

        >>> await redis.multi()
        >>> await redis.set('key', 'value')
        >>> ...
        >>> await redis.execute()
        >>> await redis.close_connection()
    """

    __slots__ = ('_scheme', '_host', '_port', '_username', '_password', '_db', '_decoder',
                 '_connect_timeout', '_read_timeout', '_write_timeout', '_ssl_ctx', '_handshake_timeout',
                 '_get_connection', '_conn', '_conn_lock', '_proto', '_pipeline', '_pipeline_buf', '_future',
                 '_cmd_count', '_listener', '_subscription', '_subscription_queue', '_anext_lock')

    def __init__(self,
                 address: str = 'redis://localhost:6379?db=0',
                 connect_timeout: float = CONNECT_TIMEOUT,
                 timeout: tp.Union[float, tuple, list] = TIMEOUT,
                 decoder: tp.Callable = None,
                 ssl_ctx: ssl.SSLContext = None):
        """
        Args:
            address (:obj:`str`, optional): Redis address to connect as uri:

                * `redis://[USERNAME][:PASSWORD@]HOST[:PORT][?db=DATABASE]`

                * `redis+unix://[USERNAME][:PASSWORD@]SOCKET_PATH[?db=DATABASE]`

                default: `redis://localhost:6379?db=0`
            connect_timeout (:obj:`float`, optional): Timeout used to get initialized :py:class:`Redis` instance
                and as a :obj:`ssl_handshake_timeout` argument for :obj:`asyncio.open_connection` call.
            timeout (:obj:`float`, optional): Timeout used for read and write operations.
                It is possibly to specify separately values for read and write.

                Example:

                    >>> Redis(timeout=(read_timeout, write_timeout))

                If common or read timeout is specified it will affect all
                Redis blocking read commands such as blpop, etc.
                For example, this code will raise :obj:`asyncio.TimeoutError` after one second
                though a timeout of zero for blpop command can be used to block indefinitely.

                    >>> redis = siderpy.Redis(timeout=1)
                    >>> await redis.blpop('empty_list', 0)  # asyncio.TimeoutError exception
                    >>>                                     # will occur here after 1 second

                To avoid this situation set read timeout to :obj:`None`.

                    >>> redis = siderpy.Redis(timeout=(None, 15))
                    >>> await redis.blpop('empty_list', 0)  # will block indefinitely

            decoder (:py:class:`typing.Callable`, optional): function to decode raw bytes from Redis.

            ssl_ctx (:py:class:`ssl.SSLContext`, optional): SSL context object to enable SSL(TLS).
        """
        self._scheme = None
        self._username = None
        self._password = None
        self._host = None
        self._port = 6379
        self._db = 0
        parsed = urllib.parse.urlparse(address)
        if parsed.scheme is None:
            raise ValueError('Scheme is required')
        if parsed.scheme not in ('redis', 'redis+unix'):
            raise ValueError('Unsupported scheme %s' % parsed.scheme)
        self._scheme = parsed.scheme
        if parsed.username:
            self._username = parsed.username
        if parsed.password:
            self._password = parsed.password
        if parsed.hostname is None:
            raise ValueError('hostname or unix path is required')
        self._host = parsed.hostname
        if parsed.port is not None:
            self._port = parsed.port
        if parsed.path:
            raise ValueError('Uri path param is not supported')
        if parsed.query:
            self._db = dict([param_str.split('=', 1) for param_str in parsed.query.split('&')]).get('db')
        self._decoder = None
        self._connect_timeout = connect_timeout
        if isinstance(timeout, (tuple, list)):
            self._read_timeout, self._write_timeout = timeout
        else:
            self._read_timeout = timeout
            self._write_timeout = timeout
        self._ssl_ctx = ssl_ctx
        self._handshake_timeout = None
        if self._ssl_ctx:
            self._handshake_timeout = self._connect_timeout
        self._conn = None
        self._conn_lock = asyncio.Lock()
        self._proto = Protocol()
        self._pipeline = False
        self._pipeline_buf = []
        self._future = None
        self._cmd_count = None
        self._listener = None
        self._subscription = False
        self._subscription_queue = asyncio.Queue()
        self._anext_lock = asyncio.Lock()
        if self._scheme == 'redis':
            self._get_connection = functools.partial(
                    asyncio.open_connection,
                    host=self._host,
                    port=self._port,
                    ssl=self._ssl_ctx,
                    ssl_handshake_timeout=self._handshake_timeout)
        elif self._scheme == 'redis+unix':
            self._get_connection = functools.partial(
                    asyncio.open_unix_connection,
                    path=self._hostname,
                    ssl=self._ssl_ctx,
                    ssl_handshake_timeout=self._handshake_timeout)

    def __str__(self):
        if self._scheme == 'redis':
            return '<{}.{} ({}, {}) [{}]>'.format(
                self.__class__.__module__, self.__class__.__name__, self._host, self._port, hex(id(self)))
        return '<{}.{} ({}) [{}]>'.format(
            self.__class__.__module__, self.__class__.__name__, os.path.basename(self._host), hex(id(self)))

    def __repr__(self):
        return self.__str__()

    def close_connection(self):
        """Close established connection"""
        if self._listener:
            self._listener.cancel()
        if self._conn is not None:
            self._conn[1].close()
            self._conn = None

    def pipeline_on(self):
        """Enable pipeline mode. In this mode, all commands are saved to the internal pipeline buffer
        and not executed until `pipeline_execute` method is invoked directly."""
        if self._listener:
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
        if self._listener:
            raise SiderPyError('Connection in PubSub mode')
        if self._pipeline_buf:
            res = await self._execute_cmd_list(self._pipeline_buf)
            self._pipeline_buf = []
            return res

    def pipeline_clear(self):
        """Clear internal pipeline buffer"""
        self._pipeline_buf = []

    async def execute_cmd(self, cmd_name: str, *args):
        """Execute command

        Args:
            cmd_name (:obj:`str`, optional): Redis command:

        Example:

            >>> result = await redis.execute_cmd('get', 'key')
        """
        if not self._pipeline:
            async with self._conn_lock:
                return await self._execute_cmd_list([(cmd_name, args)])
        self._pipeline_buf.append((cmd_name, args))

    async def _create_connection(self):
        # LOG.debug('%s create connection', self)
        if self._listener:
            self._listener.cancel()
            self._listener = None
        if self._connect_timeout is None:
            self._conn = await self._get_connection()
        else:
            self._conn = await asyncio.wait_for(self._get_connection(), self._connect_timeout)
        cmd_list = []
        if self._password is not None:
            if self._username is not None:
                cmd_list.append(['auth', (self._username, self._password)])
            else:
                cmd_list.append(['auth', (self._password,)])
        if self._db is not None and self._db != 0:
            cmd_list.append(['select', (self._db,)])
        if cmd_list:
            try:
                await self._execute_cmd_list(cmd_list)
            except Exception:
                self.close_connection()
                raise

    async def _execute_cmd_list(self, cmd_list: list):
        if self._conn is None or self._conn[1].is_closing():
            await self._create_connection()
        array = bytearray()
        for cmd_name, args in cmd_list:
            array.extend(self._proto.make_cmd(cmd_name, args))
            if not self._subscription and cmd_name in {'subscribe', 'psubscribe'}:
                self._subscription = True
        if self._subscription and self._listener is None:
            self._listener = asyncio.create_task(self._listen())
        r, w = self._conn
        # LOG.debug('%s fd=%s write %s', self, w.get_extra_info('socket').fileno(), array)
        if self._listener is not None:
            self._future = asyncio.get_event_loop().create_future()
        try:
            w.write(array)
            if self._write_timeout is not None:
                await asyncio.wait_for(w.drain(), self._write_timeout)
            else:
                await w.drain()
            if self._listener is None:
                data = []
                proto = self._proto
                cmd_count = len(cmd_list)
                while len(data) < cmd_count:
                    if self._read_timeout is None:
                        raw = await r.read(2048)
                    else:
                        raw = await asyncio.wait_for(r.read(2048), self._read_timeout)
                    if raw == b'' and r.at_eof():
                        raise ConnectionError
                    # # pylint: disable=protected-access
                    # LOG.debug('%s fd=%s read %s', self, r._transport.get_extra_info('socket').fileno(), raw)
                    proto.feed(raw)
                    while True:
                        item = proto.gets()
                        if item is False:
                            break
                        data.append(item)
                        if not proto.has_data():
                            break
            else:
                self._cmd_count = len(cmd_list)
                if self._read_timeout is None:
                    data = await self._future
                else:
                    data = await asyncio.wait_for(self._future, self._read_timeout)
            if len(data) == 1:
                data = data[0]
        except (asyncio.CancelledError, Exception) as e:
            LOG.debug('%s close connection %s', self, w)
            w.close()
            await w.wait_closed()
            self._conn = None
            raise e
        if isinstance(data, Exception):
            raise RedisError(data)
        return data

    async def _listen(self):
        try:
            self._subscription_queue = asyncio.Queue()
            proto = self._proto
            r, w = self._conn
            incomming = []
            while self._subscription:
                data = []
                while len(data) < self._cmd_count:
                    if self._read_timeout is None:
                        raw = await r.read(2048)
                    else:
                        raw = await asyncio.wait_for(r.read(2048), self._read_timeout)
                    if raw == b'' and r.at_eof():
                        raise ConnectionError
                    # # pylint: disable=protected-access
                    # LOG.debug('%s fd=%s read %s', self, r._transport.get_extra_info('socket').fileno(), raw)
                    proto.feed(raw)
                    while True:
                        item = proto.gets()
                        if item is False:
                            break
                        data.append(item)
                        if not proto.has_data():
                            break
                for item in data:
                    if self._subscription:
                        if item[0] in {b'message', b'pmessage'}:
                            self._subscription_queue.put_nowait(item)
                        else:
                            incomming.append(item)
                            if item[0] in {b'unsubscribe', b'punsubscribe'} and item[2] == 0:
                                self._subscription = False
                                self._subscription_queue.put_nowait(StopAsyncIteration())
                    else:
                        incomming.append(data)
                if incomming:
                    self._future.set_result(incomming)
                    incomming = []
        except asyncio.CancelledError:
            self._subscription_queue.put_nowait(StopAsyncIteration())
        except (asyncio.TimeoutError, ConnectionError, OSError) as e:
            LOG.debug('%s %s %s', self, e.__class__.__name__, e)
            self._subscription_queue.put_nowait(e)
        except Exception as e:
            LOG.exception('%s %s %s', self, e.__class__.__name__, e)
            self._subscription_queue.put_nowait(e)
        finally:
            self._listener = None

    def __aiter__(self):
        return self

    async def __anext__(self):
        async with self._anext_lock:
            if not self._subscription and self._subscription_queue.qsize() == 0:
                raise StopAsyncIteration()
            item = await self._subscription_queue.get()
            if isinstance(item, Exception):
                raise item
            return item

    async def delete(self, *args):
        return await self.execute_cmd('del', *args)

    async def execute(self):
        return await self.execute_cmd('exec')

    def __getattr__(self, attr_name: str):
        return functools.partial(self.execute_cmd, attr_name)


class Pool:

    __slots__ = ('_factory', '_size', '_queue', '_used')

    def __init__(self, factory: tp.Coroutine, size: int = POOL_SIZE):
        self._factory = factory
        self._size = size
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
        if self._queue.qsize():
            item = self._queue.get_nowait()
        else:
            item = await self._queue.get()
        if item is None:
            try:
                item = await self._factory()
            except (asyncio.CancelledError, Exception) as e:
                self._queue.put_nowait(None)
                raise e
        self._used.add(item)
        # LOG.debug('%s get %s', self, item)
        return item

    def put(self, item):
        # LOG.debug('%s put %s', self, item)
        if item in self._used:
            self._used.remove(item)
            self._queue.put_nowait(item)

    @contextlib.asynccontextmanager
    async def get_item(self, timeout: float = None):
        if timeout is None:
            item = await self.get()
        else:
            item = await asyncio.wait_for(self.get(), timeout)
        try:
            yield item
        finally:
            # LOG.debug('%s put %s', self, item)
            self._used.remove(item)
            self._queue.put_nowait(item)

    def close(self, func: tp.Callable):
        for item in self._used:
            func(item)
        while self._queue.qsize():
            item = self._queue.get_nowait()
            if item is not None:
                func(item)


class RedisPool:
    """Class representing a pool of connections to a Redis server

        >>> import siderpy
        >>> pool = siderpy.RedisPool('redis://localhost:6379/0', size=10)
        >>> await pool.ping()
        >>> ...
        >>> pool.close_connections()
    """

    __slots__ = ('_address', '_connect_timeout', '_read_timeout', '_write_timeout', '_size', '_ssl_ctx', '_pool')

    def __init__(self,
                 address: str = 'redis://localhost:6379/0',
                 connect_timeout: float = CONNECT_TIMEOUT,
                 timeout: tp.Union[float, tuple, list] = TIMEOUT,
                 size: int = POOL_SIZE,
                 pool_cls=Pool,
                 ssl_ctx: ssl.SSLContext = None):
        """
        Args:
            address (:obj:`str`, optional): same as :obj:`address` argument for :py:class:`Redis`.
            connect_timeout (:obj:`float`, optional): same as :obj:`connect_timeout` argument for :obj:`Redis`.
            timeout (:obj:`float`, optional): same as :obj:`timeout` argument for :py:class:`Redis`.
            size (:obj:`int`, optional): Pool size.
            ssl_ctx (:py:class:`ssl.SSLContext`, optional): same as :obj:`ssl_ctx` argument for :py:class:`Redis`.
        """
        self._address = address
        self._connect_timeout = connect_timeout
        if isinstance(timeout, (tuple, list)):
            self._read_timeout, self._write_timeout = timeout
        else:
            self._read_timeout = timeout
            self._write_timeout = timeout
        self._size = size
        self._ssl_ctx = ssl_ctx
        self._pool = pool_cls(self._factory, size=size)

    def __str__(self):
        return '<{}.{} {} [{}]>'.format(
            self.__class__.__module__, self.__class__.__name__, self._pool, hex(id(self)))

    async def _factory(self):
        return Redis(address=self._address,
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
            if redis._listener:  # pylint: disable=protected-access
                try:
                    # pylint: disable=protected-access
                    await asyncio.wait_for(asyncio.wait({redis._listener}), timeout)
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
