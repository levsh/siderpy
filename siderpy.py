import asyncio
import collections
import contextlib
import functools
import logging
import numbers
import ssl
import typing as tp

try:
    import hiredis
except ImportError:
    hiredis = None


LOG = logging.getLogger(__name__)
LOG.setLevel('INFO')

logging.basicConfig()

REDIS_PORT = 6379
CONN_TIMEOUT = 30
POOL_SIZE = 4


class RedisError(Exception):
    pass


class Protocol:

    def __init__(self):
        if hiredis is None:
            self._reader = None
            self._ready = collections.deque()
            self._remain = None
            self._parser_map = {
                b'+': self._parse_string,
                b'-': self._parse_error,
                b':': self._parse_integer,
                b'$': self._parse_bulk_string,
                b'*': self._parse_array}
            self.feed = self._feed
            self.gets = self._gets
            self.has_data = self._has_data
        else:
            self._reader = hiredis.Reader()
            self.feed = self._reader.feed
            self.gets = self._gets_hiredis
            self.has_data = self._reader.has_data

    def __str__(self):
        if self._reader:
            return f'{self.__class__.__name__} {self._reader}'
        return f'{self.__class__.__name__}'

    def __repr__(self):
        return self.__str__()

    def reset(self):
        if self._reader:
            self._reader = hiredis.Reader()
        else:
            self._ready.clear()
            self._remain = None

    def make_cmd(self, cmd_name: str, cmd_args: list) -> bytearray:
        buf = bytearray()
        buf.extend(b'*%d\r\n$%d\r\n%s\r\n' % (len(cmd_args) + 1, len(cmd_name), cmd_name.encode()))
        for arg in cmd_args:
            if isinstance(arg, (str, numbers.Number)):
                arg = str(arg).encode()
            buf.extend(b'$%d\r\n%s\r\n' % (len(arg), arg))
        return buf

    def _has_data(self) -> bool:
        return bool(self._ready) or bool(self._remain)

    def _feed(self, bytestring: bytes):
        if self._remain:
            bytestring = self._remain + bytestring
        while bytestring:
            data, bytestring = self._parser_map[bytestring[:1]](bytestring)
            if data is False:
                return
            self._ready.append(data)
            self._remain = bytestring

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

    def _parse_string(self, bytestring: bytes) -> tp.Tuple[bytes]:
        data, remain = bytestring[1:].split(b'\r\n', 1)
        return data, remain

    def _parse_error(self, bytestring: bytes) -> tp.Tuple[bytes]:
        data, remain = bytestring[1:].split(b'\r\n', 1)
        return RedisError(data.decode()), remain

    def _parse_integer(self, bytestring: bytes) -> tp.Tuple[bytes]:
        data, remain = bytestring[1:].split(b'\r\n', 1)
        return int(data.decode()), remain

    def _parse_bulk_string(self, bytestring: bytes) -> tp.Tuple[bytes]:
        string_len, remain = bytestring[1:].split(b'\r\n', 1)
        string_len = int(string_len.decode())
        if string_len == -1:
            return None, remain
        if len(remain) - 2 < string_len:
            return False, bytestring
        return remain[:string_len], remain[string_len + 2:]

    def _parse_array(self, bytestring: bytes) -> tp.Tuple[bytes]:
        out = []
        number_of_elements, remain = bytestring[1:].split(b'\r\n', 1)
        number_of_elements = int(number_of_elements.decode())
        if number_of_elements == -1:
            return None, remain
        if number_of_elements == 0:
            return [], remain
        while len(out) != number_of_elements:
            bytestring = remain
            element, remain = self._parser_map[bytestring[:1]](bytestring)
            if bytestring == remain:
                return None, bytestring
            out.append(element)
        return out, remain


class Pool:

    def __init__(self, factory: tp.Coroutine, size: int=0, test: tp.Callable=None, on_create: tp.Callable=None):
        self.factory = factory
        self.size = size
        self.test = test
        self.on_create = on_create
        self._queue = asyncio.LifoQueue(maxsize=self.size)
        for _ in range(self.size):
            self._queue.put_nowait(None)

    def __str__(self):
        return '<{}.{} size {}, available {}>'.format(
                self.__class__.__module__, self.__class__.__name__, self.size, self._queue.qsize())

    def __repr__(self):
        return self.__str__()

    async def get(self, timeout: float=None):
        async def call():
            item = await self._queue.get()
            if item is None or self.test and not self.test(item):
                try:
                    item = await self.factory()
                    if callable(self.on_create):
                        self.on_create(item)
                except Exception:
                    self._queue.put_nowait(None)
                    raise
            return item
        return await asyncio.wait_for(call(), timeout)

    def put(self, item):
        self._queue.put_nowait(item)

    @contextlib.asynccontextmanager
    async def get_item(self, timeout: float=None):
        item = await self.get(timeout=timeout)
        try:
            yield item
        finally:
            self.put(item)


class Executor:

    __slots__ = ['proto', 'cmd_bytestrings']

    def __init__(self):
        self.proto = Protocol()
        self.cmd_bytestrings = []

    async def read(self, conn: tp.Tuple[asyncio.StreamReader, asyncio.StreamWriter]) -> tp.List[bytes]:
        out = []
        proto = self.proto
        feed = proto.feed
        gets = proto.gets
        has_data = proto.has_data
        while True:
            raw = await conn[0].read(1024)
            feed(raw)
            data = None
            while True:
                data = gets()
                if data is False:
                    break
                out.append(data)
                if not has_data():
                    return out
            if not has_data():
                return out

    def append(self, cmd_name: str, args: tuple):
        self.cmd_bytestrings.append(self.proto.make_cmd(cmd_name, args))

    async def execute_buf(self, conn: tp.Tuple[asyncio.StreamReader, asyncio.StreamWriter]):
        cmd_count = len(self.cmd_bytestrings)
        cmd_bytestring = b''.join(self.cmd_bytestrings)
        conn[1].write(cmd_bytestring)
        self.cmd_bytestrings = []
        await conn[1].drain()
        data = []
        while len(data) < cmd_count:
            data += await self.read(conn)
        if len(data) == 1:
            data = data[0]
        if isinstance(data, RedisError):
            raise data
        return data

    async def execute(self,
                      conn: tp.Tuple[asyncio.StreamReader, asyncio.StreamWriter],
                      cmd_name: str,
                      args: tuple,
                      read: bool=True):
        cmd_bytestring = self.proto.make_cmd(cmd_name, args)
        conn[1].write(cmd_bytestring)
        self.cmd_bytestrings = []
        await conn[1].drain()
        if not read:
            return
        data = await self.read(conn)
        if len(data) == 1:
            data = data[0]
        if isinstance(data, RedisError):
            raise data
        return data


class Redis:

    __slots__ = ['_host', '_port', '_timeout', '_ssl_ctx', '_pool', '_executor', '_pipeline',
                 '_subscriber', '_subscriber_cb', '_subscriber_channels']

    def __init__(self,
                 host: str,
                 port: int=REDIS_PORT,
                 timeout: float=CONN_TIMEOUT,
                 ssl_ctx: ssl.SSLContext=None):
        self._host = host
        self._port = port
        self._timeout = timeout
        self._ssl_ctx = ssl_ctx
        self._pool = Pool(self._create_connection, size=1,
                          test=lambda conn: not conn[1].is_closing(),
                          on_create=self._on_connection_create)
        self._executor = Executor()
        self._pipeline = False
        self._subscriber = None
        self._subscriber_cb = None
        self._subscriber_channels = set()

    async def _create_connection(self) -> tp.Tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        handshake_timeout = None
        if self._ssl_ctx:
            handshake_timeout = self._timeout
        return await asyncio.open_connection(host=self._host,
                                             port=self._port,
                                             ssl=self._ssl_ctx,
                                             ssl_handshake_timeout=handshake_timeout)

    def _on_connection_create(self, conn: tp.Tuple[asyncio.StreamReader, asyncio.StreamWriter]):
        if self._subscriber:
            self._subscriber.cancel()
            self._subscriber = None
            self._subscriber_cb = None
            self._subscriber_channels = set()

    @contextlib.asynccontextmanager
    async def pipeline(self):
        self._pipeline = True
        resp = []
        try:
            yield resp
        finally:
            self._pipeline = False

        async with self._pool.get_item(timeout=self._timeout) as conn:
            resp.extend(await self._executor.execute_buf(conn))

    async def _execute(self, cmd_name: str, *args):
        if self._pipeline:
            self._executor.append(cmd_name, args)
            return

        async def call():
            async with self._pool.get_item() as conn:
                return await self._executor.execute(conn, cmd_name, args, read=self._subscriber is None)
        return await asyncio.wait_for(call(), self._timeout)

        # async with self._pool.get_item() as conn:
        #     return await self._executor.execute(conn, cmd_name, args, read=self._subscriber is None)

    async def _subscribe(self, cmd_name: str, callback: tp.Coroutine, *channels):
        if not callable(callback):
            raise TypeError(f"'{callback}' is not callable")

        self._subscriber_cb = callback

        if self._subscriber is None:
            conn = await self._pool.get()
            self._pool.put(conn)

            async def subscriber():
                while True:
                    messages = await self._executor.read(conn)
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

    def __init__(self,
                 host: str,
                 port: int=REDIS_PORT,
                 timeout: float=CONN_TIMEOUT,
                 pool_size: int=POOL_SIZE,
                 ssl_ctx: ssl.SSLContext=None):
        self._host = host
        self._port = port
        self._timeout = timeout
        self._ssl_ctx = ssl_ctx
        self._pool = Pool(self._factory, size=pool_size)

    async def _factory(self):
        return Redis(self._host, port=self._port, timeout=self._timeout, ssl_ctx=self._ssl_ctx)

    @contextlib.asynccontextmanager
    async def get_one(self):
        redis = await asyncio.wait_for(self._pool.get(), self._timeout)
        try:
            yield redis
            if redis._subscriber:
                try:
                    await asyncio.wait_for(asyncio.wait({redis._subscriber}), self._timeout)
                except asyncio.TimeoutError:
                    raise RedisError('Returning into pool instance with active pub/sub')
        finally:
            self._pool.put(redis)

    async def _execute(self, attr_name: str, *args):
        async def call():
            async with self._pool.get_item() as redis:
                return await getattr(redis, attr_name)(*args)
        return await asyncio.wait_for(call(), self._timeout)

    def __getattr__(self, attr_name: str):
        return functools.partial(self._execute, attr_name)
