__all__ = [
    "SiderPyError",
    "RedisError",
    "QueueClosedError",
    "LOG",
    "CONNECT_TIMEOUT",
    "TIMEOUT",
    "POOL_SIZE",
    "Redis",
    "RedisPool",
]
__version__ = "0.4.1"

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
import urllib.parse

try:
    import hiredis
except ImportError:
    hiredis = None


log_frmt = logging.Formatter("%(asctime)s %(name)s %(levelname)s %(message)s")
log_hndl = logging.StreamHandler(stream=sys.stderr)
log_hndl.setFormatter(log_frmt)
LOG = logging.getLogger(__name__)
LOG.addHandler(log_hndl)
LOG.setLevel("INFO")


CONNECT_TIMEOUT = None
TIMEOUT = None
POOL_SIZE = 4


class SiderPyError(Exception):
    """Base error"""


class RedisError(SiderPyError):
    """Redis error"""


class QueueClosedError(SiderPyError):
    """Closed PubSub queue error"""


class Protocol:

    __slots__ = ("_encoding", "_reader", "_unparsed", "_parser", "_ready", "feed", "gets", "has_data")

    def __init__(self, encoding=None, errors=None):
        self._encoding = {}
        if encoding is not None:
            self._encoding["encoding"] = encoding
        if errors is not None:
            self._encoding["errors"] = errors
        if hiredis is None:
            self._reader = None
            self._unparsed = b""
            self._parser = self._parse()
            next(self._parser)
            self._ready = collections.deque()
            self.feed = self._feed
            self.gets = self._gets
            self.has_data = self._has_data
        else:
            self._reader = hiredis.Reader(**self._encoding)
            self.feed = self._reader.feed
            self.gets = self._reader.gets
            self.has_data = self._reader.has_data

    def __str__(self):
        return "[{} hiredis={}]".format(self.__class__.__name__, bool(self._reader))

    def __repr__(self):
        return self.__str__()

    def reset(self):
        if self._reader:
            self._reader = hiredis.Reader(**self._encoding)
        else:
            self._ready.clear()
            self._unparsed = b""

    def make_cmd(self, cmd_name: str, cmd_args: tp.Union[tuple, list]) -> bytearray:
        buf = bytearray()
        buf.extend(b"*%d\r\n$%d\r\n%s\r\n" % (len(cmd_args) + 1, len(cmd_name), cmd_name.encode()))
        for arg in cmd_args:
            if isinstance(arg, (str, numbers.Number)):
                arg = str(arg).encode()
            elif not isinstance(arg, (bytes, bytearray)):
                raise TypeError(f"Wrong argument '{arg}' type {type(arg)}")
            buf.extend(b"$%d\r\n%s\r\n" % (len(arg), arg))
        return buf

    def _has_data(self) -> bool:
        return bool(self._ready) or bool(self._unparsed)

    def _parse(self):
        bytestring = b""
        sub_parser = None
        sub_parser_map = {
            b"+": self._parse_string(),
            b"-": self._parse_error(),
            b":": self._parse_integer(),
            b"$": self._parse_bulk_string(),
            b"*": self._parse_array(),
        }
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
            data = bytestring[1:].split(b"\r\n", 1)
            if len(data) != 2:
                data = False, bytestring
            if self._encoding:
                data[0] = data[0].decode(**self._encoding)

    def _parse_error(self):
        data = None
        while True:
            bytestring = yield data
            data = bytestring[1:].split(b"\r\n", 1)
            if len(data) != 2:
                data = False, bytestring
            else:
                data[0] = RedisError(data[0].decode())

    def _parse_integer(self):
        data = None
        while True:
            bytestring = yield data
            data = bytestring[1:].split(b"\r\n", 1)
            if len(data) != 2:
                data = False, bytestring
            else:
                data[0] = int(data[0].decode())

    def _parse_bulk_string(self):
        data = None
        while True:
            bytestring = yield data
            data = bytestring[1:].split(b"\r\n", 1)
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
            # fmt: off
            data = remain[:strlen], remain[strlen + 2:]
            # fmt: on
            if self._encoding:
                data[0] = data[0].decode(**self._encoding)

    def _parse_array(self):
        data = None
        out = []
        while True:
            bytestring = yield data
            data = bytestring[1:].split(b"\r\n", 1)
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


class PubSubQueue(asyncio.Queue):
    """Queue class to hold incomming messages."""

    def __init__(self, *args, **kwds):
        super().__init__(*args, **kwds)
        self._exc = None
        self._closed = False

    def close(self, exc=None):
        self._exc = exc
        self._closed = True
        while self._getters:
            getter = self._getters.popleft()
            if not getter.done():
                if exc:
                    getter.set_exception(exc)
                else:
                    getter.set_result(None)

    async def get(self):
        while self.empty():
            if self._closed:
                if self._exc:
                    raise QueueClosedError from self._exc
                raise QueueClosedError
            getter = self._loop.create_future()
            self._getters.append(getter)
            try:
                await getter
            except:  # noqa: E722
                getter.cancel()  # Just in case getter is not done yet.
                try:
                    # Clean self._getters from canceled getters.
                    self._getters.remove(getter)
                except ValueError:
                    # The getter could be removed from self._getters by a
                    # previous put_nowait call.
                    pass
                if not self.empty() and not getter.cancelled():
                    # We were woken up by put_nowait(), but can't take
                    # the call.  Wake up the next in line.
                    self._wakeup_next(self._getters)
                raise
        return self.get_nowait()

    # pylint: disable=arguments-differ
    def _put(self, *args, **kwds):
        if self._closed:
            raise QueueClosedError
        return super()._put(*args, **kwds)

    def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            if self.qsize():
                return self.get_nowait()
            return await self.get()
        except QueueClosedError:
            raise self._exc or StopAsyncIteration


class Redis:
    """Class representing a single connection to a Redis server.
    Connection to the server is established automatically during first request.

    Examples:

        >>> import siderpy
        >>> redis = siderpy.Redis('redis://username:password@localhost:6379?db=0')
        >>> await redis.ping()
        >>> ...
        >>> await redis.close()
    """

    __slots__ = (
        "_scheme",
        "_host",
        "_port",
        "_username",
        "_password",
        "_db",
        "_path",
        "_connect_timeout",
        "_read_timeout",
        "_write_timeout",
        "_ssl_ctx",
        "_handshake_timeout",
        "_get_connection",
        "_conn",
        "_conn_lock",
        "_proto",
        "_pipeline",
        "_pipeline_buf",
        "_future",
        "_cmd_count",
        "_listener",
        "_subscription",
        "_pubsub_queue_maxsize",
        "_pubsub_queue",
    )

    def __init__(
        self,
        address: str = "redis://localhost:6379?db=0",
        connect_timeout: float = CONNECT_TIMEOUT,
        timeout: tp.Union[float, tuple, list] = TIMEOUT,
        ssl_ctx: ssl.SSLContext = None,
        encoding=None,
        errors=None,
        pubsub_queue_maxsize=None,
    ):
        """
        Args:
            address (:obj:`str`, optional): The Redis server address and settings to connect as uri:

                * `redis://[USERNAME][:PASSWORD@]HOST[:PORT][?db=DATABASE]`

                * `redis+unix://[USERNAME][:PASSWORD@]SOCKET_PATH[?db=DATABASE]`

                default: `redis://localhost:6379?db=0`
            connect_timeout (:obj:`float`, optional): Timeout used to get initialized :py:class:`Redis` instance
                and as :obj:`ssl_handshake_timeout` argument for :obj:`asyncio.open_connection` call.
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

            encoding (:py:class:`str`, optional): Encoding with which to decode raw data(bytes) from Redis.
            errors (:py:class:`str`, optional): Error handling scheme to use for handling of decoding errors.
            ssl_ctx (:py:class:`ssl.SSLContext`, optional): SSL context object to enable SSL(TLS).
        """
        parsed = self.parse_address(address)
        self._scheme = parsed["scheme"]
        self._host = parsed["host"]
        self._username = parsed.get("username")
        self._password = parsed.get("password")
        self._port = parsed.get("port", 6379)
        self._db = parsed.get("db", 0)
        self._path = parsed.get("path")
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
        self._proto = Protocol(encoding=encoding, errors=errors)
        self._pipeline = False
        self._pipeline_buf = []
        self._future = None
        self._cmd_count = None
        self._listener = None
        self._subscription = False
        self._pubsub_queue_maxsize = 0
        if pubsub_queue_maxsize is not None:
            self._pubsub_queue_maxsize = pubsub_queue_maxsize
        self._pubsub_queue = PubSubQueue(maxsize=self._pubsub_queue_maxsize)
        if self._scheme == "redis":
            self._get_connection = functools.partial(
                asyncio.open_connection,
                host=self._host,
                port=self._port,
                ssl=self._ssl_ctx,
                ssl_handshake_timeout=self._handshake_timeout,
            )
        elif self._scheme == "redis+unix":
            self._get_connection = functools.partial(
                asyncio.open_unix_connection,
                path=self._path,
                ssl=self._ssl_ctx,
                ssl_handshake_timeout=self._handshake_timeout,
            )

    def __str__(self):
        if self._scheme == "redis":
            return "[{}({}, {})]".format(self.__class__.__name__, self._host, self._port)
        return "[{}({})]".format(self.__class__.__name__, os.path.basename(self._path))

    def __repr__(self):
        return self.__str__()

    @classmethod
    def parse_address(cls, address: str) -> dict:
        parsed = urllib.parse.urlparse(address)
        out = {"scheme": parsed.scheme, "host": parsed.hostname}
        if not parsed.scheme:
            raise ValueError("Scheme is required")
        if parsed.scheme == "redis":
            if not parsed.hostname:
                raise ValueError("Hostname is required")
            if parsed.path:
                raise ValueError("Path param is not supported")
        elif parsed.scheme == "redis+unix":
            if not parsed.path:
                raise ValueError("Unix socket path is required")
            out["path"] = parsed.path
        else:
            raise ValueError("Scheme is not supported %s" % parsed.scheme)
        if parsed.query:
            # pylint: disable=consider-using-dict-comprehension
            db = dict([param_str.split("=", 1) for param_str in parsed.query.split("&")]).get("db")
            if db is not None:
                if not db.isdigit():
                    raise ValueError("db param must be integer")
                out["db"] = int(db)
        if "db" not in out:
            out["db"] = 0
        if parsed.username:
            out["username"] = parsed.username
        if parsed.password:
            out["password"] = parsed.password
        out["port"] = parsed.port or 6379
        return out

    async def close(self):
        """Close established connection"""
        await self._cancel_listener()
        if self._conn is not None:
            self._conn[1].close()

    def pipeline_on(self):
        """Enable pipeline mode. In this mode, all commands are saved to the internal pipeline buffer
        until :py:meth:`pipeline_off` method is invoked directly.
        To execute stored buffer call :py:meth:`pipeline_execute`"""
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
            raise SiderPyError("Connection in PubSub mode")
        if self._pipeline_buf:
            res = await self._execute_cmd_list(self._pipeline_buf)
            self._pipeline_buf = []
            return res

    def pipeline_clear(self):
        """Clear internal pipeline buffer"""
        self._pipeline_buf = []

    async def execute_cmd(self, cmd_name: str, *args):
        """Execute Redis command

        Args:
            cmd_name (:obj:`str`, optional): Redis command name.

        Example:

            >>> result = await redis.execute_cmd('get', 'key')
        """
        if not self._pipeline:
            async with self._conn_lock:
                return await self._execute_cmd_list([(cmd_name, args)])
        self._pipeline_buf.append((cmd_name, args))

    @property
    def pubsub_queue(self):
        """Instance of :py:class:`PubSubQueue` class. Holds incomming messages."""
        return self._pubsub_queue

    async def _cancel_listener(self):
        if self._listener:
            self._listener.cancel()
            try:
                await self._listener
            except asyncio.CancelledError:
                pass

    async def _open_connection(self):
        await self._cancel_listener()
        self._proto.reset()
        if self._connect_timeout is None:
            self._conn = await self._get_connection()
        else:
            self._conn = await asyncio.wait_for(self._get_connection(), self._connect_timeout)
        cmd_list = []
        if self._password is not None:
            if self._username is not None:
                cmd_list.append(["auth", (self._username, self._password)])
            else:
                cmd_list.append(["auth", (self._password,)])
        if self._db is not None and self._db != 0:
            cmd_list.append(["select", (self._db,)])
        if cmd_list:
            try:
                await self._execute_cmd_list(cmd_list)
            except Exception:
                await self.close()
                raise

    async def _read(self):
        data = []
        r, _ = self._conn
        proto = self._proto
        while len(data) < self._cmd_count:
            if self._read_timeout is None:
                raw = await r.read(2048)
            else:
                raw = await asyncio.wait_for(r.read(2048), self._read_timeout)
            if raw == b"" and r.at_eof():
                raise ConnectionError
            proto.feed(raw)
            while True:
                item = proto.gets()
                if item is False:
                    break
                data.append(item)
                if not proto.has_data():
                    break
        return data

    async def _execute_cmd_list(self, cmd_list: list):
        if self._conn is None or self._conn[1].is_closing():
            await self._open_connection()
        array = bytearray()
        for cmd_name, args in cmd_list:
            array.extend(self._proto.make_cmd(cmd_name, args))
            if not self._subscription and cmd_name in {"subscribe", "psubscribe"}:
                self._subscription = True
        if self._subscription and self._listener is None:
            self._listener = asyncio.create_task(self._listen())
        if self._listener is not None:
            self._future = asyncio.get_event_loop().create_future()
        _, w = self._conn
        try:
            w.write(array)
            if self._write_timeout is not None:
                await asyncio.wait_for(w.drain(), self._write_timeout)
            else:
                await w.drain()
            self._cmd_count = len(cmd_list)
            if self._listener is None:
                data = await self._read()
            else:
                if self._read_timeout is None:
                    data = await self._future
                else:
                    data = await asyncio.wait_for(self._future, self._read_timeout)
            if len(data) == 1:
                data = data[0]
        except (asyncio.CancelledError, Exception) as e:
            w.close()
            await w.wait_closed()
            self._conn = None
            raise e
        if isinstance(data, Exception):
            raise RedisError(data)
        return data

    async def _listen(self):
        try:
            incomming = []
            while self._subscription:
                data = await self._read()
                for item in data:
                    if self._subscription:
                        if isinstance(item, list) and item[0] in {b"message", b"pmessage"}:
                            if not self._pubsub_queue.full():
                                self._pubsub_queue.put_nowait(item)
                            else:
                                await self._pubsub_queue.put(item)
                        else:
                            if isinstance(item, Exception):
                                incomming.append(item)
                            elif item[1] == b"":
                                incomming.append(item[0])
                            else:
                                incomming.append(item)
                                if item[0] in {b"unsubscribe", b"punsubscribe"} and item[2] == 0:
                                    self._subscription = False
                                    self._pubsub_queue.close()
                    else:
                        incomming.append(item)
                if incomming:
                    self._future.set_result(incomming)
                    incomming = []
        except asyncio.CancelledError:
            self._pubsub_queue.close()
        except (asyncio.TimeoutError, ConnectionError, OSError) as e:
            LOG.error("%s %s %s", self, e.__class__.__name__, e)
            self._pubsub_queue.close(exc=e)
        except Exception as e:
            LOG.error("%s %s %s", self, e.__class__.__name__, e)
            self._pubsub_queue.close(exc=e)
        finally:
            self._listener = None
            self._subscription = False
            self._pubsub_queue.close()
            self._pubsub_queue = PubSubQueue(maxsize=self._queue_maxsize)

    def __aiter__(self):
        return self._pubsub_queue

    async def delete(self, *args):
        """Redis `del` command"""
        return await self.execute_cmd("del", *args)

    async def execute(self):
        """Redis `exec` command"""
        return await self.execute_cmd("exec")

    def __getattr__(self, attr_name: str):
        return functools.partial(self.execute_cmd, attr_name)


class Pool:

    __slots__ = ("_factory", "_size", "_queue", "_used")

    def __init__(self, factory: tp.Coroutine, size: int = POOL_SIZE):
        self._factory = factory
        self._size = size
        self._queue = asyncio.LifoQueue(maxsize=self._size)
        self._used = set()
        for _ in range(self._size):
            self._queue.put_nowait(None)

    def __str__(self):
        return "[{} {}/{}]".format(self.__class__.__name__, self._size, self._queue.qsize())

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
        return item

    def put(self, item):
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
            self._used.remove(item)
            self._queue.put_nowait(item)

    async def close(self, func: tp.Callable):
        coros = [func(item) for item in self._used]
        while self._queue.qsize():
            item = self._queue.get_nowait()
            if item is not None:
                coros.append(func(item))
        await asyncio.gather(*coros)


class RedisPool:
    """Class representing a pool of connections to a Redis server

        >>> import siderpy
        >>> pool = siderpy.RedisPool('redis://localhost:6379?db=0', size=10)
        >>> await pool.ping()
        >>> await pool.get('key')
        >>> ...
        >>> await pool.close()

    Pool doesn't implement multi/exec and pub/sub commands.
    For performance reasons it's better to use Redis instance as command executor instead of pool itself. For example:

        >>> with pool.get_redis() as redis:
        >>>     await redis.set(...)
        >>>     await redis.get(...)
        >>>     ...
    """

    __slots__ = (
        "_address",
        "_parsed",
        "_connect_timeout",
        "_read_timeout",
        "_write_timeout",
        "_size",
        "_ssl_ctx",
        "_pool",
    )

    def __init__(
        self,
        address: str = "redis://localhost:6379?db=0",
        connect_timeout: float = CONNECT_TIMEOUT,
        timeout: tp.Union[float, tuple, list] = TIMEOUT,
        size: int = POOL_SIZE,
        pool_cls=Pool,
        ssl_ctx: ssl.SSLContext = None,
    ):
        """
        Args:
            address (:obj:`str`, optional): same as :obj:`address` argument for :py:class:`Redis`.
            connect_timeout (:obj:`float`, optional): same as :obj:`connect_timeout` argument for :obj:`Redis`.
            timeout (:obj:`float`, optional): same as :obj:`timeout` argument for :py:class:`Redis`.
            size (:obj:`int`, optional): Pool size.
            ssl_ctx (:py:class:`ssl.SSLContext`, optional): same as :obj:`ssl_ctx` argument for :py:class:`Redis`.
        """
        self._address = address
        self._parsed = Redis.parse_address(address)
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
        return "[{}({}://{}:{}?db={}){}]".format(
            self.__class__.__name__,
            self._parsed["scheme"],
            self._parsed["host"],
            self._parsed["port"],
            self._parsed["db"],
            self._pool,
        )

    def __repr__(self):
        return self.__str__()

    async def _factory(self) -> Redis:
        return Redis(
            address=self._address,
            connect_timeout=self._connect_timeout,
            timeout=(self._read_timeout, self._write_timeout),
            ssl_ctx=self._ssl_ctx,
        )

    @contextlib.asynccontextmanager
    async def get_redis(self, timeout: float = None):
        """Context manager for getting Redis instance

        :param float timeout: Timeout to get :py:class:`Redis` instance

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
                    await redis.close()
                    # pylint: disable=raise-missing-from
                    raise SiderPyError("Closising Redis instance because active pub/sub")
        finally:
            self._pool.put(redis)

    async def close(self):
        """Close all established connections"""

        async def close(redis):
            await redis.close()

        await self._pool.close(close)

    async def _execute(self, attr_name: str, *args):
        async with self._pool.get_item(timeout=self._connect_timeout) as redis:
            return await redis.execute_cmd(attr_name, *args)

    def __getattr__(self, attr_name: str):
        if attr_name in {"multi", "exec", "discard", "subscribe", "psubscribe", "unsubscribe", "punsubscribe"}:
            raise AttributeError("'%s' object has no attribute '%s'" % (self, attr_name))
        return functools.partial(self._execute, attr_name)

    async def delete(self, *args):
        """Redis `del` command"""
        return await self._execute("del", *args)
