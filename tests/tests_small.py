import asyncio
import collections
import functools
import sys
import types

from unittest import mock

try:
    import hiredis
except ImportError:
    hiredis = None

import pytest

import siderpy

siderpy.LOG.setLevel("INFO")


pytestmark = pytest.mark.asyncio


class TestProtocol:
    @classmethod
    def setup_class(cls):
        siderpy.hiredis = None

    @classmethod
    def teardown_class(cls):
        siderpy.hiredis = sys.modules.get("hiredis")

    def test__init(self):
        proto = siderpy.Protocol()
        assert proto._reader is None
        assert proto._unparsed == b""
        assert isinstance(proto._parser, types.GeneratorType)
        assert isinstance(proto._ready, collections.deque) and len(proto._ready) == 0
        assert proto.feed == proto._feed
        assert proto.gets == proto._gets
        assert proto.has_data == proto._has_data

    def test__str(self):
        proto = siderpy.Protocol()
        assert str(proto) == "<siderpy.Protocol hiredis=False [{}]>".format(hex(id(proto)))

    def test_reset(self):
        proto = siderpy.Protocol()
        proto._ready.append(1)
        proto.reset()
        assert isinstance(proto._ready, collections.deque)
        assert len(proto._ready) == 0
        assert proto._unparsed == b""

    def test_make_cmd(self):
        proto = siderpy.Protocol()
        assert proto.make_cmd("get", ["key"]) == b"*2\r\n$3\r\nget\r\n$3\r\nkey\r\n"
        assert proto.make_cmd("set", ["key", "value"]) == b"*3\r\n$3\r\nset\r\n$3\r\nkey\r\n$5\r\nvalue\r\n"
        assert proto.make_cmd("set", ["key", b"value"]) == b"*3\r\n$3\r\nset\r\n$3\r\nkey\r\n$5\r\nvalue\r\n"
        assert proto.make_cmd("set", ["key", 1]) == b"*3\r\n$3\r\nset\r\n$3\r\nkey\r\n$1\r\n1\r\n"
        with pytest.raises(TypeError):
            assert proto.make_cmd("blpop", ["key", None])

    def test__has_data(self):
        proto = siderpy.Protocol()
        proto._ready.append(1)
        assert proto._has_data()

        proto = siderpy.Protocol()
        proto._unparsed = b"+OK\r\n"
        assert proto._has_data()

        proto = siderpy.Protocol()
        proto._ready.append(1)
        proto._unparsed = b"+OK\r\n"
        assert proto._has_data()

    def test__parse_string(self):
        proto = siderpy.Protocol()
        parser = proto._parse_string()
        assert isinstance(parser, types.GeneratorType)
        next(parser)

        assert parser.send(b"+OK\r\n") == [b"OK", b""]
        assert parser.send(b"+OK") == (False, b"+OK")
        assert parser.send(b"+OK\r\n$6\r") == [b"OK", b"$6\r"]

    def test__parse_error(self):
        proto = siderpy.Protocol()
        parser = proto._parse_error()
        assert isinstance(parser, types.GeneratorType)
        next(parser)

        data = parser.send(b"-Err\r\n")
        assert data and isinstance(data[0], siderpy.RedisError) and data[1] == b""
        data = parser.send(b"-Err")
        assert data == (False, b"-Err")
        data = parser.send(b"-Err\r\n+OK")
        assert data and isinstance(data[0], siderpy.RedisError) and data[1] == b"+OK"

    def test__parse_integer(self):
        proto = siderpy.Protocol()
        parser = proto._parse_integer()
        assert isinstance(parser, types.GeneratorType)
        next(parser)

        assert parser.send(b":1000\r\n") == [1000, b""]
        assert parser.send(b":1000") == (False, b":1000")
        assert parser.send(b":1000\r\n+OK") == [1000, b"+OK"]

    def test__parse_bulk_string(self):
        proto = siderpy.Protocol()
        parser = proto._parse_bulk_string()
        assert isinstance(parser, types.GeneratorType)
        next(parser)

        assert parser.send(b"$6\r\nfoobar\r\n") == (b"foobar", b"")
        assert parser.send(b"$6\r\nfoo") == (False, b"$6\r\nfoo")
        assert parser.send(b"$6\r\nfoobar") == (False, b"$6\r\nfoobar")
        assert parser.send(b"$6\r\nfoobar\r\n+OK") == (b"foobar", b"+OK")
        assert parser.send(b"$-1\r\n") == (None, b"")
        assert parser.send(b"$0\r\n\r\n") == (b"", b"")

    def test__parse_array(self):
        proto = siderpy.Protocol()
        parser = proto._parse_array()
        assert isinstance(parser, types.GeneratorType)
        next(parser)

        assert parser.send(b"*0\r\n") == ([], b"")
        assert parser.send(b"*0\r") == (False, b"*0\r")
        assert parser.send(b"*0\r\n+OK") == ([], b"+OK")

        assert parser.send(b"*-1\r\n") == (None, b"")
        assert parser.send(b"*-1\r") == (False, b"*-1\r")
        assert parser.send(b"*-1\r\n+OK") == (None, b"+OK")

        assert parser.send(b"*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n") == ([b"foo", b"bar"], b"")
        assert parser.send(b"*3\r\n:1\r\n:2\r\n:3\r\n") == ([1, 2, 3], b"")
        assert parser.send(b"*5\r\n:1\r\n:2\r\n:3\r\n:4\r\n$6\r\nfoobar\r\n") == ([1, 2, 3, 4, b"foobar"], b"")

        assert parser.send(b"*2\r\n$3\r\nfoo\r\n$3\r\n") == (False, b"$3\r\n")
        assert parser.send(b"$3\r\nbar\r\n") == ([b"foo", b"bar"], b"")

        data = parser.send(b"*2\r\n*2\r\n:1\r\n:2\r\n*2\r\n+Foo\r\n+Bar\r\n")
        assert len(data[0]) == 2
        assert data[0][0] == [1, 2]
        assert data[0][1] == [b"Foo", b"Bar"]
        assert data[1] == b""

        data = parser.send(b"*3\r\n$3\r\nfoo\r\n$-1\r\n$3\r\nbar\r\n")
        assert data[0] == [b"foo", None, b"bar"] and data[1] == b""

    def test__parse(self):
        proto = siderpy.Protocol()
        parser = proto._parse()
        assert isinstance(parser, types.GeneratorType)
        next(parser)

        assert parser.send(b"+OK") == (False, b"+OK")
        assert parser.send(b"+OK\r\n") == [b"OK", b""]
        data = parser.send(b"-Err\r\n")
        assert isinstance(data[0], siderpy.RedisError) and data[1] == b""
        assert parser.send(b":1000") == (False, b":1000")
        assert parser.send(b":1000\r\n") == [1000, b""]
        assert parser.send(b":1000\r\n+OK\r\n") == [1000, b"+OK\r\n"]
        assert parser.send(b"$6\r\nfoobar\r\n") == (b"foobar", b"")
        assert parser.send(b"*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n") == ([b"foo", b"bar"], b"")

    def test__feed(self):
        proto = siderpy.Protocol()
        assert proto.feed(b"+OK\r\n") is None
        assert proto._unparsed == b""
        assert len(proto._ready) == 1
        assert proto._ready.popleft() == b"OK"

        proto = siderpy.Protocol()
        assert proto.feed(b"+OK") is None
        assert proto._unparsed == b"+OK"
        assert len(proto._ready) == 0
        assert proto.feed(b"\r\n") is None
        assert proto._unparsed == b""
        assert len(proto._ready) == 1
        assert proto._ready.popleft() == b"OK"

    def test__gets(self):
        proto = siderpy.Protocol()

        proto._ready.append(b"OK")
        assert proto.gets() == b"OK"
        assert len(proto._ready) == 0

        proto._ready.append(siderpy.RedisError("Err"))
        assert isinstance(proto.gets(), siderpy.RedisError)
        assert len(proto._ready) == 0


@pytest.mark.skipif(hiredis is None, reason="hiredis is not installed")
class TestProtocolHiredis:
    def test__init(self):
        proto = siderpy.Protocol()
        assert isinstance(proto._reader, hiredis.Reader)
        assert not hasattr(proto, "_unparsed")
        assert not hasattr(proto, "_parser")
        assert not hasattr(proto, "_ready")
        assert proto.feed == proto._reader.feed
        assert proto.has_data == proto._reader.has_data

    def test__str(self):
        proto = siderpy.Protocol()
        assert str(proto) == "<siderpy.Protocol hiredis=True [{}]>".format(hex(id(proto)))

    def test_reset(self):
        proto = siderpy.Protocol()
        reader = proto._reader
        proto.reset()
        assert proto._reader is not reader

    def test_make_cmd(self):
        proto = siderpy.Protocol()
        assert proto.make_cmd("get", ["key"]) == b"*2\r\n$3\r\nget\r\n$3\r\nkey\r\n"
        assert proto.make_cmd("set", ["key", "value"]) == b"*3\r\n$3\r\nset\r\n$3\r\nkey\r\n$5\r\nvalue\r\n"
        assert proto.make_cmd("set", ["key", b"value"]) == b"*3\r\n$3\r\nset\r\n$3\r\nkey\r\n$5\r\nvalue\r\n"
        assert proto.make_cmd("set", ["key", 1]) == b"*3\r\n$3\r\nset\r\n$3\r\nkey\r\n$1\r\n1\r\n"


@pytest.mark.skipif(sys.version_info < (3, 8), reason="requires python3.8 or higher")
class TestRedis:
    def test__parse_address(self):
        parsed = siderpy.Redis._parse_address("redis://username:password@localhost:6379?db=0")
        assert parsed.get("scheme") == "redis"
        assert parsed.get("username") == "username"
        assert parsed.get("password") == "password"
        assert parsed.get("host") == "localhost"
        assert parsed.get("port") == 6379
        assert parsed.get("path") is None
        assert parsed.get("db") == 0

    def test__parse_invalid_address(self):
        with pytest.raises(ValueError, match=r"Scheme is required"):
            siderpy.Redis._parse_address("localhost")
        with pytest.raises(ValueError, match=r"Scheme is required"):
            siderpy.Redis._parse_address("redis//localhost")
        with pytest.raises(ValueError, match=r"Scheme is not supported http"):
            siderpy.Redis._parse_address("http://localhost")
        with pytest.raises(ValueError, match=r"Path param is not supported"):
            siderpy.Redis._parse_address("redis://localhost/0")
        with pytest.raises(ValueError, match=r"Hostname is required"):
            siderpy.Redis._parse_address("redis://:password")
        with pytest.raises(ValueError, match=r"Unix socket path is required"):
            siderpy.Redis._parse_address("redis+unix://:password")

    def test__init_defaults(self):
        with mock.patch("siderpy.Protocol") as mock_proto:
            with mock.patch("siderpy.PubSubQueue") as mock_queue:
                redis = siderpy.Redis()
                assert redis._scheme == "redis"
                assert redis._username is None
                assert redis._password is None
                assert redis._host == "localhost"
                assert redis._port == 6379
                assert redis._path is None
                assert redis._db == 0
                assert redis._connect_timeout == siderpy.CONNECT_TIMEOUT
                assert redis._read_timeout is None
                assert redis._write_timeout is None
                assert redis._ssl_ctx is None
                assert redis._conn is None
                assert redis._conn_lock is not None
                assert redis._proto is not None
                assert redis._pipeline is False
                assert redis._pipeline_buf == []
                assert redis._future is None
                assert redis._cmd_count is None
                assert redis._subscription is False
                assert redis._anext_lock is not None
                assert redis._listener is None
                assert redis._pubsub_queue is not None
                assert redis._get_connection is not None
                mock_proto.assert_called_once_with(encoding=None, errors=None)
                mock_queue.assert_called_once_with(maxsize=0)

    def test__init_custom(self):
        with mock.patch("siderpy.Protocol") as mock_proto:
            with mock.patch("siderpy.PubSubQueue") as mock_queue:
                redis = siderpy.Redis(
                    "redis://user:pass@127.0.0.1:6380?db=1",
                    connect_timeout=33,
                    timeout=(10, 20),
                    encoding="utf-8",
                    errors="strict",
                    ssl_ctx=object(),
                    pubsub_queue_maxsize=100,
                )
                assert redis._scheme == "redis"
                assert redis._username == "user"
                assert redis._password == "pass"
                assert redis._host == "127.0.0.1"
                assert redis._port == 6380
                assert redis._path is None
                assert redis._db == 1
                assert redis._connect_timeout == 33
                assert redis._read_timeout == 10
                assert redis._write_timeout == 20
                assert redis._ssl_ctx is not None
                assert redis._conn is None
                assert redis._conn_lock is not None
                assert redis._proto is not None
                assert redis._pipeline is False
                assert redis._pipeline_buf == []
                assert redis._future is None
                assert redis._cmd_count is None
                assert redis._subscription is False
                assert redis._anext_lock is not None
                assert redis._listener is None
                assert redis._pubsub_queue is not None
                assert redis._get_connection is not None
                mock_proto.assert_called_once_with(encoding="utf-8", errors="strict")
                mock_queue.assert_called_once_with(maxsize=100)

            redis = siderpy.Redis("redis+unix://user:pass@/var/run/redis.sock?db=1")
            assert redis._scheme == "redis+unix"
            assert redis._username == "user"
            assert redis._password == "pass"
            assert redis._host is None
            assert redis._port == 6379
            assert redis._path == "/var/run/redis.sock"
            assert redis._db == 1

    def test__str(self):
        redis = siderpy.Redis("redis://127.0.0.1:5555")
        assert str(redis) == "<siderpy.Redis (127.0.0.1, 5555) [{}]>".format(hex(id(redis)))

        redis = siderpy.Redis("redis+unix:///var/run/redis.sock")
        assert str(redis) == "<siderpy.Redis (redis.sock) [{}]>".format(hex(id(redis)))

    async def test_close_connection(self):
        redis = siderpy.Redis()
        mock_conn = mock.MagicMock()
        redis._conn = mock_conn
        mock_queue = mock.MagicMock()
        redis._pubsub_queue = mock_queue
        await redis.close_connection()
        mock_conn[1].close.assert_called_once()
        mock_queue.close.assert_not_called()
        assert redis._conn is None

        with mock.patch("siderpy.Redis._cancel_listener") as mock_cancel_listener:
            redis = siderpy.Redis()
            mock_listener = mock.MagicMock()
            redis._listener = mock_listener
            mock_conn = mock.MagicMock()
            redis._conn = mock_conn
            mock_queue = mock.MagicMock()
            redis._pubsub_queue = mock_queue
            await redis.close_connection()
            mock_cancel_listener.assert_awaited_once()
            mock_conn[1].close.assert_called_once()
            assert redis._conn is None

    def test_pipeline_on(self):
        redis = siderpy.Redis()
        redis.pipeline_on()
        assert redis._pipeline is True

    def test_pipeline_off(self):
        redis = siderpy.Redis()
        redis._pipeline = True
        redis.pipeline_off()
        assert redis._pipeline is False

    def test_pipeline_contextmanager(self):
        redis = siderpy.Redis()
        with redis.pipeline():
            assert redis._pipeline is True
        assert redis._pipeline is False

    async def test_pipeline_execute(self):
        with mock.patch.object(siderpy.Redis, "_execute_cmd_list", return_value=None) as mock_method:
            redis = siderpy.Redis()
            await redis.pipeline_execute()
            mock_method.assert_not_awaited()
        with mock.patch.object(siderpy.Redis, "_execute_cmd_list", return_value=None) as mock_method:
            redis = siderpy.Redis()
            redis._pipeline_buf = [b"+OK\r\n", b":1000\r\n"]
            await redis.pipeline_execute()
            mock_method.assert_awaited_once_with([b"+OK\r\n", b":1000\r\n"])

    def test_pipeline_clear(self):
        redis = siderpy.Redis()
        redis._pipeline_buf = [1, 2, 3]
        redis.pipeline_clear()
        assert redis._pipeline_buf == []

    async def test__open_connection(self):
        with mock.patch("asyncio.open_connection") as mock_func:
            redis = siderpy.Redis()
            await redis._open_connection()
            mock_func.assert_awaited_once_with(host="localhost", port=6379, ssl=None, ssl_handshake_timeout=None)

    async def test__create_unix_connection(self):
        with mock.patch("asyncio.open_unix_connection") as mock_func:
            redis = siderpy.Redis(address="redis+unix:///var/run/redis.sock")
            await redis._open_connection()
            mock_func.assert_awaited_once_with(path="/var/run/redis.sock", ssl=None, ssl_handshake_timeout=None)

    async def test__open_connection_timeout(self):
        async def sleep(*args, **kwds):
            await asyncio.sleep(10)

        with mock.patch("asyncio.open_connection", side_effect=sleep) as mock_func:
            ssl_ctx = object()
            redis = siderpy.Redis(connect_timeout=1, ssl_ctx=ssl_ctx)
            with pytest.raises(asyncio.TimeoutError):
                await redis._open_connection()
            mock_func.assert_awaited_once_with(host="localhost", port=6379, ssl=ssl_ctx, ssl_handshake_timeout=1)

    async def test__open_connection_auth(self):
        with mock.patch("asyncio.open_connection") as mock_func:
            with mock.patch("siderpy.Redis._execute_cmd_list") as mock_execute_cmd_list:
                redis = siderpy.Redis("redis://:password@127.0.0.1:7777")
                await redis._open_connection()
            mock_func.assert_awaited_once_with(host="127.0.0.1", port=7777, ssl=None, ssl_handshake_timeout=None)
            mock_execute_cmd_list.assert_awaited_once_with([["auth", ("password",)]])

    async def test__open_connection_acl_auth(self):
        with mock.patch("asyncio.open_connection") as mock_func:
            with mock.patch("siderpy.Redis._execute_cmd_list") as mock_execute_cmd_list:
                redis = siderpy.Redis("redis://username:password@127.0.0.1:7777")
                await redis._open_connection()
            mock_func.assert_awaited_once_with(host="127.0.0.1", port=7777, ssl=None, ssl_handshake_timeout=None)
            mock_execute_cmd_list.assert_awaited_once_with([["auth", ("username", "password")]])

    async def test__open_connection_custom_db(self):
        with mock.patch("asyncio.open_connection") as mock_func:
            with mock.patch("siderpy.Redis._execute_cmd_list") as mock_execute_cmd_list:
                redis = siderpy.Redis("redis://127.0.0.1?db=5")
                await redis._open_connection()
            mock_func.assert_awaited_once_with(host="127.0.0.1", port=6379, ssl=None, ssl_handshake_timeout=None)
            mock_execute_cmd_list.assert_awaited_once_with([["select", (5,)]])

    async def test__open_connection_auth_failed(self):
        with mock.patch("asyncio.open_connection") as mock_func:
            with mock.patch("siderpy.Redis._execute_cmd_list", side_effect=siderpy.RedisError) as mock_execute_cmd_list:
                with mock.patch("siderpy.Redis.close_connection") as mock_close_connection:
                    redis = siderpy.Redis("redis://username:password@127.0.0.1")
                    with pytest.raises(siderpy.RedisError):
                        await redis._open_connection()
            mock_func.assert_awaited_once_with(host="127.0.0.1", port=6379, ssl=None, ssl_handshake_timeout=None)
            mock_execute_cmd_list.assert_awaited_once_with([["auth", ("username", "password")]])
            mock_close_connection.assert_awaited_once()

    @mock.patch("asyncio.wait_for")
    async def test__read(self, mock_wait_for):
        r = mock.MagicMock()
        r.read = mock.AsyncMock(return_value=b"*1\r\n+OK\r\n")
        w = mock.MagicMock()
        w.drain = mock.AsyncMock()
        w.wait_closed = mock.AsyncMock()
        redis = siderpy.Redis()
        redis._conn = (r, w)
        redis._cmd_count = 1
        data = await redis._read()
        r.read.assert_awaited_once()
        mock_wait_for.assert_not_awaited()
        assert data == [[b"OK"]]

    async def test__read_timeout(self):
        async def sleep(*args, **kwds):
            await asyncio.sleep(10)

        r = mock.MagicMock()
        r.read = sleep
        w = mock.MagicMock()
        w.is_closing = mock.MagicMock(return_value=False)
        w.drain = mock.AsyncMock()
        w.wait_closed = mock.AsyncMock()
        redis = siderpy.Redis(timeout=(0.1, None))
        redis._conn = (r, w)
        redis._cmd_count = 1
        with pytest.raises(asyncio.TimeoutError):
            await redis._read()

    @mock.patch("asyncio.wait_for")
    @mock.patch("asyncio.create_task")
    async def test__execute_cmd_list_open_conn(self, mock_create_task, mock_wait_for):
        r = mock.MagicMock()
        r.read = mock.AsyncMock(return_value=b"$8\r\npingpong\r\n")
        w = mock.MagicMock()
        w.drain = mock.AsyncMock()
        w.wait_closed = mock.AsyncMock()
        redis = siderpy.Redis()
        with mock.patch(
            "siderpy.Redis._open_connection", side_effect=lambda: setattr(redis, "_conn", (r, w))
        ) as mock_open_connection:
            resp = await redis._execute_cmd_list([["ping", ("pingpong",)]])
        mock_open_connection.assert_awaited_once()
        w.write.assert_called_once_with(bytearray(b"*2\r\n$4\r\nping\r\n$8\r\npingpong\r\n"))
        w.drain.assert_awaited_once()
        r.read.assert_awaited_once()
        assert resp == b"pingpong"
        assert redis._subscription is False
        mock_create_task.assert_not_called()
        assert redis._listener is None
        assert redis._conn == (r, w)
        w.close.assert_not_called()
        w.wait_closed.assert_not_awaited()
        mock_wait_for.assert_not_awaited()

    @mock.patch("asyncio.wait_for")
    @mock.patch("asyncio.create_task")
    async def test__execute_cmd_not_open_conn(self, mock_create_task, mock_wait_for):
        r = mock.MagicMock()
        r.read = mock.AsyncMock(return_value=b"$8\r\npingpong\r\n")
        w = mock.MagicMock()
        w.is_closing = mock.MagicMock(return_value=False)
        w.drain = mock.AsyncMock()
        w.wait_closed = mock.AsyncMock()
        redis = siderpy.Redis()
        redis._conn = (r, w)
        with mock.patch("siderpy.Redis._open_connection") as mock_open_connection:
            resp = await redis._execute_cmd_list([["ping", ("pingpong",)]])
        mock_open_connection.assert_not_awaited()
        w.write.assert_called_once_with(bytearray(b"*2\r\n$4\r\nping\r\n$8\r\npingpong\r\n"))
        w.drain.assert_awaited_once()
        r.read.assert_awaited_once()
        assert resp == b"pingpong"
        assert redis._subscription is False
        mock_create_task.assert_not_called()
        assert redis._listener is None
        assert redis._conn == (r, w)
        w.close.assert_not_called()
        w.wait_closed.assert_not_awaited()
        mock_wait_for.assert_not_awaited()

    @mock.patch("asyncio.create_task")
    async def test__execute_cmd_list_write_timeout(self, mock_create_task):
        r = mock.MagicMock()
        r.read = mock.AsyncMock(return_value=b"$8\r\npingpong\r\n")
        w = mock.MagicMock()
        w.is_closing = mock.MagicMock(return_value=False)
        w.drain = functools.partial(asyncio.sleep, 1000)
        w.wait_closed = mock.AsyncMock()
        redis = siderpy.Redis(timeout=(None, 0.1))
        redis._conn = (r, w)
        with mock.patch("siderpy.Redis._open_connection") as mock_open_connection:
            with pytest.raises(asyncio.TimeoutError):
                await redis._execute_cmd_list([["ping", ("pingpong",)]])
        mock_open_connection.assert_not_awaited()
        w.write.assert_called_once_with(bytearray(b"*2\r\n$4\r\nping\r\n$8\r\npingpong\r\n"))
        r.read.assert_not_awaited()
        assert redis._subscription is False
        mock_create_task.assert_not_called()
        assert redis._listener is None
        assert redis._conn is None
        w.close.assert_called_once()
        w.wait_closed.assert_awaited_once()

    @mock.patch("asyncio.create_task")
    async def test__execute_cmd_list_read_timeout(self, mock_create_task):
        async def sleep(*args, **kwds):
            await asyncio.sleep(10)

        r = mock.MagicMock()
        r.read = sleep
        w = mock.MagicMock()
        w.is_closing = mock.MagicMock(return_value=False)
        w.drain = mock.AsyncMock()
        w.wait_closed = mock.AsyncMock()
        redis = siderpy.Redis(timeout=(0.1, None))
        redis._conn = (r, w)
        with mock.patch("siderpy.Redis._open_connection") as mock_open_connection:
            with pytest.raises(asyncio.TimeoutError):
                await redis._execute_cmd_list([["ping", ("pingpong",)]])
        mock_open_connection.assert_not_awaited()
        w.write.assert_called_once_with(bytearray(b"*2\r\n$4\r\nping\r\n$8\r\npingpong\r\n"))
        assert redis._subscription is False
        mock_create_task.assert_not_called()
        assert redis._listener is None
        assert redis._conn is None
        w.close.assert_called_once()
        w.wait_closed.assert_awaited_once()


@pytest.mark.skipif(sys.version_info < (3, 8), reason="requires python3.8 or higher")
class Test_Pool:
    def test__init(self):
        async def factory():
            return

        pool = siderpy.Pool(factory)
        assert pool._factory == factory
        assert pool._size == siderpy.POOL_SIZE
        assert isinstance(pool._queue, asyncio.LifoQueue)
        assert pool._queue.maxsize == pool._size
        assert pool._queue.qsize() == pool._size
        assert len(pool._used) == 0

        pool = siderpy.Pool(factory, size=10)
        assert pool._factory == factory
        assert pool._size == 10
        assert isinstance(pool._queue, asyncio.LifoQueue)
        assert pool._queue.maxsize == 10
        assert pool._queue.qsize() == 10
        assert len(pool._used) == 0

    def test__str(self):
        pool = siderpy.Pool(lambda *args: args)
        assert str(pool) == "<siderpy.Pool size {}, available {} [{}]>".format(
            siderpy.POOL_SIZE, siderpy.POOL_SIZE, hex(id(pool))
        )

    async def test_get(self):
        item = object()
        factory = mock.AsyncMock(return_value=item)
        pool = siderpy.Pool(factory)
        assert item == await pool.get()
        factory.assert_awaited_once()
        assert len(pool._used) == 1 and item in pool._used
        assert pool._queue.qsize() == pool._size - 1

    async def test_put(self):
        item = object()
        factory = mock.AsyncMock(return_value=item)
        pool = siderpy.Pool(factory)
        o = await pool.get()
        pool.put(o)
        assert len(pool._used) == 0
        assert pool._queue.qsize() == pool._size

    async def test_put_alien(self):
        item = object()
        factory = mock.AsyncMock(return_value=item)
        pool = siderpy.Pool(factory)
        await pool.get()
        alien = object()
        pool.put(alien)
        assert len(pool._used) == 1
        assert pool._queue.qsize() == pool._size - 1

    async def test_close(self):
        item1 = object()
        item2 = object()
        factory = mock.AsyncMock(side_effect=[item1, item2])
        pool = siderpy.Pool(factory)
        await pool.get()
        pool.put(await pool.get())
        items = []

        async def func(item):
            items.append(item)

        await pool.close(func)
        assert items == [item1, item2]


@pytest.mark.skipif(sys.version_info < (3, 8), reason="requires python3.8 or higher")
class TestRedisPool:
    def test__init_defaults(self):
        pool = siderpy.RedisPool()
        assert pool._address == "redis://localhost:6379?db=0"
        assert pool._connect_timeout == siderpy.CONNECT_TIMEOUT
        assert pool._read_timeout is None
        assert pool._write_timeout is None
        assert pool._size == siderpy.POOL_SIZE
        assert pool._ssl_ctx is None
        assert pool._pool is not None and pool._pool._size == pool._size

    def test__init_custom(self):
        pool = siderpy.RedisPool(
            "redis://127.0.0.1:7777?db=5", size=1, connect_timeout=33, timeout=(10, 20), ssl_ctx=object()
        )
        assert pool._address == "redis://127.0.0.1:7777?db=5"
        assert pool._connect_timeout == 33
        assert pool._read_timeout == 10
        assert pool._write_timeout == 20
        assert pool._size == 1
        assert pool._ssl_ctx is not None
        assert pool._pool is not None and pool._pool._size == pool._size

    def test__str(self):
        pool = siderpy.RedisPool()
        assert str(pool) == "<siderpy.RedisPool {} [{}]>".format(pool._pool, hex(id(pool)))
