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

siderpy.LOG.setLevel('INFO')


pytestmark = pytest.mark.asyncio


class TestProtocol:

    @classmethod
    def setup_class(cls):
        siderpy.hiredis = None

    @classmethod
    def teardown_class(cls):
        siderpy.hiredis = sys.modules.get('hiredis')

    def test__init(self):
        proto = siderpy.Protocol()
        assert proto._reader is None
        assert proto._unparsed == b''
        assert isinstance(proto._parser, types.GeneratorType)
        assert isinstance(proto._ready, collections.deque) and len(proto._ready) == 0
        assert proto.feed == proto._feed
        assert proto.gets == proto._gets
        assert proto.has_data == proto._has_data

    def test__str(self):
        proto = siderpy.Protocol()
        assert str(proto) == '<siderpy.Protocol hiredis=False [{}]>'.format(hex(id(proto)))

    def test_reset(self):
        proto = siderpy.Protocol()
        proto._ready.append(1)
        proto.reset()
        assert isinstance(proto._ready, collections.deque)
        assert len(proto._ready) == 0
        assert proto._unparsed == b''

    def test_make_cmd(self):
        proto = siderpy.Protocol()
        assert proto.make_cmd(
                'get', ['key']) == b'*2\r\n$3\r\nget\r\n$3\r\nkey\r\n'
        assert proto.make_cmd(
                'set', ['key', 'value']) == b'*3\r\n$3\r\nset\r\n$3\r\nkey\r\n$5\r\nvalue\r\n'
        assert proto.make_cmd(
                'set', ['key', b'value']) == b'*3\r\n$3\r\nset\r\n$3\r\nkey\r\n$5\r\nvalue\r\n'
        assert proto.make_cmd(
                'set', ['key', 1]) == b'*3\r\n$3\r\nset\r\n$3\r\nkey\r\n$1\r\n1\r\n'

    def test__has_data(self):
        proto = siderpy.Protocol()
        proto._ready.append(1)
        assert proto._has_data()

        proto = siderpy.Protocol()
        proto._unparsed = b'+OK\r\n'
        assert proto._has_data()

        proto = siderpy.Protocol()
        proto._ready.append(1)
        proto._unparsed = b'+OK\r\n'
        assert proto._has_data()

    def test__parse_string(self):
        proto = siderpy.Protocol()
        parser = proto._parse_string()
        assert isinstance(parser, types.GeneratorType)
        next(parser)

        assert parser.send(b'+OK\r\n') == [b'OK', b'']
        assert parser.send(b'+OK') == [False, b'+OK']
        assert parser.send(b'+OK\r\n$6\r') == [b'OK', b'$6\r']

    def test__parse_error(self):
        proto = siderpy.Protocol()
        parser = proto._parse_error()
        assert isinstance(parser, types.GeneratorType)
        next(parser)

        data = parser.send(b'-Err\r\n')
        assert data and isinstance(data[0], siderpy.RedisError) and data[1] == b''
        data = parser.send(b'-Err')
        assert data == [False, b'-Err']
        data = parser.send(b'-Err\r\n+OK')
        assert data and isinstance(data[0], siderpy.RedisError) and data[1] == b'+OK'

    def test__parse_integer(self):
        proto = siderpy.Protocol()
        parser = proto._parse_integer()
        assert isinstance(parser, types.GeneratorType)
        next(parser)

        assert parser.send(b':1000\r\n') == [1000, b'']
        assert parser.send(b':1000') == [False, b':1000']
        assert parser.send(b':1000\r\n+OK') == [1000, b'+OK']

    def test__parse_bulk_string(self):
        proto = siderpy.Protocol()
        parser = proto._parse_bulk_string()
        assert isinstance(parser, types.GeneratorType)
        next(parser)

        assert parser.send(b'$6\r\nfoobar\r\n') == [b'foobar', b'']
        assert parser.send(b'$6\r\nfoo') == [False, b'$6\r\nfoo']
        assert parser.send(b'$6\r\nfoobar') == [False, b'$6\r\nfoobar']
        assert parser.send(b'$6\r\nfoobar\r\n+OK') == [b'foobar', b'+OK']
        assert parser.send(b'$-1\r\n') == [None, b'']
        assert parser.send(b'$0\r\n\r\n') == [b'', b'']

    def test__parse_array(self):
        proto = siderpy.Protocol()
        parser = proto._parse_array()
        assert isinstance(parser, types.GeneratorType)
        next(parser)

        assert parser.send(b'*0\r\n') == [[], b'']
        assert parser.send(b'*0\r') == [False, b'*0\r']
        assert parser.send(b'*0\r\n+OK') == [[], b'+OK']

        assert parser.send(b'*-1\r\n') == [None, b'']
        assert parser.send(b'*-1\r') == [False, b'*-1\r']
        assert parser.send(b'*-1\r\n+OK') == [None, b'+OK']

        assert parser.send(b'*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n') == [[b'foo', b'bar'], b'']
        assert parser.send(b'*3\r\n:1\r\n:2\r\n:3\r\n') == [[1, 2, 3], b'']
        assert parser.send(b'*5\r\n:1\r\n:2\r\n:3\r\n:4\r\n$6\r\nfoobar\r\n') == [[1, 2, 3, 4, b'foobar'], b'']

        assert parser.send(b'*2\r\n$3\r\nfoo\r\n$3\r\n') == [False, b'$3\r\n']
        assert parser.send(b'$3\r\nbar\r\n') == [[b'foo', b'bar'], b'']

        data = parser.send(b'*2\r\n*2\r\n:1\r\n:2\r\n*2\r\n+Foo\r\n+Bar\r\n')
        assert len(data[0]) == 2
        assert data[0][0] == [1, 2]
        assert data[0][1] == [b'Foo', b'Bar']
        assert data[1] == b''

        data = parser.send(b'*3\r\n$3\r\nfoo\r\n$-1\r\n$3\r\nbar\r\n')
        assert data[0] == [b'foo', None, b'bar'] and data[1] == b''

    def test__parse(self):
        proto = siderpy.Protocol()
        parser = proto._parse()
        assert isinstance(parser, types.GeneratorType)
        next(parser)

        assert parser.send(b'+OK') == [False, b'+OK']
        assert parser.send(b'+OK\r\n') == [b'OK', b'']
        data = parser.send(b'-Err\r\n')
        assert isinstance(data[0], siderpy.RedisError) and data[1] == b''
        assert parser.send(b':1000') == [False, b':1000']
        assert parser.send(b':1000\r\n') == [1000, b'']
        assert parser.send(b':1000\r\n+OK\r\n') == [1000, b'+OK\r\n']
        assert parser.send(b'$6\r\nfoobar\r\n') == [b'foobar', b'']
        assert parser.send(b'*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n') == [[b'foo', b'bar'], b'']

    def test__feed(self):
        proto = siderpy.Protocol()
        assert proto.feed(b'+OK\r\n') is None
        assert proto._unparsed == b''
        assert len(proto._ready) == 1
        assert proto._ready.popleft() == b'OK'

        proto = siderpy.Protocol()
        assert proto.feed(b'+OK') is None
        assert proto._unparsed == b'+OK'
        assert len(proto._ready) == 0
        assert proto.feed(b'\r\n') is None
        assert proto._unparsed == b''
        assert len(proto._ready) == 1
        assert proto._ready.popleft() == b'OK'

    def test__gets(self):
        proto = siderpy.Protocol()

        proto._ready.append(b'OK')
        assert proto.gets() == b'OK'
        assert len(proto._ready) == 0

        proto._ready.append(siderpy.RedisError('Err'))
        with pytest.raises(siderpy.RedisError, match='Err'):
            proto.gets()
        assert len(proto._ready) == 0


@pytest.mark.skipif(hiredis is None, reason="hiredis is not installed")
class TestProtocolHiredis:

    def test__init(self):
        proto = siderpy.Protocol()
        assert isinstance(proto._reader, hiredis.Reader)
        assert not hasattr(proto, '_unparsed')
        assert not hasattr(proto, '_parser')
        assert not hasattr(proto, '_ready')
        assert proto.feed == proto._reader.feed
        assert proto.gets == proto._gets_hiredis
        assert proto.has_data == proto._reader.has_data

    def test__str(self):
        proto = siderpy.Protocol()
        assert str(proto) == '<siderpy.Protocol hiredis=True [{}]>'.format(hex(id(proto)))

    def test_reset(self):
        proto = siderpy.Protocol()
        reader = proto._reader
        proto.reset()
        assert proto._reader is not reader

    def test_make_cmd(self):
        proto = siderpy.Protocol()
        assert proto.make_cmd(
                'get', ['key']) == b'*2\r\n$3\r\nget\r\n$3\r\nkey\r\n'
        assert proto.make_cmd(
                'set', ['key', 'value']) == b'*3\r\n$3\r\nset\r\n$3\r\nkey\r\n$5\r\nvalue\r\n'
        assert proto.make_cmd(
                'set', ['key', b'value']) == b'*3\r\n$3\r\nset\r\n$3\r\nkey\r\n$5\r\nvalue\r\n'
        assert proto.make_cmd(
                'set', ['key', 1]) == b'*3\r\n$3\r\nset\r\n$3\r\nkey\r\n$1\r\n1\r\n'

    def test__gets_hiredis(self):
        proto = siderpy.Protocol()

        proto.feed(b'+OK\r\n')
        assert proto.gets() == b'OK'

        proto.feed(b'-Err\r\n')
        with pytest.raises(siderpy.RedisError, match='Err'):
            proto.gets()


@pytest.mark.skipif(sys.version_info < (3, 8), reason="requires python3.8 or higher")
class Test_Pool:

    def test__init(self):
        async def factory():
            return

        def test(*args, **kwds):
            return

        def on_create(*args, **kwds):
            return

        pool = siderpy.Pool(factory)
        assert pool._factory == factory
        assert pool._size == siderpy.POOL_SIZE
        assert pool._test is None
        assert pool._on_create is None
        assert isinstance(pool._queue, asyncio.LifoQueue)
        assert pool._queue.maxsize == pool._size
        assert pool._queue.qsize() == pool._size
        assert len(pool._used) == 0

        pool = siderpy.Pool(factory, size=10, test=test, on_create=on_create)
        assert pool._factory == factory
        assert pool._size == 10
        assert pool._test == test
        assert pool._on_create == on_create
        assert isinstance(pool._queue, asyncio.LifoQueue)
        assert pool._queue.maxsize == 10
        assert pool._queue.qsize() == 10
        assert len(pool._used) == 0

    def test__str(self):
        pool = siderpy.Pool(lambda *args: args)
        assert str(pool) == '<siderpy.Pool size {}, available {} [{}]>'.format(
                siderpy.POOL_SIZE, siderpy.POOL_SIZE, hex(id(pool)))

    async def test_get(self):
        item = object()
        factory = mock.AsyncMock(return_value=item)
        pool = siderpy.Pool(factory)
        assert item == await pool.get()
        factory.assert_awaited_once()
        assert len(pool._used) == 1 and item in pool._used
        assert pool._queue.qsize() == pool._size - 1

    async def test_get_itemtest_not_called(self):
        item = object()
        factory = mock.AsyncMock(return_value=item)
        test = mock.MagicMock(return_value=True)
        pool = siderpy.Pool(factory, test=test)
        assert item == await pool.get()
        factory.assert_awaited_once()
        assert len(pool._used) == 1 and item in pool._used
        assert pool._queue.qsize() == pool._size - 1
        test.assert_not_called()

    async def test_get_itemtest_true_called(self):
        item = object()
        factory = mock.AsyncMock(return_value=item)
        test = mock.MagicMock(return_value=True)
        pool = siderpy.Pool(factory, size=1, test=test)
        pool._queue.get_nowait()
        pool._queue.put_nowait(item)
        assert item == await pool.get()
        assert len(pool._used) == 1 and item in pool._used
        assert pool._queue.qsize() == pool._size - 1
        test.assert_called_once_with(item)
        factory.assert_not_awaited()

    async def test_get_itemtest_false_called(self):
        item = object()
        factory = mock.AsyncMock(return_value=item)
        test = mock.MagicMock(return_value=False)
        pool = siderpy.Pool(factory, size=1, test=test)
        pool._queue.get_nowait()
        pool._queue.put_nowait(item)
        assert item == await pool.get()
        assert len(pool._used) == 1 and item in pool._used
        assert pool._queue.qsize() == pool._size - 1
        test.assert_called_once_with(item)
        factory.assert_awaited_once()

    async def test_get_timeout(self):
        item = object()
        factory = mock.AsyncMock(return_value=item)
        test = mock.MagicMock(return_value=False)
        pool = siderpy.Pool(factory, size=1, test=test)
        await pool.get()
        with pytest.raises(asyncio.TimeoutError):
            await pool.get(timeout=0.1)

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
        func = mock.MagicMock()
        pool.close(func)
        func.assert_has_calls([mock.call(item1), mock.call(item2)])


@pytest.mark.skipif(sys.version_info < (3, 8), reason="requires python3.8 or higher")
class TestRedis:

    def test__init(self):
        redis = siderpy.Redis('localhost')
        assert redis._host == 'localhost'
        assert redis._port == siderpy.REDIS_PORT
        assert redis._connect_timeout == siderpy.CONNECT_TIMEOUT
        assert redis._read_timeout is None
        assert redis._write_timeout is None
        assert redis._ssl_ctx is None
        assert redis._proto.__class__ == siderpy.Protocol
        assert redis._pipeline is False
        assert redis._buf == []
        assert redis._subscriber is None
        assert redis._subscriber_cb is None
        assert redis._subscriber_cb is None
        assert redis._subscriber_channels == set()

        redis = siderpy.Redis('localhost', port=777, connect_timeout=33, timeout=(10, 20), ssl_ctx=object())
        assert redis._host == 'localhost'
        assert redis._port == 777
        assert redis._connect_timeout == 33
        assert redis._read_timeout == 10
        assert redis._write_timeout == 20
        assert redis._ssl_ctx is not None
        assert redis._proto.__class__ == siderpy.Protocol
        assert redis._pipeline is False
        assert redis._buf == []
        assert redis._subscriber is None
        assert redis._subscriber_cb is None
        assert redis._subscriber_cb is None
        assert redis._subscriber_channels == set()

    def test__str(self):
        redis = siderpy.Redis('localhost')
        assert str(redis) == '<siderpy.Redis (localhost, {}) [{}]>'.format(siderpy.REDIS_PORT, hex(id(redis)))

    def test_close_connection(self):
        redis = siderpy.Redis('localhost')
        redis._pool = mock.MagicMock()
        redis.close_connection()
        redis._pool.close.assert_called_once()

    def test_pipeline_on(self):
        redis = siderpy.Redis('localhost')
        redis.pipeline_on()
        assert redis._pipeline is True

        redis._subscriber = object()
        with pytest.raises(siderpy.SiderPyError, match='Client in subscribe mode'):
            redis.pipeline_on()

    def test_pipeline_off(self):
        redis = siderpy.Redis('localhost')
        redis._pipeline = True
        redis.pipeline_off()
        assert redis._pipeline is False

    def test_pipeline(self):
        redis = siderpy.Redis('localhost')
        with redis.pipeline():
            assert redis._pipeline is True
        assert redis._pipeline is False

    async def test_pipeline_execute(self):
        with mock.patch.object(siderpy.Redis, '_execute_bytestring', return_value=None) as mock_method:
            redis = siderpy.Redis('localhost')
            await redis.pipeline_execute()
            mock_method.assert_not_awaited()
        with mock.patch.object(siderpy.Redis, '_execute_bytestring', return_value=None) as mock_method:
            redis = siderpy.Redis('localhost')
            redis._buf = [b'+OK\r\n', b':1000\r\n']
            await redis.pipeline_execute()
            mock_method.assert_awaited_once_with(b'+OK\r\n:1000\r\n')

    def test_pipeline_clear(self):
        redis = siderpy.Redis('localhost')
        redis._buf = [1, 2, 3]
        redis.pipeline_clear()
        assert redis._buf == []

    async def test__create_connection(self):
        with mock.patch('asyncio.open_connection') as mock_func:
            redis = siderpy.Redis('localhost')
            await redis._create_connection()
            mock_func.assert_awaited_once_with(host=redis._host,
                                               port=redis._port,
                                               ssl=redis._ssl_ctx,
                                               ssl_handshake_timeout=None)
        with mock.patch('asyncio.open_connection') as mock_func:
            redis = siderpy.Redis('localhost', connect_timeout=33, ssl_ctx=object())
            await redis._create_connection()
            mock_func.assert_awaited_once_with(host=redis._host,
                                               port=redis._port,
                                               ssl=redis._ssl_ctx,
                                               ssl_handshake_timeout=redis._connect_timeout)

    async def test__read(self):
        r = mock.Mock()
        r.read = mock.AsyncMock(return_value=b'$4\r\nPONG\r\n$3\r\nfoo\r\n')
        redis = siderpy.Redis('localhost')
        data = await redis._read(r)
        r.read.assert_awaited_once_with(1024)
        assert data == [b'PONG', b'foo']

        r = mock.Mock()
        r.read = mock.AsyncMock(return_value=b'')
        r.at_eof = mock.MagicMock(return_value=True)
        redis = siderpy.Redis('localhost')
        with pytest.raises(ConnectionError):
            await redis._read(r)

        raw_data = b'$1500\r\n' + b'a' * 1500 + b'\r\n'
        r = mock.Mock()
        r.read = mock.AsyncMock(side_effect=[raw_data[:1024], raw_data[1024:]])
        redis = siderpy.Redis('localhost')
        assert await redis._read(r) == [b'a' * 1500]

    async def test__execute_bytestring(self):
        r = mock.Mock()
        w = mock.Mock()
        w.write = mock.MagicMock()
        w.drain = mock.AsyncMock()
        with mock.patch.object(siderpy.Redis, '_read', return_value=[b'PONG']) as mock_read:
            with mock.patch.object(siderpy.Pool, 'get', return_value=(r, w)) as mock_get:
                redis = siderpy.Redis('localhost')
                data = await redis._execute_bytestring(b'$4\r\nPING\r\n')
                assert data == b'PONG'
                mock_get.assert_awaited_once_with(timeout=redis._connect_timeout)
                mock_read.assert_awaited_once_with(r)

    async def test__execute_bytestring_write_timeout(self):
        r = mock.Mock()
        w = mock.Mock()
        w.write = mock.MagicMock()
        w.drain = functools.partial(asyncio.sleep, 10)
        w.close = mock.MagicMock()
        w.wait_closed = mock.AsyncMock()
        with mock.patch.object(siderpy.Redis, '_read', return_value=[b'PONG']):
            with mock.patch.object(siderpy.Pool, 'get', return_value=(r, w)):
                redis = siderpy.Redis('localhost', timeout=(None, 0.1))
                with pytest.raises(asyncio.TimeoutError):
                    await redis._execute_bytestring(b'$4\r\nPING\r\n')
                    w.close.assert_called_once()
                    w.wait_closed.assert_awaited_once()

    async def test__execute_bytestring_read_timeout(self):
        r = mock.Mock()
        w = mock.Mock()
        w.write = mock.MagicMock()
        w.drain = mock.AsyncMock()
        w.close = mock.MagicMock()
        w.wait_closed = mock.AsyncMock()
        with mock.patch.object(siderpy.Redis, '_read', side_effect=functools.partial(asyncio.sleep, 10)):
            with mock.patch.object(siderpy.Pool, 'get', return_value=(r, w)):
                redis = siderpy.Redis('localhost', timeout=(0.1, 0.1))
                with pytest.raises(asyncio.TimeoutError):
                    await redis._execute_bytestring(b'$4\r\nPING\r\n')
                    w.close.assert_called_once()
                    w.wait_closed.assert_awaited_once()


@pytest.mark.skipif(sys.version_info < (3, 8), reason="requires python3.8 or higher")
class TestRedisPool:

    def test__init(self):
        pool = siderpy.RedisPool('localhost')
        assert pool._host == 'localhost'
        assert pool._port == siderpy.REDIS_PORT
        assert pool._connect_timeout == siderpy.CONNECT_TIMEOUT
        assert pool._read_timeout is None
        assert pool._write_timeout is None
        assert pool._size == siderpy.POOL_SIZE
        assert pool._ssl_ctx is None
        assert pool._pool is not None and pool._pool._size == pool._size

        pool = siderpy.RedisPool('localhost', port=777, size=1, connect_timeout=33, timeout=(10, 20), ssl_ctx=object())
        assert pool._host == 'localhost'
        assert pool._port == 777
        assert pool._connect_timeout == 33
        assert pool._read_timeout == 10
        assert pool._write_timeout == 20
        assert pool._size == 1
        assert pool._ssl_ctx is not None
        assert pool._pool is not None and pool._pool._size == pool._size

    def test__str(self):
        pool = siderpy.RedisPool('localhost')
        assert str(pool) == '<siderpy.RedisPool (localhost, {}) {} [{}]>'.format(
                siderpy.REDIS_PORT, pool._pool, hex(id(pool)))
