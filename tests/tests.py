import asyncio
import os
import ssl

import aioredis
import pytest

import siderpy

siderpy.LOG.setLevel('DEBUG')


REDIS_HOST = os.environ.get('REDIS_HOST', 'redis')
REDIS_PORT = os.environ.get('REDIS_PORT', 6379)
USE_SSL = os.environ.get('SIDERPY_USE_SSL')


@pytest.fixture
def proto():
    return siderpy.Protocol()


class TestRedisProtocol:

    def test_parse_simple_str(self, proto):
        proto.feed(b'+OK\r\n')
        assert proto.has_data()
        data = proto.gets()
        assert data == b'OK'
        if not siderpy.hiredis:
            assert proto._unparsed == b''
        assert not proto.has_data()

    def test_parse_simple_str_double(self, proto):
        proto.feed(b'+OK\r\n')
        proto.feed(b'+OK\r\n')
        assert proto.has_data()
        data = proto.gets()
        assert data == b'OK'
        if not siderpy.hiredis:
            assert proto._unparsed == b''
        assert proto.has_data()
        data = proto.gets()
        assert data == b'OK'
        if not siderpy.hiredis:
            assert proto._unparsed == b''
        assert not proto.has_data()

    def test_parse_err(self, proto):
        proto.feed(b'-Err\r\n')
        assert proto.has_data()
        with pytest.raises(siderpy.RedisError, match='Err'):
            proto.gets()
        if not siderpy.hiredis:
            assert proto._unparsed == b''
        assert not proto.has_data()

    def test_parse_int(self, proto):
        proto.feed(b':1000\r\n')
        assert proto.has_data()
        data = proto.gets()
        assert data == 1000
        if not siderpy.hiredis:
            assert proto._unparsed == b''
        assert not proto.has_data()

    def test_parse_bulk_str(self, proto):
        proto.feed(b'$6\r\nfoobar\r\n')
        assert proto.has_data()
        data = proto.gets()
        assert data == b'foobar'
        if not siderpy.hiredis:
            assert proto._unparsed == b''
        assert not proto.has_data()

    def test_parse_empty_str(self, proto):
        proto.feed(b'$0\r\n\r\n')
        assert proto.has_data()
        data = proto.gets()
        assert data == b''
        if not siderpy.hiredis:
            assert proto._unparsed == b''
        assert not proto.has_data()

    def test_parse_null(self, proto):
        proto.feed(b'$-1\r\n')
        assert proto.has_data()
        data = proto.gets()
        assert data is None
        if not siderpy.hiredis:
            assert proto._unparsed == b''
        assert not proto.has_data()

    def test_parse_empty_array(self, proto):
        proto.feed(b'*0\r\n')
        assert proto.has_data()
        data = proto.gets()
        assert data == []
        if not siderpy.hiredis:
            assert proto._unparsed == b''
        assert not proto.has_data()

    def test_parse_array(self, proto):
        proto.feed(b'*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n')
        assert proto.has_data()
        data = proto.gets()
        assert data == [b'foo', b'bar']
        if not siderpy.hiredis:
            assert proto._unparsed == b''
        assert not proto.has_data()

        proto.feed(b'*3\r\n:1\r\n:2\r\n:3\r\n')
        assert proto.has_data()
        data = proto.gets()
        assert data == [1, 2, 3]
        if not siderpy.hiredis:
            assert proto._unparsed == b''
        assert not proto.has_data()

        proto.feed(b'*5\r\n:1\r\n:2\r\n:3\r\n:4\r\n$6\r\nfoobar\r\n')
        assert proto.has_data()
        data = proto.gets()
        assert data == [1, 2, 3, 4, b'foobar']
        if not siderpy.hiredis:
            assert proto._unparsed == b''
        assert not proto.has_data()

    def test_parse_null_array(self, proto):
        proto.feed(b'*-1\r\n')
        assert proto.has_data()
        data = proto.gets()
        assert data is None
        if not siderpy.hiredis:
            assert proto._unparsed == b''
        assert not proto.has_data()

    def test_parse_big_incomplete_array(self, proto):
        raw = (b'*1000\r\n$6\r\nvalue0\r\n$6\r\nvalue1\r\n$6\r\nvalue2\r\n$6\r\nvalue3\r\n$6\r\nvalue4\r\n'
               b'$6\r\nvalue5\r\n$6\r\nvalue6\r\n$6\r\nvalue7\r\n$6\r\nvalue8\r\n$6\r\nvalue9\r\n$7\r\n'
               b'value10\r\n$7\r\nvalue11\r\n$7\r\nvalue12\r\n$7\r\nvalue13\r\n$7\r\nvalue14\r\n$7\r\n'
               b'value15\r\n$7\r\nvalue16\r\n$7\r\nvalue17\r\n$7\r\nvalue18\r\n$7\r\nvalue19\r\n$7\r\n'
               b'value20\r\n$7\r\nvalue21\r\n$7\r\nvalue22\r\n$7\r\nvalue23\r\n$7\r\nvalue24\r\n$7\r\n'
               b'value25\r\n$7\r\nvalue26\r\n$7\r\nvalue27\r\n$7\r\nvalue28\r\n$7\r\nvalue29\r\n$7\r\n'
               b'value30\r\n$7\r\nvalue31\r\n$7\r\nvalue32\r\n$7\r\nvalue33\r\n$7\r\nvalue34\r\n$7\r\n'
               b'value35\r\n$7\r\nvalue36\r\n$7\r\nvalue37\r\n$7\r\nvalue38\r\n$7\r\nvalue39\r\n$7\r\n'
               b'value40\r\n$7\r\nvalue41\r\n$7\r\nvalue42\r\n$7\r\nvalue43\r\n$7\r\nvalue44\r\n$7\r\n'
               b'value45\r\n$7\r\nvalue46\r\n$7\r\nvalue47\r\n$7\r\nvalue48\r\n$7\r\nvalue49\r\n$7\r\n'
               b'value50\r\n$7\r\nvalue51\r\n$7\r\nvalue52\r\n$7\r\nvalue53\r\n$7\r\nvalue54\r\n$7\r\n'
               b'value55\r\n$7\r\nvalue56\r\n$7\r\nvalue57\r\n$7\r\nvalue58\r\n$7\r\nvalue59\r\n$7\r\n'
               b'value60\r\n$7\r\nvalue61\r\n$7\r\nvalue62\r\n$7\r\nvalue63\r\n$7\r\nvalue64\r\n$7\r\n'
               b'value65\r\n$7\r\nvalue66\r\n$7\r\nvalue67\r\n$7\r\nvalue68\r\n$7\r\nvalue69\r\n$7\r\n'
               b'value70\r\n$7\r\nvalue71\r\n$7\r\nvalue72\r\n$7\r\nvalue73\r\n$7\r\nvalue74\r\n$7\r\n'
               b'value75\r\n$7\r\nvalue76\r\n$7\r\nvalue77\r\n$7\r\nvalue78\r\n')
        proto.feed(raw)
        assert proto.gets() is False
        assert not proto.has_data()

    def test_parse_array_in_two_steps(self, proto):
        raw = b'*5\r\n$6\r\nvalue0\r\n$6\r\nvalue1\r\n$6\r\nvalue2\r\n$6\r\nvalue3'
        proto.feed(raw)
        assert proto.gets() is False
        assert proto.has_data()
        raw = b'\r\n$6\r\nvalue4\r\n'
        proto.feed(raw)
        assert proto.gets() == [b'value0', b'value1', b'value2', b'value3', b'value4']
        assert not proto.has_data()

    def test_parse_array_array(self, proto):
        proto.feed(b'*2\r\n*2\r\n:1\r\n:2\r\n*2\r\n+Foo\r\n-Bar\r\n')
        assert proto.has_data()
        data = proto.gets()
        assert data[0] == [1, 2]
        assert data[1][0] == b'Foo'
        if not siderpy.hiredis:
            assert isinstance(data[1][1], siderpy.RedisError)
            assert proto._unparsed == b''
        else:
            assert isinstance(data[1][1], siderpy.hiredis.ReplyError)
        assert not proto.has_data()

        proto.feed(b'*3\r\n$3\r\nfoo\r\n$-1\r\n$3\r\nbar\r\n')
        assert proto.has_data()
        data = proto.gets()
        assert data == [b'foo', None, b'bar']
        if not siderpy.hiredis:
            assert proto._unparsed == b''
        assert not proto.has_data()

    def test_parse_unparsed(self, proto):
        proto.feed(b'*3\r\n:1\r\n:2\r\n:3\r\n$6\r\n')
        assert proto.has_data()
        data = proto.gets()
        assert data == [1, 2, 3]
        if not siderpy.hiredis:
            assert proto._unparsed == b'$6\r\n'
        assert proto.has_data()
        assert proto.gets() is False

    def test_parse(self, proto):
        proto.feed(b'$6\r\nfoobar\r\n:1000\r\n')
        assert proto.has_data()
        data = proto.gets()
        assert data == b'foobar'
        assert proto.has_data()
        data = proto.gets()
        assert data == 1000
        if not siderpy.hiredis:
            assert proto._unparsed == b''
        assert not proto.has_data()

        proto.feed(b'$6\r\nfoobar\r\n:1000\r\n$6\r\n')
        assert proto.has_data()
        data = proto.gets()
        assert data == b'foobar'
        assert proto.has_data()
        data = proto.gets()
        assert data == 1000
        assert proto.has_data()
        if not siderpy.hiredis:
            assert proto._unparsed == b'$6\r\n'

    def test_make_cmd(self, proto):
        assert proto.make_cmd(
                'get', ['key']) == b'*2\r\n$3\r\nget\r\n$3\r\nkey\r\n'
        assert proto.make_cmd(
                'set', ['key', 'value']) == b'*3\r\n$3\r\nset\r\n$3\r\nkey\r\n$5\r\nvalue\r\n'
        assert proto.make_cmd(
                'set', ['key', b'value']) == b'*3\r\n$3\r\nset\r\n$3\r\nkey\r\n$5\r\nvalue\r\n'
        assert proto.make_cmd(
                'set', ['key', 1]) == b'*3\r\n$3\r\nset\r\n$3\r\nkey\r\n$1\r\n1\r\n'


pytestmark = pytest.mark.asyncio


@pytest.fixture
def event_loop(scope='session'):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        yield loop
    finally:
        loop.close()


@pytest.fixture()
def redis():
    ssl_ctx = None
    if USE_SSL:
        ssl_ctx = ssl.create_default_context()
        ssl_ctx.check_hostname = False
        ssl_ctx.load_verify_locations(os.path.join(os.path.dirname(__file__), 'domain.crt'))
    redis = siderpy.Redis(REDIS_HOST, port=REDIS_PORT, ssl_ctx=ssl_ctx)
    try:
        yield redis
    finally:
        redis.close_connection()


@pytest.fixture()
async def prepare(redis):
    await redis.flushall()
    yield


@pytest.fixture()
def pool():
    ssl_ctx = None
    if USE_SSL:
        ssl_ctx = ssl.create_default_context()
        ssl_ctx.check_hostname = False
        ssl_ctx.load_verify_locations(os.path.join(os.path.dirname(__file__), 'domain.crt'))
    pool = siderpy.RedisPool(REDIS_HOST, port=REDIS_PORT, ssl_ctx=ssl_ctx, pool_size=4)
    try:
        yield pool
    finally:
        pool.close_connections()


@pytest.fixture()
async def aio_redis():
    ssl_ctx = None
    if USE_SSL:
        ssl_ctx = ssl.create_default_context()
        ssl_ctx.check_hostname = False
        ssl_ctx.load_verify_locations(os.path.join(os.path.dirname(__file__), 'domain.crt'))
    aio_redis = await aioredis.create_redis_pool('redis://{}:{}'.format(REDIS_HOST, REDIS_PORT), ssl=ssl_ctx)
    yield aio_redis


class TestRedis:

    async def test_quit(self, event_loop, prepare, redis):
        resp = await redis.quit()
        assert resp == b'OK'

    async def test_del(self, event_loop, prepare, redis):
        resp = await redis.delete('key')
        assert resp == 0

    async def test_redis_list(self, event_loop, prepare, redis):
        resp = await redis.client('list')
        assert b'db=0' in resp

    async def test_select(self, event_loop, prepare, redis):
        resp = await redis.select(1)
        assert resp == b'OK'
        resp = await redis.client('list')
        assert b'db=1' in resp

    async def test_append(self, event_loop, prepare, redis):
        resp = await redis.append('key', 'value')
        assert resp == 5
        resp = await redis.append('key', 'value')
        assert resp == 10

    async def test_auth(self, event_loop, prepare, redis):
        with pytest.raises(siderpy.RedisError, match='ERR Client sent AUTH, but no password is set'):
            await redis.auth('password')

    async def test_set_get(self, event_loop, prepare, redis):
        resp = await redis.set('key', 'value')
        assert resp == b'OK'
        resp = await redis.get('not_exist_key')
        assert resp is None
        resp = await redis.get('key')
        assert resp == b'value'

    async def test_set_ex_get(self, event_loop, prepare, redis):
        resp = await redis.set('key', 'value', 'PX', 10)
        assert resp == b'OK'
        await asyncio.sleep(0.2)
        resp = await redis.get('key')
        assert resp is None

    async def test_bitpos(self, event_loop, prepare, redis):
        resp = await redis.set('mykey', b'\xff\xf0\x00')
        assert resp == b'OK'
        resp = await redis.bitpos('mykey', 0)
        assert resp == 12
        resp = await redis.set('mykey', b'\x00\xff\xf0')
        assert resp == b'OK'
        resp = await redis.bitpos('mykey', 1, 0)
        assert resp == 8
        resp = await redis.bitpos('mykey', 1, 2)
        assert resp == 16
        resp = await redis.set('mykey', b'\x00\x00\x00')
        assert resp == b'OK'
        resp = await redis.bitpos('mykey', 1)
        assert resp == -1

    async def test_rpush_lpop(self, event_loop, prepare, redis):
        resp = await redis.rpush('list', 'a', 'b', 'c')
        resp = await redis.lpop('list')
        assert resp == b'a'

    async def test_rpush_rpop(self, event_loop, prepare, redis):
        resp = await redis.rpush('list', 'a', 'b', 'c')
        resp = await redis.rpop('list')
        assert resp == b'c'

    async def test_blpop(self, event_loop, prepare, pool):

        async def push_coro():
            await asyncio.sleep(1)
            await pool.lpush('list', 'value')

        asyncio.create_task(push_coro())

        resp = await pool.blpop('list', 10000)
        assert resp == [b'list', b'value']

    async def test_xadd(self, event_loop, prepare, redis):
        resp = await redis.xadd(*'mystream 111 sensor-id 1234 temperature 19.8'.split())
        assert resp == b'111-0'

    async def test_multi(self, event_loop, prepare, redis):
        await redis.multi()
        await redis.set('key1', 'value1')
        await redis.set('key2', 'value2')
        await redis.keys('*')
        resp = await redis.execute()
        assert len(resp) == 3
        assert resp[:2] == [b'OK', b'OK']
        assert sorted(resp[2]) == [b'key1', b'key2']

    async def test_pool_multi(self, event_loop, prepare, pool):
        async with pool.get_one() as redis:
            await redis.multi()
            await redis.set('key1', 'value1')
            await redis.set('key2', 'value2')
            await redis.keys('*')
            resp = await redis.execute()
        assert len(resp) == 3
        assert resp[:2] == [b'OK', b'OK']
        assert sorted(resp[2]) == [b'key1', b'key2']

    async def test_pipeline(self, event_loop, prepare, redis):
        async with redis.pipeline() as resp:
            await redis.set('key', 'value')
            await redis.ping()
            await redis.ping()
            await redis.get('key')
        assert resp == [b'OK', b'PONG', b'PONG', b'value']

    async def test_pool_pipeline(self, event_loop, prepare, pool):
        async with pool.get_one() as redis:
            async with redis.pipeline() as resp:
                await redis.set('key', 'value')
                await redis.ping()
                await redis.ping()
                await redis.get('key')
        assert resp == [b'OK', b'PONG', b'PONG', b'value']

    async def test_pipeline_multi(self, event_loop, prepare, redis):
        async with redis.pipeline() as resp:
            await redis.multi()
            await redis.set('key1', 'value1')
            await redis.set('key2', 'value2')
            await redis.keys('*')
            await redis.execute()
        flag1 = resp == [b'OK', b'QUEUED', b'QUEUED', b'QUEUED', [b'OK', b'OK', [b'key1', b'key2']]]
        flag2 = resp == [b'OK', b'QUEUED', b'QUEUED', b'QUEUED', [b'OK', b'OK', [b'key2', b'key1']]]
        assert flag1 or flag2

    async def test_subscribe1(self, event_loop, prepare, redis):
        messages = []

        async def consumer(data):
            topic, channel, message = data
            messages.append(message)

        resp = await redis.subscribe(consumer, 'channel1', 'channel2')
        resp = await redis.unsubscribe()
        assert resp is None
        assert messages == []

    async def test_subscribe2(self, event_loop, prepare, pool):
        messages = []

        async def consumer(data):
            topic, channel, message = data
            messages.append(message)

        async def producer():
            async with pool.get_one() as redis:
                await redis.publish('channel1', 'message1')
                await redis.publish('channel1', 'message2')
                await redis.publish('channel2', 'message3')

        async with pool.get_one() as redis:
            await redis.subscribe(consumer, 'channel1', 'channel2')
            await asyncio.wait({asyncio.create_task(producer())})
            await asyncio.sleep(1)
            await redis.unsubscribe()
        assert messages == [b'message1', b'message2', b'message3']

    async def test_psubscribe1(self, event_loop, prepare, redis):
        messages = []

        async def consumer(data):
            topic, pattern, channel, message = data
            messages.append(message)

        resp = await redis.psubscribe(consumer, 'channel1.*', 'channel2.*')
        resp = await redis.punsubscribe()
        assert resp is None
        assert messages == []

    async def test_psubscribe2(self, event_loop, prepare, pool):
        messages = []

        async def consumer(data):
            topic, pattern, channel, message = data
            messages.append(message)

        async def producer():
            async with pool.get_one() as redis:
                await redis.publish('channel1.a', 'message1')
                await redis.publish('channel1.b', 'message2')
                await redis.publish('channel2.c', 'message3')

        async with pool.get_one() as redis:
            await redis.psubscribe(consumer, 'channel1.*', 'channel2.*')
            await asyncio.wait({asyncio.create_task(producer())})
            await asyncio.sleep(1)
            await redis.punsubscribe()
        assert messages == [b'message1', b'message2', b'message3']

    async def test_subscribe_psubscribe_mix(self, event_loop, prepare, pool):
        messages = []

        async def consumer(data):
            topic, *data = data
            messages.append(data[-1])

        async def producer():
            async with pool.get_one() as redis:
                await redis.publish('channel1', 'message1')
                await redis.publish('channel*', 'message2')

        async with pool.get_one() as redis:
            await redis.subscribe(consumer, 'channel1')
            await redis.psubscribe(consumer, 'channel*')
            await asyncio.wait({asyncio.create_task(producer())})
            await asyncio.sleep(1)
            await redis.unsubscribe()
            await redis.punsubscribe()
        assert messages == [b'message1', b'message1', b'message2']


class TestBenchmarks:

    count = 1000

    async def test_set_get(self, event_loop, prepare, redis):
        await redis.ping()
        keys = [f'key{i}' for i in range(self.count)]
        for _ in range(5):
            for i in range(self.count):
                await redis.set(f'key{i}', f'value{i}')
            for _ in range(self.count):
                await redis.mget(*keys)

    async def test_set_get_aioredis(self, event_loop, prepare, aio_redis):
        if siderpy.hiredis is None:
            return
        keys = [f'key{i}' for i in range(self.count)]
        for _ in range(5):
            for i in range(self.count):
                await aio_redis.set(f'key{i}', f'value{i}')
            for _ in range(self.count):
                await aio_redis.mget(*keys)
