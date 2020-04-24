import asyncio

import aioredis
import pytest

import siderpy

siderpy.LOG.setLevel('DEBUG')


REDIS = 'redis'


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
            assert proto._remain == b''
        assert not proto.has_data()

    def test_parse_simple_str_double(self, proto):
        proto.feed(b'+OK\r\n')
        proto.feed(b'+OK\r\n')
        assert proto.has_data()
        data = proto.gets()
        assert data == b'OK'
        if not siderpy.hiredis:
            assert proto._remain == b''
        assert proto.has_data()
        data = proto.gets()
        assert data == b'OK'
        if not siderpy.hiredis:
            assert proto._remain == b''
        assert not proto.has_data()

    def test_parse_err(self, proto):
        proto.feed(b'-Err\r\n')
        assert proto.has_data()
        with pytest.raises(siderpy.RedisError, match='Err'):
            proto.gets()
        if not siderpy.hiredis:
            assert proto._remain == b''
        assert not proto.has_data()

    def test_parse_int(self, proto):
        proto.feed(b':1000\r\n')
        assert proto.has_data()
        data = proto.gets()
        assert data == 1000
        if not siderpy.hiredis:
            assert proto._remain == b''
        assert not proto.has_data()

    def test_parse_bulk_str(self, proto):
        proto.feed(b'$6\r\nfoobar\r\n')
        assert proto.has_data()
        data = proto.gets()
        assert data == b'foobar'
        if not siderpy.hiredis:
            assert proto._remain == b''
        assert not proto.has_data()

    def test_parse_empty_str(self, proto):
        proto.feed(b'$0\r\n\r\n')
        assert proto.has_data()
        data = proto.gets()
        assert data == b''
        if not siderpy.hiredis:
            assert proto._remain == b''
        assert not proto.has_data()

    def test_parse_null(self, proto):
        proto.feed(b'$-1\r\n')
        assert proto.has_data()
        data = proto.gets()
        assert data is None
        if not siderpy.hiredis:
            assert proto._remain == b''
        assert not proto.has_data()

    def test_parse_empty_array(self, proto):
        proto.feed(b'*0\r\n')
        assert proto.has_data()
        data = proto.gets()
        assert data == []
        if not siderpy.hiredis:
            assert proto._remain == b''
        assert not proto.has_data()

    def test_parse_array(self, proto):
        proto.feed(b'*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n')
        assert proto.has_data()
        data = proto.gets()
        assert data == [b'foo', b'bar']
        if not siderpy.hiredis:
            assert proto._remain == b''
        assert not proto.has_data()

        proto.feed(b'*3\r\n:1\r\n:2\r\n:3\r\n')
        assert proto.has_data()
        data = proto.gets()
        assert data == [1, 2, 3]
        if not siderpy.hiredis:
            assert proto._remain == b''
        assert not proto.has_data()

        proto.feed(b'*5\r\n:1\r\n:2\r\n:3\r\n:4\r\n$6\r\nfoobar\r\n')
        assert proto.has_data()
        data = proto.gets()
        assert data == [1, 2, 3, 4, b'foobar']
        if not siderpy.hiredis:
            assert proto._remain == b''
        assert not proto.has_data()

    def test_parse_null_array(self, proto):
        proto.feed(b'*-1\r\n')
        assert proto.has_data()
        data = proto.gets()
        assert data is None
        if not siderpy.hiredis:
            assert proto._remain == b''
        assert not proto.has_data()

    def test_parse_array_array(self, proto):
        proto.feed(b'*2\r\n*2\r\n:1\r\n:2\r\n*2\r\n+Foo\r\n-Bar\r\n')
        assert proto.has_data()
        data = proto.gets()
        assert data[0] == [1, 2]
        assert data[1][0] == b'Foo'
        if not siderpy.hiredis:
            assert isinstance(data[1][1], siderpy.RedisError)
            assert proto._remain == b''
        else:
            assert isinstance(data[1][1], siderpy.hiredis.ReplyError)
        assert not proto.has_data()

        proto.feed(b'*3\r\n$3\r\nfoo\r\n$-1\r\n$3\r\nbar\r\n')
        assert proto.has_data()
        data = proto.gets()
        assert data == [b'foo', None, b'bar']
        if not siderpy.hiredis:
            assert proto._remain == b''
        assert not proto.has_data()

    def test_parse_remain(self, proto):
        proto.feed(b'*3\r\n:1\r\n:2\r\n:3\r\n$6\r\n')
        assert proto.has_data()
        data = proto.gets()
        assert data == [1, 2, 3]
        if not siderpy.hiredis:
            assert proto._remain == b'$6\r\n'
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
            assert proto._remain == b''
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
            assert proto._remain == b'$6\r\n'

    def test_make_cmd(self, proto):
        assert proto.make_cmd(
                'get', ['key']) == b'*2\r\n$3\r\nget\r\n$3\r\nkey\r\n'
        assert proto.make_cmd(
                'set', ['key', 'value']) == b'*3\r\n$3\r\nset\r\n$3\r\nkey\r\n$5\r\nvalue\r\n'
        assert proto.make_cmd(
                'set', ['key', b'value']) == b'*3\r\n$3\r\nset\r\n$3\r\nkey\r\n$5\r\nvalue\r\n'
        assert proto.make_cmd(
                'set', ['key', 1]) == b'*3\r\n$3\r\nset\r\n$3\r\nkey\r\n$1\r\n1\r\n'


@pytest.fixture
def event_loop(scope='session'):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    yield loop
    loop.close()


pytestmark = pytest.mark.asyncio


@pytest.fixture()
async def prepare():
    redis = siderpy.Redis(REDIS)
    await redis.flushall()
    yield


@pytest.fixture()
def redis():
    return siderpy.Redis(REDIS)


@pytest.fixture()
def pool():
    return siderpy.RedisPool(REDIS, pool_size=4)


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

    async def test_set_get(self, event_loop, prepare):
        redis = siderpy.Redis(REDIS)
        await redis.ping()
        keys = [f'key{i}' for i in range(self.count)]
        for _ in range(5):
            for i in range(self.count):
                await redis.set(f'key{i}', f'value{i}')
            for _ in range(self.count):
                await redis.mget(*keys)

    async def test_set_get_aioredis(self, event_loop, prepare):
        redis = await aioredis.create_redis_pool('redis://' + REDIS)
        keys = [f'key{i}' for i in range(self.count)]
        for _ in range(5):
            for i in range(self.count):
                await redis.set(f'key{i}', f'value{i}')
            for _ in range(self.count):
                await redis.mget(*keys)
