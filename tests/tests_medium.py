import asyncio
import os
import ssl

import pytest

import siderpy

siderpy.LOG.setLevel('INFO')


REDIS_HOST = os.environ.get('REDIS_HOST', 'redis')
REDIS_PORT = os.environ.get('REDIS_PORT', 6379)
TESTS_USE_SSL = os.environ.get('TESTS_USE_SSL')


pytestmark = pytest.mark.asyncio


@pytest.fixture(scope='function')
def event_loop():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        yield loop
    finally:
        loop.close()


@pytest.fixture(scope='function')
def redis():
    ssl_ctx = None
    if TESTS_USE_SSL:
        ssl_ctx = ssl.create_default_context()
        ssl_ctx.check_hostname = False
        ssl_ctx.load_verify_locations(os.path.join(os.path.dirname(__file__), 'domain.crt'))
    redis = siderpy.Redis(REDIS_HOST, port=REDIS_PORT, ssl_ctx=ssl_ctx)
    try:
        yield redis
    finally:
        redis.close_connection()


@pytest.fixture(scope='function')
async def prepare(event_loop, redis):
    await redis.flushall()
    yield


@pytest.fixture(scope='function')
def pool():
    ssl_ctx = None
    if TESTS_USE_SSL:
        ssl_ctx = ssl.create_default_context()
        ssl_ctx.check_hostname = False
        ssl_ctx.load_verify_locations(os.path.join(os.path.dirname(__file__), 'domain.crt'))
    pool = siderpy.RedisPool(REDIS_HOST, port=REDIS_PORT, ssl_ctx=ssl_ctx, size=4)
    try:
        yield pool
    finally:
        pool.close_connections()


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
        async with pool.get_redis() as redis:
            await redis.multi()
            await redis.set('key1', 'value1')
            await redis.set('key2', 'value2')
            await redis.keys('*')
            resp = await redis.execute()
        assert len(resp) == 3
        assert resp[:2] == [b'OK', b'OK']
        assert sorted(resp[2]) == [b'key1', b'key2']

    async def test_pipeline(self, event_loop, prepare, redis):
        with redis.pipeline():
            await redis.set('key', 'value')
            await redis.ping()
            await redis.ping()
            await redis.get('key')
            resp = await redis.pipeline_execute()
        assert resp == [b'OK', b'PONG', b'PONG', b'value']

    async def test_pool_pipeline(self, event_loop, prepare, pool):
        async with pool.get_redis() as redis:
            with redis.pipeline():
                await redis.set('key', 'value')
                await redis.ping()
                await redis.ping()
                await redis.get('key')
                resp = await redis.pipeline_execute()
        assert resp == [b'OK', b'PONG', b'PONG', b'value']

    async def test_pipeline_multi(self, event_loop, prepare, redis):
        with redis.pipeline():
            await redis.multi()
            await redis.set('key1', 'value1')
            await redis.set('key2', 'value2')
            await redis.keys('*')
            await redis.execute()
            resp = await redis.pipeline_execute()
        flag1 = resp == [b'OK', b'QUEUED', b'QUEUED', b'QUEUED', [b'OK', b'OK', [b'key1', b'key2']]]
        flag2 = resp == [b'OK', b'QUEUED', b'QUEUED', b'QUEUED', [b'OK', b'OK', [b'key2', b'key1']]]
        assert flag1 or flag2, resp

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
            async with pool.get_redis() as redis:
                await redis.publish('channel1', 'message1')
                await redis.publish('channel1', 'message2')
                await redis.publish('channel2', 'message3')

        async with pool.get_redis() as redis:
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
            async with pool.get_redis() as redis:
                await redis.publish('channel1.a', 'message1')
                await redis.publish('channel1.b', 'message2')
                await redis.publish('channel2.c', 'message3')

        async with pool.get_redis() as redis:
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
            async with pool.get_redis() as redis:
                await redis.publish('channel1', 'message1')
                await redis.publish('channel*', 'message2')

        async with pool.get_redis() as redis:
            await redis.subscribe(consumer, 'channel1')
            await redis.psubscribe(consumer, 'channel*')
            await asyncio.wait({asyncio.create_task(producer())})
            await asyncio.sleep(1)
            await redis.unsubscribe()
            await redis.punsubscribe()
        assert messages == [b'message1', b'message1', b'message2']
