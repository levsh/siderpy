import asyncio
import os
import ssl

import pytest
import pytest_asyncio

import siderpy

siderpy.LOG.setLevel("DEBUG")


REDIS_HOST = os.environ.get("REDIS_HOST", "redis")
REDIS_PORT = os.environ.get("REDIS_PORT", "6379")
TESTS_USE_SSL = os.environ.get("TESTS_USE_SSL")


pytestmark = pytest.mark.asyncio


@pytest.fixture(scope="function")
def event_loop():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        yield loop
    finally:
        loop.close()


@pytest_asyncio.fixture(scope="function")
async def redis():
    ssl_ctx = None
    if TESTS_USE_SSL:
        ssl_ctx = ssl.create_default_context()
        ssl_ctx.check_hostname = False
        ssl_ctx.load_verify_locations(os.path.join(os.path.dirname(__file__), "domain.crt"))
    redis = siderpy.Redis(f"redis://{REDIS_HOST}:{REDIS_PORT}", ssl_ctx=ssl_ctx)
    try:
        yield redis
    finally:
        await redis.close()


@pytest_asyncio.fixture(scope="function")
async def prepare(event_loop, redis):
    await redis.flushall()
    yield


@pytest_asyncio.fixture(scope="function")
async def pool():
    ssl_ctx = None
    if TESTS_USE_SSL:
        ssl_ctx = ssl.create_default_context()
        ssl_ctx.check_hostname = False
        ssl_ctx.load_verify_locations(os.path.join(os.path.dirname(__file__), "domain.crt"))
    pool = siderpy.RedisPool(f"redis://{REDIS_HOST}:{REDIS_PORT}", ssl_ctx=ssl_ctx, size=4)
    try:
        yield pool
    finally:
        await pool.close()


class TestRedis:
    async def test_quit(self, event_loop, prepare, redis):
        resp = await redis.quit()
        assert resp == b"OK"

    async def test_del(self, event_loop, prepare, redis):
        resp = await redis.delete("key")
        assert resp == 0

    async def test_redis_list(self, event_loop, prepare, redis):
        resp = await redis.client("list")
        assert b"db=0" in resp

    async def test_select(self, event_loop, prepare, redis):
        resp = await redis.select(1)
        assert resp == b"OK"
        resp = await redis.client("list")
        assert b"db=1" in resp

    async def test_append(self, event_loop, prepare, redis):
        resp = await redis.append("key", "value")
        assert resp == 5
        resp = await redis.append("key", "value")
        assert resp == 10

    async def test_auth(self, event_loop, prepare, redis):
        with pytest.raises(
            siderpy.RedisError,
            match=(
                "ERR AUTH <password> called without any password configured for the default user. "
                "Are you sure your configuration is correct?"
            ),
        ):
            await redis.auth("password")

    async def test_set_get(self, event_loop, prepare, redis):
        resp = await redis.set("key", "value")
        assert resp == b"OK"
        resp = await redis.get("not_exist_key")
        assert resp is None
        resp = await redis.get("key")
        assert resp == b"value"

    async def test_set_ex_get(self, event_loop, prepare, redis):
        resp = await redis.set("key", "value", "PX", 10)
        assert resp == b"OK"
        await asyncio.sleep(0.2)
        resp = await redis.get("key")
        assert resp is None

    async def test_bitpos(self, event_loop, prepare, redis):
        resp = await redis.set("mykey", b"\xff\xf0\x00")
        assert resp == b"OK"
        resp = await redis.bitpos("mykey", 0)
        assert resp == 12
        resp = await redis.set("mykey", b"\x00\xff\xf0")
        assert resp == b"OK"
        resp = await redis.bitpos("mykey", 1, 0)
        assert resp == 8
        resp = await redis.bitpos("mykey", 1, 2)
        assert resp == 16
        resp = await redis.set("mykey", b"\x00\x00\x00")
        assert resp == b"OK"
        resp = await redis.bitpos("mykey", 1)
        assert resp == -1

    async def test_rpush_lpop(self, event_loop, prepare, redis):
        resp = await redis.rpush("list", "a", "b", "c")
        resp = await redis.lpop("list")
        assert resp == b"a"

    async def test_rpush_rpop(self, event_loop, prepare, redis):
        resp = await redis.rpush("list", "a", "b", "c")
        resp = await redis.rpop("list")
        assert resp == b"c"

    async def test_multi(self, event_loop, prepare, redis):
        await redis.multi()
        await redis.set("key1", "value1")
        await redis.set("key2", "value2")
        await redis.keys("*")
        resp = await redis.execute()
        assert len(resp) == 3
        assert resp[:2] == [b"OK", b"OK"]
        assert sorted(resp[2]) == [b"key1", b"key2"]

    async def test_xadd(self, event_loop, prepare, redis):
        resp = await redis.xadd(*"mystream 111 sensor-id 1234 temperature 19.8".split())
        assert resp == b"111-0"

    async def test_pipeline(self, event_loop, prepare, redis):
        with redis.pipeline():
            await redis.set("key", "value")
            await redis.ping()
            await redis.ping()
            await redis.get("key")
            resp = await redis.pipeline_execute()
        assert resp == [b"OK", b"PONG", b"PONG", b"value"]

    async def test_pipeline_multi(self, event_loop, prepare, redis):
        with redis.pipeline():
            await redis.multi()
            await redis.set("key1", "value1")
            await redis.set("key2", "value2")
            await redis.keys("*")
            await redis.execute()
            resp = await redis.pipeline_execute()
        flag1 = resp == [b"OK", b"QUEUED", b"QUEUED", b"QUEUED", [b"OK", b"OK", [b"key1", b"key2"]]]
        flag2 = resp == [b"OK", b"QUEUED", b"QUEUED", b"QUEUED", [b"OK", b"OK", [b"key2", b"key1"]]]
        assert flag1 or flag2, resp

    async def test_blpop(self, event_loop, prepare, pool):
        async def push_coro():
            await asyncio.sleep(1)
            await pool.lpush("list", "value")

        asyncio.create_task(push_coro())

        resp = await pool.blpop("list", 10000)
        assert resp == [b"list", b"value"]

    async def test_pool_multi(self, event_loop, prepare, pool):
        async with pool.get_redis() as redis:
            await redis.multi()
            await redis.set("key1", "value1")
            await redis.set("key2", "value2")
            await redis.keys("*")
            resp = await redis.execute()
        assert len(resp) == 3
        assert resp[:2] == [b"OK", b"OK"]
        assert sorted(resp[2]) == [b"key1", b"key2"]

    async def test_pool_pipeline(self, event_loop, prepare, pool):
        async with pool.get_redis() as redis:
            with redis.pipeline():
                await redis.set("key", "value")
                await redis.ping()
                await redis.ping()
                await redis.get("key")
                resp = await redis.pipeline_execute()
        assert resp == [b"OK", b"PONG", b"PONG", b"value"]

    async def test_subscribe1(self, event_loop, prepare, redis):
        resp = await redis.subscribe("channel1", "channel2")
        assert resp == [[b"subscribe", b"channel1", 1], [b"subscribe", b"channel2", 2]]
        assert redis._listener is not None
        resp = await redis.unsubscribe()
        assert resp == [[b"unsubscribe", b"channel1", 1], [b"unsubscribe", b"channel2", 0]] or resp == [
            [b"unsubscribe", b"channel2", 1],
            [b"unsubscribe", b"channel1", 0],
        ]
        assert redis._listener is None

    async def test_subscribe2(self, event_loop, prepare, pool):
        async def producer():
            async with pool.get_redis() as redis:
                await redis.publish("channel1", "message1")
                await redis.publish("channel1", "message2")
                await redis.publish("channel2", "message3")

        messages = []
        async with pool.get_redis() as redis:
            await redis.subscribe("channel1", "channel2")
            asyncio.create_task(producer())

            async def consume():
                async for message in redis:
                    messages.append(message)
                    if len(messages) == 3:
                        await redis.unsubscribe()

            await asyncio.wait_for(consume(), 5)
        assert messages == [
            [b"message", b"channel1", b"message1"],
            [b"message", b"channel1", b"message2"],
            [b"message", b"channel2", b"message3"],
        ]

    async def test_psubscribe1(self, event_loop, prepare, redis):
        resp = await redis.psubscribe("channel1.*", "channel2.*")
        assert resp == [[b"psubscribe", b"channel1.*", 1], [b"psubscribe", b"channel2.*", 2]]
        assert redis._listener is not None
        resp = await redis.punsubscribe()
        assert resp == [[b"punsubscribe", b"channel1.*", 1], [b"punsubscribe", b"channel2.*", 0]] or resp == [
            [b"punsubscribe", b"channel1.*", 0],
            [b"punsubscribe", b"channel2.*", 1],
        ]
        assert redis._listener is None

    async def test_psubscribe2(self, event_loop, prepare, pool):
        async def producer():
            async with pool.get_redis() as redis:
                await redis.publish("channel1.1", "message1")
                await redis.publish("channel1.2", "message2")
                await redis.publish("channel2.1", "message3")

        messages = []
        async with pool.get_redis() as redis:
            await redis.psubscribe("channel1.*", "channel2.*")
            asyncio.create_task(producer())

            async def consume():
                async for message in redis:
                    messages.append(message)
                    if len(messages) == 3:
                        await redis.punsubscribe()

            await asyncio.wait_for(consume(), 5)
        assert messages == [
            [b"pmessage", b"channel1.*", b"channel1.1", b"message1"],
            [b"pmessage", b"channel1.*", b"channel1.2", b"message2"],
            [b"pmessage", b"channel2.*", b"channel2.1", b"message3"],
        ]

    async def test_subscribe_psubscribe_mix(self, event_loop, prepare, pool):
        async def producer():
            async with pool.get_redis() as redis:
                await redis.publish("channel1", "message1")
                await redis.publish("channel*", "message2")

        messages = []
        async with pool.get_redis() as redis:
            await redis.subscribe("channel1")
            await redis.psubscribe("channel*")
            asyncio.create_task(producer())

            async def consume():
                async for message in redis:
                    messages.append(message)
                    if len(messages) == 3:
                        await redis.unsubscribe()
                        await redis.punsubscribe()

            await asyncio.wait_for(consume(), 5)
        assert messages == [
            [b"message", b"channel1", b"message1"],
            [b"pmessage", b"channel*", b"channel1", b"message1"],
            [b"pmessage", b"channel*", b"channel*", b"message2"],
        ]

    async def test_subscribe_and_ping(self, event_loop, prepare, pool):
        async with pool.get_redis() as redis:
            queue = redis.pubsub_queue
            resp = await redis.subscribe("channel1", "channel2")
            assert resp == [[b"subscribe", b"channel1", 1], [b"subscribe", b"channel2", 2]]

            await pool.publish("channel1", "Hello World!")

            resp = await redis.ping()
            assert resp == b"pong"

            resp = await redis.unsubscribe()
            assert resp == [[b"unsubscribe", b"channel1", 1], [b"unsubscribe", b"channel2", 0]] or resp == [
                [b"unsubscribe", b"channel2", 1],
                [b"unsubscribe", b"channel1", 0],
            ]

            messages = [message async for message in queue]
            assert messages == [[b"message", b"channel1", b"Hello World!"]]

    async def test_subscribe_exc(self, event_loop, prepare, pool):
        async with pool.get_redis() as redis:
            await redis.subscribe("channel1", "channel2")
            with pytest.raises(
                siderpy.RedisError,
                match=(
                    r"ERR Can't execute 'get': only \(P|S\)SUBSCRIBE / \(P|S\)UNSUBSCRIBE / PING / QUIT / RESET "
                    "are allowed in this context"
                ),
            ):
                await redis.get("key")
            await redis.unsubscribe()

    async def test_pool_del(self, event_loop, prepare, pool):
        async with pool.get_redis() as redis:
            await redis.set("key", "value")
        resp = await pool.delete("key")
        assert resp == 1
