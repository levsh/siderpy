Getting Started
===============

Installation
------------

Make sure you have both `pip <https://pip.pypa.io/en/stable/installing/>`_ and at least version 3.6 of Python.

To install SiderPy with `hiredis <https://github.com/redis/hiredis-py>`_ support run

.. code-block:: bash

    pip install git+https://github.com/levsh/siderpy.git#egg=siderpy[hiredis]

or with pure python parser

.. code-block:: bash

    pip install git+https://github.com/levsh/siderpy.git#egg=siderpy

Basic usage
-----------

It's very simple.

SiderPy uses uri format to connect to the Redis server.

.. code-block:: python

    import siderpy

    redis = siderpy.Redis('redis://username:password@localhost:6379?db=0')

or in case of unix socket

.. code-block:: python

    redis = siderpy.Redis('redis+unix://username:password@/var/run/redis.sock?db=0')

It's possibly to specify connection open timeout, read/write timeout, ssl, and server data decoding.
For `__init__` method details see :py:meth:`API Reference <siderpy.Redis.__init__>`

Instead of opening connection to the server at the time :py:class:`~siderpy.Redis` object was created,
the connection is establish lazy at the first call.
It's allows to create :py:class:`~siderpy.Redis` instance inside `__init__` method.

:py:class:`~siderpy.Redis` class doesn't explicitly define Redis commands as methods of itself, 
except `execute` (exec) and `delete` (del), but calling command is same as calling instance method.

.. code-block:: python

    await redis.ping()
    await redis.ping('Hello!')
    await redis.set('key', 'value')
    response = await redis.get('key')

After the Redis object no longer needed call :py:meth:`~siderpy.Redis.close` method 
to close underlying connection to the server and free resources.

.. code-block:: python

    await redis.close()


Transactions with multi/exec
----------------------------

To use transaction just wraps your command into multi/exec block.

.. code-block:: python

    await redis.multi()
    await redis.set('key', 'value')
    ...
    await redis.execute()  # Redis 'exec' command

Pipeline
--------

To enable pipeline call :py:meth:`~siderpy.Redis.pipeline_on`.
After that all subsequent commands are saved in the internall buffer until :py:meth:`~siderpy.Redis.pipeline_off`
method is called. To execute stored buffer run :py:meth:`~siderpy.Redis.pipeline_execute`.

.. code-block:: python

    redis.pipeline_on()
    await redis.set('key1', 'value1')
    await redis.set('key2', 'value2')
    ...
    await redis.set('keyN', 'valueN')
    response = await redis.pipeline_execute()
    redis.pipeline_off()

    # or
    with redis.pipeline():
        ...
    await redis.pipeline_execute()

Publish/Subscribe
-----------------

Publish to a channel:

.. code-block:: python

    await redis.publish('channel', 'Hello World!')

Subscribe to a channel(s):

.. code-block:: python

    await redis.subscribe('channel1', 'channel2', ..., 'channelN')

To receive messages from subscribed channels just iterate over :py:class:`~siderpy.Redis` object.

.. code-block:: python

    async for message in redis:
        print(message)

or use :py:attr:`~siderpy.Redis.pubsub_queue` directly

.. code-block:: python

    # 1
    message = await redis.pubsub_queue.get() 

    # 2
    async for message in redis.pubsub_queue:
        ...

If a error occurs during consuming then it will be raised.

.. code-block:: python

    async for mesasge in redis:
        print(message)
    # connection error occurs

    Traceback (most recent call last):
      File "test.py", line 24, in <module>
        asyncio.run(main())
        ...
        raise ConnectionError
    ConnectionError


.. code-block:: python

    await redis.pubsub_queue.get()
    # connection error occurs

    Traceback (most recent call last):
      File "test.py", line 24, in <module>
        asyncio.run(main())
        ...
        raise ConnectionError
    ConnectionError

In this case it's necessary to resubscribe again to continue recieving messages.

Pool
----

:py:class:`~siderpy.Redis` class represents a signle network connection.
If yout need a pool of connections use :py:class:`~siderpy.RedisPool` or implement your own.
:py:class:`~siderpy.RedisPool` supports direct commands call except connection dependent commands such as
subscribe, psubscribe, unsubscribe, punsubscribe, multi, exec, discard, etc.

.. code-block:: python

    pool = siderpy.RedisPool('redis://localhost:6379?db=0')
    await pool.ping()
    await pool.get('key')

But it's recommended to get the :py:class:`~siderpy.Redis` object and use it

.. code-block:: python

    async with pool.get_redis() as redis:
        await redis.get('key')
        ...
