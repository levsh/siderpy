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

    redis = siderpy.Redis('redis://localhost:6379?db=0')

or in case of unix socket

.. code-block:: python

    redis = siderpy.Redis('redis+unix///var/run/redis.sock')

SiderPy supports connection open timeout, read/write timeout, ssl, and server data decoding.
For `__init__` method details see :py:meth:`API Reference <siderpy.Redis.__init__>`

:py:meth:`~siderpy.Redis` class represents a signle network connection.
If yout need a pool of connections see :py:meth:`~siderpy.RedisPool` or implement your own.

Although :py:meth:`~siderpy.Redis` class doesn't explicitly define Redis commands as methods of itself, 
except `exec` (execute) and `del` (delete), calling command is as simple as calling instance method:

.. code-block:: python

    await redis.ping()
    await redis.ping('Hello!')
    await redis.set('key', 'value')
    response = await redis.get('key')

After the Redis object no longer needed call :py:meth:`~siderpy.Redis.close_connection` method 
to close underlying connection to the server and free resources:

.. code-block:: python

    redis.close_connection()


Transactions with multi/exec
----------------------------

To use transaction just wraps your command into multi/exec block

.. code-block:: python

    await redis.multi()
    await redis.set('key', 'value')
    ...
    await redis.execute()  # Redis 'exec' command

Publish/Subscribe
-----------------

Publish to a channel:

.. code-block:: python

    await redis.publish('channel', 'Hello World!')

Subscribe to a channel(s):

.. code-block:: python

    await redis.subscribe('channel1', 'channel2', ..., 'channelN')

To receive messages from subscribed channels just iterate over :py:meth:`~siderpy.Redis` object:

.. code-block:: python

    async for message in redis:
        print(message)

or use :py:meth:`~siderpy.Redis.pubsub_queue` directly:

.. code-block:: python

    while True:
        message = await redis.pubsub_queue.get() 

If a error occurs during consuming then it will raised to client code:

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

In this case it's necessary to resubscribe again to continue reciving messages.
