SiderPy
=======

Minimalistic Python asyncio Redis client library

![CI](https://github.com/levsh/siderpy/workflows/CI/badge.svg)

Installation
------------

```bash
  $ pip install git+https://github.com/levsh/siderpy.git
```
or
```bash
  $ git clone https://github.com/levsh/siderpy.git
  $ cd siderpy
  $ python setup.py install
```

Examples
--------

```python
  import siderpy
    
  redis = siderpy.Redis('172.0.0.1')
  await redis.auth('secret_password')
  await redis.select(1)
  await redis.set('key', 'value')
  value = await redis.get('key')
```

Multi
```python
  import siderpy
  
  redis = siderpy.Redis('172.0.0.1')
  await redis.multi()
  await redis.set('key1', 'value1')
  await redis.set('key2', 'value2')
  await redis.get('key2')
  resp = await cli.execute()
```

Pipeline
```python
  import siderpy
  
  redis = siderpy.Redis('172.0.0.1')
  async with redis.pipeline() as response:
    await redis.ping()
    for key, value in {'key1': 'value1', 'key2': 'value2'}.items():
        await redis.set(key, value)
  print(response)
```
