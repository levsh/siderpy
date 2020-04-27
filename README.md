# SiderPy

Minimalistic Python asyncio Redis client library

**master** ![tests](https://github.com/levsh/siderpy/workflows/tests/badge.svg)

## Installation

hiredis support
```bash
    pip install git+https://github.com/levsh/siderpy.git#egg=siderpy[hiredis]
```

or pure python
```bash
  $ pip install git+https://github.com/levsh/siderpy.git
```

## Examples

Basic
```python
In [1]: import siderpy                                                                                                                                                                                

In [2]: redis = siderpy.Redis('localhost', port=6379)                                                                                                                                                   

In [3]: await redis.select(1)                                                                                                                                                                           
Out[3]: b'OK'

In [4]: await redis.set('key', 'value')                                                                                                                                                                 
Out[4]: b'OK'

In [5]: await redis.get('key')                                                                                                                                                                          
Out[5]: b'value'

In [6]: redis.close_connection()
```

Multi
```python
In [1]: import siderpy                                                                                                                                                                                

In [2]: redis = siderpy.Redis('localhost', port=6379)                                                                                                                                                   

In [3]: await redis.multi()                                                                                                                                                                             
Out[3]: b'OK'

In [4]: await redis.set('key1', 'value1')                                                                                                                                                               
Out[4]: b'QUEUED'

In [5]: await redis.set('key2', 'value2')                                                                                                                                                               
Out[5]: b'QUEUED'

In [6]: await redis.execute()                                                                                                                                                                           
Out[6]: [b'OK', b'OK']

In [6]: redis.close_connection()
```

Pipeline
```python
In [1]: import siderpy                                                                                                                                                                                

In [2]: redis = siderpy.Redis('localhost', port=6379)                                                                                                                                                   

In [3]: redis.pipeline_on()                                                                                                                                                                             

In [4]: await redis.ping()                                                                                                                                                                              

In [5]: await redis.set('key', 'value')                                                                                                                                                                 

In [6]: await redis.get('key')                                                                                                                                                                          

In [7]: await redis.pipeline_execute()                                                                                                                                                                  
Out[7]: [b'PONG', b'OK', b'value']

In [8]: redis.pipeline_off()                                                                                                                                                                            

In [9]: redis.close_connection()
```

Pub/Sub
```python
NotImplementedError
```

Sentinel
```python
In [1]: import siderpy                                                                                                                                                                                

In [2]: redis = siderpy.Redis('localhost', port=26379)                                                                                                                                                  

In [3]: await redis.sentinel('masters')                                                                                                                                                                 
Out[3]: 
[[b'name',
  b'mymaster',
  b'ip',
  b'127.0.0.1',
  b'port',
  b'6379',
  ...

In [4]: redis.close_connection()
```

Pool
```python

In [1]: import siderpy                                                                                                                                                                                

In [2]: pool = siderpy.RedisPool('localhost', port=6379, size=10)                                                                                                                                     

In [3]: await pool.ping()                                                                                                                                                                             
Out[3]: b'PONG'

In [4]: await pool.set('key', 'value')                                                                                                                                                                
Out[4]: b'OK'

In [5]: await pool.get('key')                                                                                                                                                                         
Out[5]: b'value'

In [6]: # or                                                                                                                                                                                          

In [7]: async with pool.get_redis() as redis: 
   ...:     print(await redis.ping())                                                                                                                                                                 
b'PONG'

In [8]: pool.close_connections()
```
