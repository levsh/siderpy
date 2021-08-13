# SiderPy

Minimalistic Python asyncio Redis client library

![tests](https://github.com/levsh/siderpy/workflows/tests/badge.svg)
[![Documentation Status](https://readthedocs.org/projects/siderpy/badge/?version=latest)](https://siderpy.readthedocs.io/en/latest/?badge=latest)
      
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

```python
In [1]: import siderpy                                                                                                                                                                                

In [2]: redis = siderpy.Redis('redis://localhost:6379')                                                                                                                                                   

In [3]: await redis.select(1)                                                                                                                                                                           
Out[3]: b'OK'

In [4]: await redis.set('key', 'value')                                                                                                                                                                 
Out[4]: b'OK'

In [5]: await redis.get('key')                                                                                                                                                                          
Out[5]: b'value'

In [6]: await redis.close()
```

## Documentation

[`siderpy.readthedocs.io`](https://siderpy.readthedocs.io/en/latest/)


## Benchmark

Benchmark test available at [`github workflow actions`](https://github.com/levsh/siderpy/actions?query=workflow%3Atests+branch%3Amaster)
step `Benchmark`.
