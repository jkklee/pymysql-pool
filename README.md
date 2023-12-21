# PyMySQL Connection Pool

[中文文档](https://github.com/jkklee/pymysql-pool/blob/master/README_zh.md)

A simple but not simple mysql connection pool based on `PyMySQL`.

## The problem to solve
While using pymysql with python multithreading, generally we will face the questions:  
1. It can't share a connection created by main thread with all sub-threads. It will result in the following error:  
    `pymysql.err.InternalError: Packet sequence number wrong - got 0 expected 1`  
2. If we make every sub-thread to create a connection and close it when this sub-thread ends that's workable but obviously lead to high cost on establishing connections with MySQL.

## Features
1. Simple: just use it, there is no extra learning costs.
2. Performance: almost no extra load compared to the original PyMysql([simple benchmark](https://github.com/jkklee/pymysql-pool#simple-benchmark)).
3. Flexible: pre_create connection or just create when really need; normal pool size and max pool size for the scalability, it all depends on you. 
4. Thoughtful: `connection lifetime` and `pre_ping` mechanism, in case of borrow a brokend connection from the pool(such as closed by the mysql server due to `wait_timeout` setting). 

## Basic components
This module contains two classes: 
- `Connection` class: this is a subclass of `pymysql.connections.Connection`. It can be used with or without a connection_pool, **It used in the exact same way as pymysql**. The details implementation of connection pool is hiddened (when used with a connection_pool additional actions are needed to maintain the pool).  
- `ConnectionPool` class: instance of this class represents the actual connection_pool.

## Misc
Using the concept of connection pool, there are also some aspects should be considered except the core features, such as:

- when getting connection from a pool: we should deal with the **retry_num** and **retry_interval** parameters，in order to give the borrower more chance and don't return the `GetConnectionFromPoolError` error directly.
- when putting connection back to pool: if the queries executed without exceptions, this connection can be putted back to the pool directly; but if **exception** occurred we have to decide whether this connection should be putted back to the pool depending on if it is **reusable** (depends on the exception type).

Luckily, this module will take care of these complicated details for you automaticly.

It also allows to create more than one connection_pool (with distinct `ConnectionPool.name` attribute) to be associated with different databases.

## Usage example
#### Installation
```
pip install pymysql-pool
```

In the example below we're going to see how it works:  

1. Create a pool with base/normal size is 2 and max size is 3, with pre_create_num=2 means will create 2 connections in the init phase:
    ```
    >>> import pymysqlpool
    >>> pymysqlpool.logger.setLevel('DEBUG')
    >>> config={'host':'xxxx', 'user':'xxx', 'password':'xxx', 'database':'xxx', 'autocommit':True}

    >>> mypool = pymysqlpool.ConnectionPool(size=2, maxsize=3, pre_create_num=2, name='mypool', **config)
    03-08 15:54:50    DEBUG: Create new connection in pool(mypool)
    03-08 15:54:50    DEBUG: Create new connection in pool(mypool)
    >>> mypool.total_num
    2

    >>> con1 = mypool.get_connection()
    12-25 21:38:48    DEBUG: Get connection from pool(mypool)
    >>> con2 = mypool.get_connection()
    12-25 21:38:51    DEBUG: Get connection from pool(mypool)
    >>> mypool.available_num
    0
    ```
2. Now the pool is empty, and we still borrow a connection from it, with the default parameters of get_connection(), we will see :
    ```
    >>> con3=mypool.get_connection()
    03-08 15:57:32    DEBUG: Retry to get connection from pool(mypool)
    03-08 15:57:32    DEBUG: Retry to get connection from pool(mypool)
    03-08 15:57:32    DEBUG: Retry to get connection from pool(mypool)
    03-08 15:57:33    DEBUG: Create new connection in pool(mypool)
    ```
    above message show us: although pool is empty, but the max size isn't reached, so after several times retry, a new connection is create(now max size of  pool is reached)

3. Let's try to get another connection from pool:

    ```
    >>> con4=mypool.get_connection()
    03-08 16:29:43    DEBUG: Retry to get connection from pool(mypool)
    03-08 16:29:43    DEBUG: Retry to get connection from pool(mypool)
    03-08 16:29:43    DEBUG: Retry to get connection from pool(mypool)
    Traceback (most recent call last):
    File "/Users/kai/github/pymysql-pool/pymysqlpool.py", line 176, in get_connection
        conn = self._pool.pop()
    IndexError: pop from an empty deque

    ... ...

    pymysqlpool.GetConnectionFromPoolError: can't get connection from pool(mypool), retry_interval=0.1(s)
    ```
    we can see that after several times retry, finally raise a exception `GetConnectionFromPoolError`

4. Now let's see the connection's behavior while calling close() method or using it with Context Manager Protocol

    ```
    >>> con1.close()
    2017-12-25 21:39:56    DEBUG: Put connection back to pool(mypool)
    >>> with con2:
            with con2.cursor() as cur:
                cur.execute('select 1+1')

    1
    12-20 22:44:37    DEBUG: Put connection back to pool(mypool)
    >>> mypool.total_num
    3  # as we expect
    >>> mypool.available_num
    2  # as we expect
We can see that the module maintains the pool appropriately when (and only when) we call the close() method or use the Context Manager Protocol of the connection object.

## Simple benchmark
I did a simple benchmark, focusing on the performance impact of the "extra" `get` and `return` operations in this module.  
The test logic is in the `simple-benchmark.py`, You can check and do it yourself.  
Below is my test(loop 50000 )
```
# 'pymysql-one-conn' is the best performing scenario, native pymysql, and all queries are done within a single connection
➜  pymysql-pool ✗ python3 simple-benchmark.py pymysql-one-conn 50000
total 50000 finish within 6.564s.
7616.86 queries per second, avg 0.13 ms per query
➜  pymysql-pool ✗ python3 simple-benchmark.py pymysql-one-conn 50000
total 50000 finish within 6.647s.
7522.31 queries per second, avg 0.13 ms per query
➜  pymysql-pool ✗ python3 simple-benchmark.py pymysql-one-conn 50000
total 50000 finish within 6.558s.
7623.71 queries per second, avg 0.13 ms per query
➜  pymysql-pool ✗ python3 simple-benchmark.py pymysql-one-conn 50000
total 50000 finish within 6.737s.
7421.67 queries per second, avg 0.13 ms per query

# 'pymysql-pool' uses connection pool (as long as the pool is greater than 1, it doesn't matter because the test logic is executed sequentially in a for loop).
➜  pymysql-pool ✗ python3 simple-benchmark.py pymysql-pool 50000
total 50000 finish within 6.999s.
7143.77 queries per second, avg 0.14 ms per query
➜  pymysql-pool ✗ python3 simple-benchmark.py pymysql-pool 50000
total 50000 finish within 7.066s.
7076.48 queries per second, avg 0.14 ms per query
➜  pymysql-pool ✗ python3 simple-benchmark.py pymysql-pool 50000
total 50000 finish within 6.999s.
7143.71 queries per second, avg 0.14 ms per query
➜  pymysql-pool ✗ python3 simple-benchmark.py pymysql-pool 50000
total 50000 finish within 6.968s.
7175.65 queries per second, avg 0.14 ms per query
```
As we can see that one time `get` plus `return` operation only takes about 0.01ms.

## Note
1. We should always use either the `close()` method or `Context Manager Protocol` of the connection object. Otherwise the pool will exhaust soon.

2. The `Context Manager Protocol` is preferred. It can achieve an effect similar to the "multiplexing", means the more Fine-Grained use of pool, also do more with less connections.
