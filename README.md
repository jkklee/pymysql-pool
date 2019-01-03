# PyMySQL_Connection_Pool
A simple connection pool based PyMySQL. Mainly focus on **multi threads** mode when use `pymysql`, but also compatible with single thread mode for convenience when you need to use these two mode together. Within multi threads mode support the multiplexing similar feature(when use connection with `Context Manager Protocol`).

Problem: When use pymysql with python multi threads, generally we will face the questions:
1. It can't share a connection created by main thread with all sub-threads. It will result error like this:
`pymysql.err.InternalError: Packet sequence number wrong - got 0 expected 1`
2. If we make every sub-thread to create a connection and close it when this sub-thread end, that's workable but obviously lead to high cost on establish connections with MySQL.

So I implement this module aimed at create as least connections as possible with MySQL in multi-threads programing. 

This module contain two class: 
- `Connection` is a subclass of `pymysql.connections.Connection`, it can use with or without connection_pool, **It's usage is all the same with pymysql**. The detail(when with connection_pool, it should take additional action to maintain the pool) implement about connection pool is hiddened.  
This class provide a wrapped execute_query() method for convenience, which take several parameters.
- `ConnectionPool`'s instance represent the real connection_pool.

## Use example
### Install
```
pip install pymysql-pool
```
### multi-threads mode:  
The mainly difference with single-thread mode is that we should maintain the status of the pool. Such as 'get connection from pool' or 'put connection back to pool', in which case there are also some case to deal, such as: 
- when get connection from a pool: we should deal with the **timeout** and **retry** parameters
- when put connection back to pool: if we executed queries without exceptions, this connection can go back to pool directly; but if **exception** occurred, we should decided whether this connection should go back to pool depend on if it is **reusable**(base on the exception type). If the connection shouldn't bo back to pool, we close it and **recreate** a new connection then put it to the pool.

Luckily, this module will take care of these complicated details for you automatic.

There also can create more than one connection_pool(with distinct `ConnectionPool.name` attribute) to associate with different databases.

In the example below, we will see how it work within connection_pool feature:   
```
>>> import pymysqlpool
>>> pymysqlpool.logger.setLevel('DEBUG')
>>> config={'host':'xxxx', 'user':'xxx', 'password':'xxx', 'database':'xxx', 'autocommit':True}

### Create a connection pool with 2 connection in it
>>> pool1 = pymysqlpool.ConnectionPool(size=2, name='pool1', **config)
>>> pool1.size()
2
>>> con1 = pool1.get_connection()
2017-12-25 21:38:48    DEBUG: Get connection from pool(pool1)
>>> con2 = pool1.get_connection()
2017-12-25 21:38:51    DEBUG: Get connection from pool(pool1)
>>> pool1.size()
0

### We can prophesy that here will occur some exception, because the pool1 is empty
>>> con3 = pool1.get_connection(timeout=0, retry_num=0)
Traceback (most recent call last):
  File "e:\github\pymysql-pool\pymysqlpool.py", line 115, in get_connection
    conn = self._pool.get(timeout=timeout) if timeout > 0 else self._pool.get_nowait()
queue.Empty

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "<pyshell#37>", line 1, in <module>
    con3 = pool1.get_connection(timeout=0, retry_num=0)
  File "e:\github\pymysql-pool\pymysqlpool.py", line 128, in get_connection
    self.name, timeout, total_times))
pymysqlpool.GetConnectionFromPoolError: can't get connection from pool(pool1) within 0*1 second(s)

### Now let's see the connection's behavior when call close() method and use with Context Manager Protocol
>>> con1.close()
2017-12-25 21:39:56    DEBUG: Put connection back to pool(pool1)
>>> with con1 as cur:
	cur.execute('select 1+1')

1
2017-12-25 21:40:25    DEBUG: Put connection back to pool(pool1)
### We can see that the module maintain the pool appropriate when(and only when) we call the close() method or use the Context Manager Protocol of connection object.
```

**NOTE 1:** We should always use one of the close() method or Context Manager Protocol of connection object, otherwise the pool will exhaust soon.  
**NOTE 2:** The Context Manager Protocol is preferred, it can achieve the "multiplexing" similar effect.  
**NOTE 3:** When use close() method, take care never use a connection object's close() method more than one time(you know why~).
