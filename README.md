# PyMySQL_Connection_Pool
A simple connection pool based on PyMySQL. It is mainly focused on **multi-thread** mode while using `pymysql` but also compatible with single-thread mode for convenience when you need to use these two modes together. Within multi thread-mode support the multiplexing similar feature (while using the connection with the `Context Manager Protocol`).

## The problem to solve
While using pymysql with python multithreading generally we will face the questions:  
    1. It can't share a connection created by main thread with all sub-threads. It will result in the following error:  
    `pymysql.err.InternalError: Packet sequence number wrong - got 0 expected 1`  
    2. If we make every sub-thread to create a connection and close it when this sub-thread ends that's workable but obviously lead to high cost on establishing connections with MySQL.

So I implemented this module aimed to create as least connections as possible with MySQL in multi-thread programing. 

## Take a glance
This module contains two classes: 
- `Connection` class is a subclass of `pymysql.connections.Connection`. It can be used with or without a connection_pool, **It's supposed to be used in the exact same way as the pymysql's Connection class**. The details of connection pool's implementation is hiddened (when used with a connection_pool additional actions are needed to maintain the pool).  
This class also provides a wrapped execute_query() method for convenience. It takes several arguments.
- `ConnectionPool`'s instance represents the actual connection_pool.

## Usage example
### Installation
```
pip install pymysql-pool
```
### multi-thread mode:  
The main difference with single-thread mode is that we should maintain the status of the pool. Such as 'get connection from pool' or 'put connection back to pool', in which case there are also some special cases to deal with such as: 
- when getting a connection from a pool: we should deal with the **timeout** and **retry** parameters
- when putting a connection back to pool: if we the queries were executed without exceptions, this connection can be putted back to the pool directly; but if **exception** occurred we have to decide whether this connection should be putted back to the pool depending on if it is **reusable** (it depends on the exception type). If the connection shouldn't be putted back to pool we have to close it and **recreate** a new connection and then put it to the pool.

Luckily, this module will take care of these complicated details for you automaticly.

It also allows to create more than one connection_pool (with distinct `ConnectionPool.name` attribute) to be associated with different databases.

In the example below we're going to see how it works with the connection_pool feature:   
```
>>> import pymysqlpool
>>> pymysqlpool.logger.setLevel('DEBUG')
>>> config={'host':'xxxx', 'user':'xxx', 'password':'xxx', 'database':'xxx', 'autocommit':True}

### Create a connection pool with 2 connections in it
>>> pool1 = pymysqlpool.ConnectionPool(size=2, name='pool1', **config)
>>> pool1.size()
2
>>> con1 = pool1.get_connection()
2017-12-25 21:38:48    DEBUG: Get connection from pool(pool1)
>>> con2 = pool1.get_connection()
2017-12-25 21:38:51    DEBUG: Get connection from pool(pool1)
>>> pool1.size()
0

### We can prophesy that some exception will occur here because the pool1 is empty
>>> con3 = pool1.get_connection(timeout=0, retry_num=0)
Traceback (most recent call last):
  File "e:\github\pymysql-pool\pymysqlpool.py", line 115, in get_connection
    conn = self._pool.get(timeout=timeout) if timeout > 0 else self._pool.get_nowait()
queue.Empty

During handling of the above exception another exception occurred:

Traceback (most recent call last):
  File "<pyshell#37>", line 1, in <module>
    con3 = pool1.get_connection(timeout=0, retry_num=0)
  File "e:\github\pymysql-pool\pymysqlpool.py", line 128, in get_connection
    self.name, timeout, total_times))
pymysqlpool.GetConnectionFromPoolError: can't get connection from pool(pool1) within 0*1 second(s)

### Now let's see the connection's behavior while calling close() method and while using it with Context Manager Protocol
>>> con1.close()
2017-12-25 21:39:56    DEBUG: Put connection back to pool(pool1)
>>> with con1 as cur:
	cur.execute('select 1+1')

1
2017-12-25 21:40:25    DEBUG: Put connection back to pool(pool1)
### We can see that the module maintains the pool appropriately when (and only when) we call the close() method or use the Context Manager Protocol of the connection object.
```

**NOTE 1:** We should always use either the close() method or Context Manager Protocol of the connection object. Otherwise the pool will exhaust soon.  
**NOTE 2:** The Context Manager Protocol is preferred. It can achieve an effect similar to the "multiplexing".  
**NOTE 3:** While using the close() method be careful to never use a connection object's close() method more than one time (you know why~).
