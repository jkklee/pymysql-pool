# PyMySQL 连接池

一个用于 Python 的数据库链接池模块，基于 PyMySQL。精心设计、使用简单、定位小而美。

## 要解决的问题

在 Python 多线程场景下使用 PyMySQL 时，我们通常会面临以下问题:

1. 无法在子线程中共享主线程中创建的链接，你可能会遇到这种异常：
   `pymysql.err.InternalError: Packet sequence number wrong - got 0 expected 1`
2. 如果我们让每个子线程创建一个连接，这虽然是可行的，但显然会增加与 MySQL 间创建连接的成本；尤其对于 MySQL 来说，维护每条链接都需要一定的资源，过多的链接或者频繁的创建和销毁链接都会对 MySQL 造成额外的压力。

## 特点

1. 简单: 没有额外的学习成本。
2. 性能: 与原生的 PyMysql(简单基准)相比，本模块由于维护连接池而带来的开销非常小。[简单基准测试](https://github.com/jkklee/pymysql-pool#simple-benchmark)。
3. 灵活: 预先创建连接或在真正需要时创建;普通池大小和最大池大小对于可伸缩性，这完全取决于你。
4. 周到: 包含重试机制，以及`connection lifetime`和`pre_ping`机制--以防从连接池中借用一个已断开的连接(例如，MySQL 服务器由于`wait_timeout`设置而关闭)。

## 基本组件

该模块有两个类:

- `Connection` 类: 是`pymysql.connections.Connection`的子类，它同时支持有连接池和无连接池两种使用模式。 **它用起来和 pymysql 完全一样**. 维护连接池的逻辑细节被巧妙的隐藏在了经过覆写过的相关方法中。
- `ConnectionPool` 类: 该类实现了维护连接池的逻辑，创建、获取、返回等方法以及总连接数和可用连接数两个属性。

## 其他方面

使用连接池，还有其他一些方面需要考虑（可以调校），例如：

- 当获取链接时: 我们需要考虑下当无法获取链接时的重试机制，本模块提供了**retry_num** 和 **retry_interval** 这俩参数，以便给客户端更多的获取链接的机会，而不是直接返回错误`GetConnectionFromPoolError`。
- 当归还链接时: 如果 sql 语句正常执行，那么该链接归还至连接池自然没什么疑问；但是当遇到异常时呢，我们应该将当前链接直接丢弃吗。考虑到有几种异常只是“上层错误”（如.ProgrammingError，IntegrityError 等），并不是链接本身导致的异常，这样的链接完全可以返回给连接池继续使用。本模块考虑了这种情况，以图尽可能多的复用已有链接，少创建新链接。
- 另外他还提供了`ConnectionPool.name`属性，以便创建多个连接池对象。

## 使用示例

#### 安装

```
pip install pymysql-pool
```

下面的示例中我们来看看它时如何工作的:

1. 创建一个连接池：可容纳两个链接（size 参数），这两个链接是预创建的（pre_create_num 参数）；最大可容纳 3 个链接（maxsize 参数），连接池的对象的名子为`mypool`（name 参数）

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

2. 现在池中的两个链接都被借出去，池子已经空了，让我们来看看继续执行`get_connection()`方法会怎样

   ```
   >>> con3=mypool.get_connection()
   03-08 15:57:32    DEBUG: Retry to get connection from pool(mypool)
   03-08 15:57:32    DEBUG: Retry to get connection from pool(mypool)
   03-08 15:57:32    DEBUG: Retry to get connection from pool(mypool)
   03-08 15:57:33    DEBUG: Create new connection in pool(mypool)
   ```

   上面给我们展示了，虽然连接池已空，但是因为还没到 maxsize 规定的最大连接数，所以在经过 3 次重试后（参数默认值），链接池又创建了第 3 条链接，并将它返回给客户端。现在池子以及达到了容量上限，并且依然是空的。

3. 让我们继续尝试从池中获取链接

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

   pymysqlpool.GetConnectionFromPoolError: can't get connection from pool(mypool), due to pool lack.
   ```

   我们看到经过几次重试后，最终抛出了异常 `GetConnectionFromPoolError`。

4. 接来下，让我们看看获取到的链接对象在执行`close()`方法或者使用上下文管理器（with 语句）时如何表现

   ```
   >>> con1.close()
   2017-12-25 21:39:56    DEBUG: Put connection back to pool(mypool)
   >>> with con2:
           with con2.cursor() as cur:
               cur.execute('select 1+1')

   1
   12-20 22:44:37    DEBUG: Put connection back to pool(mypool)

   >>> mypool.total_num
   3  # 如预期
   >>> mypool.available_num
   2  # 如预期
   ```

   我们看到该模块可以很好的管理连接池的“借出”和“归还”动作。

## 基准测试

我做了一个简单的基准测试，通过和原生 pymysql 对比，来评估该模块维护连接池所带来的性能方面的开销。  
测试逻辑位于`simple-benchmark.py`，你可以在你的环境下自行测试。  
测试结果（循环 50000 次）

```
# 'pymysql-one-conn' 是直接使用pymysql，只建立一次链接，在该链接内执行所有查询，这可以理解为是所有场景里最好的，性能最高的。
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

# 'pymysql-pool' 使用连接池（该测试只需创建一个大于1的、预先建立链接的池子即可）
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

我们可以看到，该模块维护连接池带来的开销是非常小的，一次`get`和`return`操作，总共只耗费约 0.01 毫秒。

## 注意

1. 我们一定要确保在不用链接时记得调用 Connection 对象的`close()`方法，否则只借不还，连接池将很快被耗尽。

2. 更推荐使用 with 语句（`Context Manager Protocol`），因为它在每次查询后都会自动返回链接，相当于更积极的归还链接，有利于更充分的使用池中的每个链接。  
   如果不用 with 语句而手动调用 close()方法来归还链接的话，考虑这么一种情况：借用链接---查询---其他逻辑---再次查询---归还链接，那么在第一次查询完毕到第二次查寻完毕这期间，其他线程时无法获得该链接的，若这期间的逻辑比较耗时，岂不是导致了该链接空置。这也是更推荐用 with 语句的原因。
