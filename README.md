# PyMySQL_Connection_Pool
A simple connection pool based PyMySQL. Compatible with single thread mode and multi threads mode. Within multi threads mode support the multiplexing similar feature.

When use pymysql with python multi threads, generally we will face the questions:
1. It can't share a connection created by main thread with all sub-threads. It will result error like this:
`pymysql.err.InternalError: Packet sequence number wrong - got 0 expected 1`
2. If we make every sub-thread create a connection and close it when this sub-thread end, that's obviously high cost on establish connections with MySQL.

So I implement this python class aimed at create as least connections with MySQL as possible in multi-threads programing. In another words, reuse the established connections as many as possible.

## Use example
**Note:** This class dose not process exception, it will throw exceptions to it's caller

### multi-threads mode:  
In this mode, developer should take care of the exceptions in "create connection", "get connection from pool", and "execute query" phases and process them, so that we can know which sub-thread(sub-task) exit unexpected. 

This example use `execute_query_multiplex()` method, it will automate "get a connection from pool", "create specified type of cursor", "close cursor" and "return connection back to pool" (when need then get, when finish then return).
```
import concurrent.futures
from sys import exit
from improved_db import ImprovedDb

config={'host'='xxxx', 'user'='xxx', 'password'='xxx', 'database'='xxx', 'antocomit'=True}
MAX_THREAD = 10  #the max threads number expected

db = ImprovedDb(config, connection_pool=True, pool_init_size=MAX_THREAD)
db.create_pool()

# NOTE ===============
# It's worth thinking about the pool_init_size parameter (default 10), 
# if the unit task processed fast, we can set a small number (or ignore it) to take most advantage of the multiplexing; 
# if the unit task take long, we may prefer to set a appropriate large number (depend the capacity of MySQL).
# ====================

def task(parm):
    """suppose parm is user's uid"""
    sql = "select * from user where uid=%s"
    try:
        res = db.execute_query_multiplex(db.pool_get_connection(), sql, parm, dictcursor=True, return_one=True)
    except Exception as err:
        print("Error: uid {} process error. {}".format(parm, err))
        exit(11)
        
    # do something with the res
    
with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_THREAD) as tp:
    for uid in uid_list:
        future = tp.submit(task, uid)
```

### single-thread mode
**Note:** This mode is simple, just use the high level method offer by the class then programing normally.
```
config={'host'='xxxx', 'user'='xxx', 'password'='xxx', 'database'='xxx', 'antocomit'=True}
db = ImprovedDb(config)
connect = db.connect()

sql_1 = "select * from user"
res_1 = db.execute_query(connect, sql_1)

sql_2 = "insert into user values (%s, %s, %s)"
value_list = [('x','x','x'), ('x','x','x'), ('x','x','x')]
try:
    '''process exceptions yourself'''
    # use exec_many parameter
    res_2 = db.execute_query(connect, sql_2, value_list, dictcursor=True, return_one=True, exec_many=True)
except Exception as err:
    print(str(err))
    
    # do something else
```
