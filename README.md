# PyMySQL_Connection_Pool
A simple connection pool based PyMySQL. Compatible with single thread mode and multi threads mode

When use pymysql with python multi threads, generally we will face the questions:
1. It can't share a connection created by main thread with all sub-threads.
2. If we make every sub-thread create a connection and close it when this sub-thread end, that's obviously waste.

So I implement this python class aimed at create as least connections with MySQL as possible in multi-threads programing. In another words, reuse the established connections as many as possible.

### Use example

#### multi-threads mode:  
**Note:** in this mode, developer should catch the exceptions in "create connection", "get connection from the pool", and "execute query" phases and process them, so that we can know which sub-thread(sub-task) exit unexpected. 
```
import concurrent.futures
from sys import exit
from improved_db import ImprovedDb

config={'host'='xxxx', 'user'='xxx', 'password'='xxx', 'database'='xxx', 'antocomit'=True}
MAX_THREAD = 5

db = ImprovedDb(config, connection_pool=True, pool_max_size=MAX_THREAD)
db.create_connection_pool()

def task(parm):
    """suppose parm is user's uid"""
    try: 
        connect = db.pool_get_connect()
    except Exception as err:
        print('Error: uid {}, can't get connection from pool. {}'.format(parm, err))
        exit(10)
    cursor = db.get_conn_cur(connect, dictcursor=True)   
    
    sql = "select * from user where uid=%s"
    try:
        res = db.db_query(cursor, sql, parm, return_one=True, pool=True)
    except Exception as err:
        print("Error: uid {} query error. {}".format(parm, err))
        exit(11)
        
    # do something with the res
    
    cursor.close()
    db.pool_return_connect(connect)
    
with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_THREAD) as tp:
    for uid in uid_list:
        future = tp.submit(task, uid)
```
#### single-thread mode
**Note:** in this mode, developer can use the `err_exit=True` parameter in method `get_db_conn()` and `db_query()`, thus you will avoid have to process the exceptions in "create connection" and "execute query" phases. Also you can ignore this parameter and process the exceptions yourself.
```
config={'host'='xxxx', 'user'='xxx', 'password'='xxx', 'database'='xxx', 'antocomit'=True}
db = ImprovedDb(config)
connect = db.pool_get_connect(err_exit=True)
cursor = connect.cursor()

sql_1 = "select * from user"
res_1 = db.db_query(cursor, sql_1, err_exit=True)  # db object will process the exceptions for you(print error message and exit)

sql_2 = "insert into user values (%s, %s, %s)"
value_list = [('x','x','x'), ('x','x','x'), ('x','x','x')]
try:
    res_2 = db.db_query(cursor, sql_2, value_list, return_one=True, exec_many=True)  # process exceptions yourself
except Exception as err:
    print(str(err))
    # do some other process
```
