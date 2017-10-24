import pymysql
import warnings
import queue
from sys import exit

warnings.filterwarnings('error', category=pymysql.err.Warning)


class ImprovedDb(object):
    """
    A improved database class based PyMySQL.
    db_config: database config information, should be a dict
    connection_pool: if use connection pool
    pool_max_size: max number of connection pool
    """
    def __init__(self, db_config, connection_pool=False, pool_init_size=10, pool_max_size=50):
        self.db_config = db_config
        if connection_pool:
            self.pool_init_size = pool_init_size
            self.pool_max_size = pool_max_size
            self.pool = queue.Queue(pool_max_size)

    def get_db_conn(self, err_exit=False, recreate=False):
        """
        Return a MySQL connection object.
        err_exit: if exit when occur Exception(single-thread mode use)
        recreate: just a flag can show more information,
                  indicate if the 'create connection' action due to lack of useable connection in the pool
        """
        if recreate: print('Warning: Create a new connection')
        try:
            connect = pymysql.connect(**self.db_config)
            return connect
        except Exception as err:
            print('\nConnect Error: {}\n'.format(err))
            if err_exit:
                # in single-thread mode, exit directly
                exit(10)
            else:
                # in multi-threads mode, throw to the caller
                raise

    @staticmethod
    def get_conn_cur(connect, dictcursor=False):
        """
        Return the given connection's cursor.
        dictcursor: cursor type is tuple(default) or dict
        """
        cur = connect.cursor() if not dictcursor else connect.cursor(pymysql.cursors.DictCursor)
        return cur

    @staticmethod
    def db_query(cur, query, args=(), return_one=False, exec_many=False, err_exit=False):
        """
        return_one: whether want only one row of the result
        exec_many: whether use pymysql's executemany() method
        err_exit: whether exit when occur exception(single-thread mode use)
        """
        try:
            if exec_many:
                res = cur.executemany(query, args)
                return res
            else:
                cur.execute(query, args)
                res = cur.fetchall()
                # if no record match the query, return () if return_one==False, else return None
                return (res[0] if res else None) if return_one else res
        except Exception as err:
            print('Query Error: {}'.format(err))
            if err_exit:
                exit(11)
            else:
                raise

    def create_connection_pool(self):
        """
        Create specified number of connections when create the pool.
        The number is the smaller of self.pool_init_size and self.pool_max_size else.
        """
        for i in range(self.pool_init_size if self.pool_init_size < self.pool_max_size else self.pool_max_size):
            connect = self.get_db_conn(err_exit=True)
            self.pool.put(connect)

    def pool_get_connect(self, timeout=5):
        """
        Multi-thread mode, sub-thread should get a connection object from the pool.
        If a sub-thread can't get a connection object, then re-create a fixed number of connections
        (use the smaller of self.pool_init_size and self.pool_max_size), and put them into the pool.
        timeout: timeout when get connection object from the pool
        """
        try:
            conn = self.pool.get(timeout=timeout)
        except queue.Empty:
            for i in range(self.pool_init_size if self.pool_init_size < self.pool_max_size else self.pool_max_size):
                ret = self.pool_return_connect(self.get_db_conn(recreate=True))
                if not ret: break
            raise Exception("cat't get connect from pool")
        # caller should the take care of the availability of the connection object from the pool
        return conn

    def pool_return_connect(self, connect, conn_type='old'):
        """
        Before the sub-thread end, should return the connection object back to the pool
        conn_type: just a flag can show more information 
        """
        try:
            self.pool.put_nowait(connect)
            return 1
        except queue.Full:
            if conn_type == 'new':
                print("Warning: Can't put new connection back to pool")
            else:
                print("Warning: Can't put connection back to pool")
            return 0
        # print('pool_return_connect', connect)  #ljk

    @staticmethod
    def db_close(connect):
        connect.close()
