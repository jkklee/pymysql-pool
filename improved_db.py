"""
author: ljk
email: chaoyuemyself@hotmail.com
"""
import pymysql
import warnings
import queue
import logging

warnings.filterwarnings('error', category=pymysql.err.Warning)
# use logging module for easy debug
logging.basicConfig(format='%(asctime)s %(levelname)8s: %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)
logger.setLevel('WARNING')


class ImprovedDb(object):
    """A improved database class based PyMySQL which support multi-threads ans async mode"""
    _POOL_HARD_LIMIT = 100

    def __init__(self, db_config):
        """db_config: database config information, should be a dict"""
        self.db_config = db_config
        self.pool = queue.Queue(self._POOL_HARD_LIMIT)

    def connect(self, recreate=False):
        """
        Create and return a MySQL connection object or cursor object.
        recreate: just a flag indicate if the 'create connection' action due to lack of useable connection in the pool
        """
        try:
            conn = pymysql.connect(**self.db_config)
            if recreate:
                logger.info('create new connection because of pool lacking')
                logger.debug('create new connection because of pool lacking: {}'.format(conn))
            return conn
        except Exception as err:
            if recreate:
                logger.error('recreate connection error when pool lacking: {}'.format(err))
            else:
                logger.error('create connection error: {}'.format(err))
            raise

    @staticmethod
    def execute_query(connection, query, args=(), dictcursor=False, return_one=False, exec_many=False):
        """
        A wrapped implementation of pymysql's execute() or executemany().
        connection: connection object created by self.connect()
        return_one: whether want only one row of the result
        exec_many: whether use pymysql's executemany() method
        """
        with connection.cursor() if not dictcursor else connection.cursor(pymysql.cursors.DictCursor) as cur:
            try:
                if exec_many:
                    cur.executemany(query, args)
                else:
                    cur.execute(query, args)
            except Exception:
                raise
            # if no record match the query, return () if return_one==False, else return None
            return cur.fetchone() if return_one else cur.fetchall()

    def execute_query_multiplex(self, connection, query, args=(), dictcursor=False, return_one=False, exec_many=False):
        """
        A convenience method for:
            ```
            connection = pymysql.connect() or self.pool_get_connection()
            cursor = connection.cursor()
            cursor.execute(query, args=())
            cursor.close()
            self.pool_put_connection(connection)
            ```
        connection: connection object
        dictcursor: cursor type is tuple(default) or dict
        return_one: whether want only one row of the result
        exec_many: whether use pymysql's executemany() method
        """
        with connection.cursor() if not dictcursor else connection.cursor(pymysql.cursors.DictCursor) as cur:
            try:
                if exec_many:
                    cur.executemany(query, args)
                else:
                    cur.execute(query, args)
            except (pymysql.err.ProgrammingError, pymysql.err.InternalError,
                    pymysql.err.IntegrityError, pymysql.err.NotSupportedError):
                cur.close()
                self.pool_put_connection(connection)
                raise
            except Exception:
                raise
            res = cur.fetchone() if return_one else cur.fetchall()
        # return connection back to the pool
        self.pool_put_connection(connection)
        return res

    def create_pool(self, pool_init_size=10):
        """
        Create pool_init_size connections when init the pool.
        """
        logger.debug('init connection pool')
        for i in range(pool_init_size):
            conn = self.connect()
            self.pool.put(conn)
        logger.debug('pool object: {}'.format(self.pool.queue))

    def pool_get_connection(self, timeout=3, retry_num=1):
        """
        Multi-thread mode, threads get connection object from the pool.
        If one thread can't get a connection object, then re-create a new connections and put it into pool.
        timeout: timeout of get connection from pool.
            1.If unit task process fast, set a small number(or just ignore it) to take most advantage of the multiplexing;
            2.If unit task may takes long, set a appropriate large number to reduce the errors come from pool lacking,
              and set the 'pool_init_size' in __init__() method as larger as your thread-pool's max_workers(also depend
              the capacity of MySQL)
        retry_num: number of retry when timeout
        """
        try:
            conn = self.pool.get(timeout=timeout)
            logger.debug('get connection: {}'.format(conn))
            # caller should the take care of the availability of the connection object from the pool
            return conn
        except queue.Empty:
            logger.warning('get connection from pool timeout')
            # create new connection at the reason of pool lacking
            conn = self.connect(recreate=True)
            self.pool_put_connection(conn, conn_type='new')
            if retry_num > 0:
                logger.warning('retry get connection from pool')
                return self.pool_get_connection(timeout, retry_num-1)

    def pool_put_connection(self, connection, conn_type='old'):
        """
        Before the sub-thread end, should return the connection object back to the pool
        conn_type: "new" or "old"(default) just a flag can show more information
        """
        try:
            logger.debug('return connection: {}'.format(connection))
            self.pool.put_nowait(connection)
        except queue.Full:
            if conn_type == 'new':
                logger.warning("can't put new connection to pool")
            else:
                logger.warning("can't put old connection back to pool")
