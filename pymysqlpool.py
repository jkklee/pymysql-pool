"""
author: ljk
email: chaoyuemyself@hotmail.com
"""
import pymysql
import warnings
import queue
import logging
import threading
import time

__all__ = ['Connection', 'ConnectionPool', 'logger']

warnings.filterwarnings('error', category=pymysql.err.Warning)
# use logging module for easy debug
logging.basicConfig(format='%(asctime)s %(levelname)8s: %(message)s', datefmt='%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)
logger.setLevel('WARNING')


class Connection(pymysql.connections.Connection):
    """
    Return a connection object with or without connection_pool feature.
    This is all the same with pymysql.connections.Connection instance except that with connection_pool feature:
        the __exit__() method additionally put the connection back to it's pool
    """
    _pool = None
    _reusable_expection = (pymysql.err.ProgrammingError, pymysql.err.IntegrityError, pymysql.err.NotSupportedError)

    def __init__(self, *args, **kwargs):
        pymysql.connections.Connection.__init__(self, *args, **kwargs)
        self.args = args
        self.kwargs = kwargs

    def __exit__(self, exc, value, traceback):
        """
        Overwrite the __exit__() method of pymysql.connections.Connection
        Base action: on successful exit, commit. On exception, rollback
        With pool additional action: put connection back to pool
        """
        if self._pool is not None:
            if not exc or exc in self._reusable_expection:
                '''reusable connection'''
                self._pool._put_connection(self)
            else:
                '''no reusable connection, close it and create a new one put to the pool'''
                self._pool = None
                try:
                    self.close()
                    logger.debug("Close non-reusable connection in pool(%s) caused by %s", self._pool.name, value)
                except Exception:
                    self._force_close()
        else:
            pymysql.connections.Connection.__exit__(self, exc, value, traceback)

    def close(self):
        """
        Overwrite the close() method of pymysql.connections.Connection
        With pool, put connection back to pool;
        Without pool, send the quit message and close the socket
        """
        if self._pool is not None:
            self._pool._put_connection(self)
        else:
            pymysql.connections.Connection.close(self)

    def ping(self, reconnect=True):
        """
        Overwrite the ping() method of pymysql.connections.Connection
        Check if the server is alive.
        :param reconnect: If the connection is closed, reconnect.
        :type reconnect: boolean
        :raise Error: If the connection is closed and reconnect=False.
        """
        if self._sock is None:
            if reconnect:
                self.connect()
                reconnect = False
            else:
                raise pymysql.err.Error("Already closed")
        try:
            self._execute_command(pymysql.constants.COMMAND.COM_PING, "")
            self._read_ok_packet()
        except Exception:
            if reconnect:
                # here add action to deal the old/broken connection in pool
                if self._pool is not None:
                    logger.debug('Connection had broken in pool(%s), reconnect it', self._pool.name)
                    self._force_close()
                self.connect()
                self.ping(False)
            else:
                raise

    def execute_query(self, query, args=(), dictcursor=False, return_one=False, exec_many=False):
        """
        A wrapped method of pymysql's execute() or executemany().
        dictcursor: whether want use the dict cursor(cursor's default type is tuple)
        return_one: whether want only one row of the result
        exec_many: whether use pymysql's executemany() method
        """
        with self:
            cur = self.cursor() if not dictcursor else self.cursor(pymysql.cursors.DictCursor)
            try:
                if exec_many:
                    cur.executemany(query, args)
                else:
                    cur.execute(query, args)
            except Exception:
                raise
            # if no record match the query, return () if return_one==False, else return None
            return cur.fetchone() if return_one else cur.fetchall()


class ConnectionPool:
    """
    Return connection_pool object, which has method can get connection from a pool with timeout and retry feature;
    put a reusable connection back to the pool, etc; also we can create different instance of this class that represent
    different pool of different DB Server or different user
    """
    _THREAD_LOCAL = threading.local()
    _THREAD_LOCAL.retry_counter = 0  # a counter used for debug get_connection() method

    def __init__(self, size=10, max_size=100, name=None, pre_create_num=0, con_lifetime=3600, *args, **kwargs):
        """
        size: int
            normal size of the pool
        max_size: int
            max size for scalability
        name: str
            optional pool name (str)
            default: host-port-user-database
        pre_create_num: int
            create specified number connections at the init phase; otherwise will create connection when really need.
        con_lifetime: int
            the max lifetime(seconds) of the connections, if it reach the specified seconds, when return to the pool:
                1. if connction_number<=size, create a new connection and replace the overlifetime one in the pool;
                   resolve the problem of mysql server side close due to 'wait_timeout'
                2. If connction_number>size, close the connection and remove it from the pool.
                   used for pool scalability.
            in order for the arg to work as expect: 
                you should make sure that mysql's 'wait_timeout' variable is greater than the con_lifetime.
            0 or negative means do not consider the lifetime
        args & kwargs:
            same as pymysql.connections.Connection()
        """
        self._size = size
        self.max_size = max_size
        self._pool = queue.Queue(max_size)
        self._pre_create_num = pre_create_num
        self._con_lifetime = con_lifetime
        self._total_con_num = 0  # total connections in use or usable
        self._args = args
        self._kwargs = kwargs
        self.name = name if name else '-'.join(
            [kwargs.get('host', 'localhost'), str(kwargs.get('port', 3306)),
             kwargs.get('user', ''), kwargs.get('database', '')])

        if pre_create_num > 0:
            for _ in range(pre_create_num):
                conn = self._create_connection()
                self._pool.put(conn)
                conn._returned = True
        else:
            self._args = args
            self._kwargs = kwargs

    def get_connection(self, timeout=0.2, retry_num=2, pre_ping=False):
        """
        timeout: int
            timeout of get a connection from pool, should be a int(0 means return or raise immediately)
        retry_num: int
            how many times will retry to get a connection
        pre_ping: bool
            before return a connection, send a ping command to the Mysql server, if the connection is broken, reconnect it
        """
        try:
            if not self._pre_create_num > 0:
                timeout = 0
            conn = self._pool.get(timeout=timeout) if timeout > 0 else self._pool.get_nowait()
            if pre_ping:
                conn.ping(reconnect=True)
            conn._returned = False
            logger.debug('Get connection from pool(%s)', self.name)
            return conn
        except queue.Empty:
            if self._total_con_num < self._size:
                return self._create_connection()
            else:
                if not hasattr(self._THREAD_LOCAL, 'retry_counter'):
                    self._THREAD_LOCAL.retry_counter = 0
                if retry_num > 0:
                    self._THREAD_LOCAL.retry_counter += 1
                    logger.debug('Retry to get connection from pool(%s), the %d times', self.name, self._THREAD_LOCAL.retry_counter)
                    retry_num -= 1
                    return self.get_connection(timeout, retry_num)
                else:
                    # normal pool has used up and even retry mechanism can't get a connection, enhance the pool up to max_size
                    if self._total_con_num < self.max_size:
                        self._THREAD_LOCAL.retry_counter = 0
                        return self._create_connection()
                    else:
                        total_times = self._THREAD_LOCAL.retry_counter + 1
                        self._THREAD_LOCAL.retry_counter = 0
                        raise GetConnectionFromPoolError("can't get connection from pool({}) within {}*{} second(s)".format(
                            self.name, timeout, total_times))

    def _put_connection(self, conn):
        if conn._pool is None:
            return
        conn.cursor().close()
        try:
            if not conn._returned:
                # consider the connection lifetime with the purpose of reduce active connections number
                if self._con_lifetime > 0 and int(time.time()) - conn._create_ts >= self._con_lifetime:
                    conn._pool = None
                    conn.close()
                    logger.debug("Close connection in pool(%s) due to lifetime reached", self.name)
                    if self._total_con_num <= self._size:
                        conn = self._create_connection()
                        self._pool.put_nowait(conn)
                        logger.debug("Put connection back to pool(%s)", self.name)
                    self._total_con_num -= 1
                else:
                    self._pool.put_nowait(conn)
                    logger.debug("Put connection back to pool(%s)", self.name)
            else:
                raise ReturnConnectionToPoolError("this connection has already returned to the pool({})".format(self.name))
        except queue.Full:
            conn._pool = None
            conn.close()
            self._total_con_num -= 1
            logger.warning("Discard connection due to pool(%s) is full, pool size:%d", self.name, self.size)
        finally:
            conn._returned = True

    def _create_connection(self):
        conn = Connection(*self._args, **self._kwargs)
        conn._pool = self
        # add attr create timestamp for connection
        conn._create_ts = int(time.time())
        # add attr indicate whether the connection has already return to pool, should not use any more
        conn._returned = False
        self._total_con_num += 1
        logger.debug('Create new connection in pool(%s)', self.name)
        return conn

    @property
    def size(self):
        return self._pool.qsize()

    @property
    def connection_num(self):
        return self._total_con_num


class GetConnectionFromPoolError(Exception):
    """Exception related can't get connection from pool within timeout seconds."""


class ReturnConnectionToPoolError(Exception):
    """Exception related can't return connection to pool."""
