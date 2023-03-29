"""
author: ljk
email: chaoyuemyself@hotmail.com
"""
import pymysql
import warnings
import logging
import time
from collections import deque

__all__ = ['Connection', 'ConnectionPool', 'logger']

warnings.filterwarnings('error', category=pymysql.err.Warning)
# use logging module for easy debug
def _init_logger():
    logger = logging.getLogger(__name__)
    handler = logging.StreamHandler()
    handler.setFomatter(
        logging.Formatter(
            fmt='%(asctime)s %(levelname)8s: %(message)s',
            datefmt='%m-%d %H:%M:%S',
        )
    )
    logger.addHandler(handler)
    logger.setLevel('WARNING')


logger = _init_logger()


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

    def __init__(self, size=10, maxsize=100, name=None, pre_create_num=0, con_lifetime=3600, *args, **kwargs):
        """
        size: int
            normal size of the pool
        maxsize: int
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
        self.maxsize = maxsize
        self._pool = deque()
        self._pre_create_num = pre_create_num if pre_create_num <= maxsize else maxsize
        self._con_lifetime = con_lifetime
        self._args = args
        self._kwargs = kwargs
        self.name = name if name else '-'.join(
            [kwargs.get('host', 'localhost'), str(kwargs.get('port', 3306)),
             kwargs.get('user', ''), kwargs.get('database', '')])
        self._created_num = deque()  # record the number of all used and available connections(use deque for thread-safe)

        if pre_create_num > 0:
            for _ in range(self._pre_create_num):
                conn = self._create_connection()
                self._pool.appendleft(conn)
                conn._returned = True
        else:
            self._args = args
            self._kwargs = kwargs

    def get_connection(self, retry_num=3, retry_interval=0.1, pre_ping=False):
        """
        timeout: int
            timeout of get a connection from pool, should be a int(0 means return or raise immediately)
        retry_num: int
            how many times will retry to get a connection
        pre_ping: bool
            before return a connection, send a ping command to the Mysql server, if the connection is broken, reconnect it
        """
        if retry_num > 10:
            retry_num = 10  # retry_num hard limit
        try:
            conn = self._pool.pop()
        except IndexError:
            if self.connection_num < self._size:
                return self._create_connection()
            if retry_num > 0:
                retry_num -= 1
                time.sleep(retry_interval)
                logger.debug('Retry to get connection from pool(%s)', self.name)
                return self.get_connection(retry_num, retry_interval, pre_ping)
            else:
                if self.connection_num < self.maxsize:
                    return self._create_connection()
                else:
                    raise GetConnectionFromPoolError("can't get connection from pool({}), retry_interval={}(s)".format(
                        self.name, retry_num, retry_interval))

        # check con_lifetime
        if self._con_lifetime > 0 and int(time.time()) - conn._create_ts >= self._con_lifetime:
            conn._pool = None
            try:
                conn.close()
            except:
                conn._force_close()
            self._created_num.pop()
            logger.debug("Close connection in pool(%s) due to lifetime reached", self.name)
            # loss one, create one
            return self._create_connection()
        else:
            if pre_ping:
                conn.ping(reconnect=True)

        conn._returned = False
        logger.debug('Get connection from pool(%s)', self.name)
        return conn

    def _put_connection(self, conn):
        if not hasattr(conn, '_pool') or conn._pool is None:
            return
        conn.cursor().close()
        if not conn._returned:
            # consider the connection lifetime with the purpose of reduce active connections number
            if self._con_lifetime > 0 and int(time.time()) - conn._create_ts >= self._con_lifetime:
                conn._pool = None
                try:
                    conn.close()
                except:
                    conn._force_close()
                self._created_num.pop()
                logger.debug("Close connection in pool(%s) due to lifetime reached", self.name)
                if self.connection_num >= self._size:
                    conn._returned = True
                    return
                conn = self._create_connection()
            self._pool.appendleft(conn)
            conn._returned = True
            logger.debug("Put connection back to pool(%s)", self.name)
        else:
            raise ReturnConnectionToPoolError("this connection has already returned to the pool({})".format(self.name))

    def _create_connection(self):
        conn = Connection(*self._args, **self._kwargs)
        conn._pool = self
        # add attr create timestamp for connection
        conn._create_ts = int(time.time())
        # add attr indicate whether the connection has already return to pool, should not use any more
        conn._returned = False
        self._created_num.append(1)
        logger.debug('Create new connection in pool(%s)', self.name)
        return conn

    @property
    def size(self):
        """available connections number for now"""
        return len(self._pool)

    @property
    def connection_num(self):
        """total connections number of all used and available"""
        return len(self._created_num)


class GetConnectionFromPoolError(Exception):
    """Exception related can't get connection from pool within timeout seconds."""


class ReturnConnectionToPoolError(Exception):
    """Exception related can't return connection to pool."""
