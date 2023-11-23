import pymysql
import logging
import time
from queue import Queue, Full
from threading import Lock
logger = logging.getLogger(__name__)


# 自定义异常基类
class CustomDatabaseException(Exception):
    def __init__(self, message, **kwargs):
        self.message = message
        self.extra_info = kwargs
        super().__init__(self.message)

    def __str__(self):
        extra_info_str = ", ".join(f"{k}={v}" for k, v in self.extra_info.items())
        return f"{self.message}. Extra info: {extra_info_str}"


# 连接错误
class DatabaseConnectionError(CustomDatabaseException):
    def __init__(self, host, port, message="Unable to establish a database connection"):
        super().__init__(message, host=host, port=port)


# 操作失败
class DatabaseOperationFailed(CustomDatabaseException):
    def __init__(self, sql, params, message="Database operation failed"):
        super().__init__(message, sql=sql, params=params)


class EnhancedConnection:
    def __init__(self, pymysql_conn):
        self.conn = pymysql_conn
        self.last_used_time = time.time()

    def __getattr__(self, item):
        # 代理到原始的 pymysql 连接对象
        return getattr(self.conn, item)

    def is_valid(self):
        try:
            self.conn.ping(reconnect=True)  # 使用 ping 方法检查连接是否存活
            return True
        except:
            return False


class MySQLDatabase:
    def __init__(self, config_mysql, pool_size=10, max_pool_size=20, idle_time=300, connect_timeout=5, retry_backoff_base=1.5, max_retry_delay=120):
        self.host = config_mysql['host']
        self.port = config_mysql['port']
        self.user = config_mysql['user']
        self.password = config_mysql['password']
        self.db = config_mysql['database']
        self.charset = config_mysql['charset']
        self.pool_size = pool_size
        self.max_pool_size = max_pool_size
        self.idle_time = idle_time
        self.connect_timeout = connect_timeout
        self.retry_backoff_base = retry_backoff_base
        self.max_retry_delay = max_retry_delay
        self.pool = Queue(maxsize=max_pool_size)
        self.lock = Lock()
        for _ in range(pool_size):
            self.pool.put(self.create_conn())

    def create_conn(self, retries=3):
        delay = self.retry_backoff_base
        while retries > 0:
            try:
                conn = pymysql.connect(
                    host=self.host,
                    port=self.port,
                    user=self.user,
                    password=self.password,
                    db=self.db,
                    charset=self.charset,
                    connect_timeout=self.connect_timeout
                )
                return EnhancedConnection(conn)
            except pymysql.MySQLError as e:
                retries -= 1
                time.sleep(delay)
                delay *= self.retry_backoff_base
                logger.error(f"Failed to connect to database. Retries left: {retries}. Error: {e}")
                if retries <= 0:
                    raise DatabaseConnectionError(host=self.host, port=self.port)

    def get_conn(self):
        with self.lock:
            if self.pool.qsize() < self.pool_size and self.pool.qsize() < self.max_pool_size:
                try:
                    self.pool.put_nowait(self.create_conn())
                except Full:
                    logger.warning("Connection pool is full. Waiting for an available connection.")
            elif self.pool.empty():
                logger.warning("Connection pool is empty. Creating a new connection.")
                return self.create_conn()

        enhanced_conn = self.pool.get()
        if not enhanced_conn.is_valid():  # 检查连接有效性
            enhanced_conn.conn.close()
            enhanced_conn = self.create_conn()
        return enhanced_conn

    def release_conn(self, enhanced_conn):
        if not enhanced_conn.is_valid():  # 检查连接有效性
            logger.warning("Connection is invalid. Discarding.")
            enhanced_conn.conn.close()
            return

        if time.time() - enhanced_conn.last_used_time > self.idle_time or self.pool.qsize() >= self.max_pool_size:
            logger.info("Closing connection due to idle time or pool size.")
            enhanced_conn.conn.close()
        else:
            self.pool.put_nowait(enhanced_conn)

    def execute(self, sql, params=None, retries=3, fetch=False, lastrowid=False):
        delay = self.retry_backoff_base
        while retries > 0:
            conn = None
            try:
                conn = self.get_conn()
                with conn.cursor() as cursor:
                    cursor.execute(sql, params)
                    if fetch:
                        result = cursor.fetchall()
                    elif lastrowid:
                        result = cursor.lastrowid
                    else:
                        result = True
                conn.commit()
                return result
            except pymysql.OperationalError as e:
                logger.error(f"Connection error: {e}. Retrying...")
                retries -= 1
                if retries <= 0:
                    raise DatabaseOperationFailed(sql=sql, params=params)
                time.sleep(delay)
                delay *= self.retry_backoff_base
            except pymysql.MySQLError as e:
                logger.error(f"MySQL error: {e}. SQL: {sql} Params: {params}")
                if conn:
                    conn.rollback()
                raise DatabaseOperationFailed(sql=sql, params=params)
            except Exception as e:
                logger.error(f"Unexpected error: {e}. SQL: {sql} Params: {params}")
                if conn:
                    conn.rollback()
                raise DatabaseOperationFailed(sql=sql, params=params)
            finally:
                if conn:
                    self.release_conn(conn)
                retries -= 1
                time.sleep(delay)  # 延迟重试
                delay *= self.retry_backoff_base

        # 如果重试耗尽，则抛出异常
        raise DatabaseOperationFailed(sql=sql, params=params)

    def batch_insert(self, table_name, columns, data_list, batch_size=100):
        placeholders = ', '.join(['%s'] * len(columns))
        sql_base = f"INSERT INTO {table_name} ({','.join(columns)}) VALUES ({placeholders})"
        total_count = len(data_list)
        for i in range(0, total_count, batch_size):
            batch_data = data_list[i:i + batch_size]
            self.execute(sql_base, params=batch_data)

    def batch_update(self, table_name, columns, data_list, where_column, batch_size=100):
        set_columns = ', '.join([f"{col} = %s" for col in columns])
        sql_base = f"UPDATE {table_name} SET {set_columns} WHERE {where_column} = %s"
        total_count = len(data_list)
        for i in range(0, total_count, batch_size):
            batch_data = data_list[i:i + batch_size]
            for record in batch_data:
                self.execute(sql_base, params=record)

    def close_all_connections(self):
        while not self.pool.empty():
            conn = self.pool.get_nowait()
            conn.close()
