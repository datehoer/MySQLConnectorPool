import pymysql
import logging
import time
from queue import Queue, Full
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


class MySQLDatabase:
    def __init__(self, config_mysql, pool_size=10, connect_timeout=5, retry_backoff_base=1.5):
        self.host = config_mysql['host']
        self.port = config_mysql['port']
        self.user = config_mysql['user']
        self.password = config_mysql['password']
        self.db = config_mysql['database']
        self.charset = config_mysql['charset']
        self.pool_size = pool_size
        self.connect_timeout = connect_timeout
        self.retry_backoff_base = retry_backoff_base
        self.pool = Queue(maxsize=pool_size)
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
                return conn
            except pymysql.MySQLError as e:
                retries -= 1
                time.sleep(delay)
                delay *= self.retry_backoff_base
                logger.error(f"Failed to connect to database. Retries left: {retries}. Error: {e}")
                if retries <= 0:
                    raise DatabaseConnectionError(host=self.host, port=self.port)

    def get_conn(self):
        while True:
            conn = self.pool.get()
            try:
                conn.ping(reconnect=True)
                return conn
            except Exception as e:
                logger.error(f"Connection lost, attempting to reconnect. Error: {e}")
                conn.close()
                if self.pool.empty():
                    return self.create_conn()

    def release_conn(self, conn):
        try:
            self.pool.put_nowait(conn)
        except Full:
            conn.close()

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
                # 捕获连接错误，例如网络问题或服务器中断
                logger.error(f"Connection error: {e}. Retrying...")
                # 这里不再需要显式释放连接，因为它可能已经失效
            except pymysql.MySQLError as e:
                # 捕获其它MySQL错误，这些错误通常不需要重试
                logger.error(f"MySQL error: {e}. SQL: {sql} Params: {params}")
                raise
            except Exception as e:
                # 捕获其他类型的异常
                logger.error(f"Unexpected error: {e}. SQL: {sql} Params: {params}")
                raise
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
            conn = self.pool.get()
            conn.close()
