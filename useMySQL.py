import pymysql
import logging
import time
from queue import Queue, Full
import pymysql.cursors
logger = logging.getLogger(__name__)


class CustomDatabaseException(Exception):
    def __init__(self, message, **kwargs):
        self.message = message
        self.extra_info = kwargs
        super().__init__(self.message)

    def __str__(self):
        extra_info_str = ", ".join(f"{k}={v}" for k, v in self.extra_info.items())
        return f"{self.message}. Extra info: {extra_info_str}"


class DatabaseConnectionError(CustomDatabaseException):
    def __init__(self, host, port, message="Unable to establish a database connection"):
        super().__init__(message, host=host, port=port)


class DatabaseOperationFailed(CustomDatabaseException):
    def __init__(self, sql, params, message="Database operation failed"):
        super().__init__(message, sql=sql, params=params)


class SingletonMeta(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        config = args[0]
        config_key = (config['host'], config['port'], config['database'])
        if config_key not in cls._instances:
            instance = super().__call__(*args, **kwargs)
            cls._instances[config_key] = instance
        return cls._instances[config_key]


class MySQLDatabase(metaclass=SingletonMeta):
    def __init__(self, config_mysql, pool_size=10, connect_timeout=5, retry_backoff_base=1.5):
        config_mysql = {k.lower(): v for k, v in config_mysql.items()}
        self.host = config_mysql.get('host')
        self.port = config_mysql.get('port')
        self.user = config_mysql.get('user')
        self.password = config_mysql.get('password')
        self.db = config_mysql.get('database')
        self.charset = config_mysql.get('charset', 'utf8mb4')
        self.pool_size = pool_size
        self.cursorclass = pymysql.cursors.DictCursor if config_mysql.get('cursorclass') else pymysql.cursors.Cursor
        self.connect_timeout = connect_timeout
        self.retry_backoff_base = retry_backoff_base
        self.pool = Queue(maxsize=pool_size)
        for _ in range(pool_size):
            self.pool.put(self.create_conn())

    def create_conn(self, retries=3):
        while retries > 0:
            try:
                return pymysql.connect(
                    host=self.host,
                    port=self.port,
                    user=self.user,
                    password=self.password,
                    db=self.db,
                    charset=self.charset,
                    connect_timeout=self.connect_timeout,
                    cursorclass=self.cursorclass
                )
            except Exception as e:
                retries -= 1
                logger.error(f"Failed to connect to database. Retries left: {retries}. Error: {e}")
                if retries <= 0:
                    raise DatabaseConnectionError(host=self.host, port=self.port)

    def get_conn(self):
        conn = self.pool.get()
        try:
            conn.ping(reconnect=True)
        except Exception as e:
            logger.error(f"Connection lost, attempting to reconnect. Error: {e}")
            conn.close()
            conn = self.create_conn()
        return conn

    def release_conn(self, conn):
        try:
            self.pool.put_nowait(conn)
        except Full:
            conn.close()
    
    def fetch_iter(self, sql, params=[], batch_size=1000, need_per=False):
        conn = self.get_conn()
        offset = 0
        try:
            while True:
                true_sql = f"{sql} LIMIT {offset}, {batch_size}"
                with conn.cursor() as cursor:
                    cursor.execute(true_sql, params)
                    results = cursor.fetchall()
                    if len(results) == 0:
                        break
                    if need_per:
                        for result in results:
                            yield result
                    else:
                        yield results
                    offset += batch_size
        except Exception as e:
            logger.error(f"Database operation error: {e}. SQL executed: {sql} with parameters {params}")
            raise DatabaseOperationFailed(sql=sql, params=params)
        finally:
            conn.rollback()
            self.release_conn(conn)

    def execute(self, sql, params=[], retries=3, lastrowid=False):
        delay = self.retry_backoff_base
        while retries > 0:
            conn = self.get_conn()
            try:
                with conn.cursor() as cursor:
                    if len(params) > 0:
                        if isinstance(params, (list, tuple)) and isinstance(params[0], (list, tuple)):
                            cursor.executemany(sql, params)
                    else:
                        cursor.execute(sql, params)
                conn.commit()
                if lastrowid:
                    return cursor.lastrowid
                return True
            except Exception as e:
                retries -= 1
                time.sleep(delay)  # Introduce a delay
                delay *= self.retry_backoff_base  # Increase the delay
                logger.error(
                    f"Database operation error: {e}. SQL executed: {sql} with parameters {params}. Retries left: {retries}")
                if retries <= 0:
                    raise DatabaseOperationFailed(sql=sql, params=params)
            finally:
                conn.rollback()  # Ensure no changes are committed during fetches
                self.release_conn(conn)

    def batch_insert(self, table_name, columns, data_list, batch_size=100):
        placeholders = ', '.join(['%s'] * len(columns))
        sql_base = f"INSERT INTO {table_name} ({','.join(columns)}) VALUES ({placeholders})"
        total_count = len(data_list)
        for i in range(0, total_count, batch_size):
            batch_data = data_list[i:i + batch_size]
            try:
                self.execute(sql_base, params=batch_data)
            except DatabaseOperationFailed as e:
                logger.error(f"Batch insert failed: {e}")

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