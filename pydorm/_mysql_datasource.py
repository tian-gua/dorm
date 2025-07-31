import threading
from time import sleep

import pymysql
from pymysql import MySQLError
from pymysql.cursors import DictCursor
from loguru import logger
from pymysql.connections import Connection


class MysqlDataSource:
    def __init__(self, ds_id, host, port, user, password, database):
        self._ds_id = ds_id
        self._dialect = "mysql"
        self._host = host
        self._port = port
        self._user = user
        self._password = password
        self._default_database = database
        self._reusable_connection = self.create_connection()
        self._reusable_connection_lock = threading.RLock()

        # 创建一个新线程调用连接池的 keep_alive 方法
        threading.Thread(target=self.keep_alive, daemon=True).start()

    def get_id(self) -> str:
        return self._ds_id

    def get_default_database(self) -> str:
        return self._default_database

    def create_connection(self):
        conn = pymysql.connect(
            host=self._host,
            port=self._port,
            user=self._user,
            password=self._password.encode("utf-8"),
            database=self._default_database,
            cursorclass=DictCursor,
        )
        logger.info(f"create connection [{id(conn)}]")
        return conn

    def get_reusable_connection(self) -> Connection:
        self._reusable_connection_lock.acquire()
        logger.info(f"get reusable connection [{id(self._reusable_connection)}]")
        return self._reusable_connection

    def release_reusable_connection(self):
        if self._reusable_connection_lock.acquire():
            self._reusable_connection_lock.release()
            logger.info(
                f"Released reusable connection [{id(self._reusable_connection)}]"
            )
        else:
            logger.error(
                f"Reusable connection [{id(self._reusable_connection)}] lock is not acquired, cannot release connection."
            )

    def keep_alive(self):
        while True:
            with self._reusable_connection_lock:
                logger.info(
                    f"Keep alive for connection[{id(self._reusable_connection)}]"
                )
                try:
                    self._reusable_connection.ping()
                except MySQLError:
                    try:
                        logger.error(
                            f"Connection[{id(self._reusable_connection)}] ping failed, recreating connection."
                        )
                        self._reusable_connection = self.create_connection()
                    finally:
                        logger.error(
                            f"Connection[{id(self._reusable_connection)}] closed due to error."
                        )
                        self._reusable_connection = self.create_connection()
            sleep(30)
