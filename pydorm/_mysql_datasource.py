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
        self._reusable_connection = None
        self._reusable_connection_lock = threading.RLock()
        self._active = False

    def active(self):
        # 创建一个新线程调用连接池的 keep_alive 方法
        self._reusable_connection = self.create_connection()
        threading.Thread(target=self.keep_alive, daemon=True).start()
        self._active = True

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
        logger.info(f"[{self._ds_id}] create connection [{id(conn)}]")
        return conn

    def get_reusable_connection(self) -> Connection:
        if not self._active:
            self.active()

        self._reusable_connection_lock.acquire()
        # logger.info(f"[{self._ds_id}] get reusable connection [{id(self._reusable_connection)}]")
        return self._reusable_connection

    def release_reusable_connection(self):
        self._reusable_connection_lock.release()
        # logger.info(
        #     f"[{self._ds_id}] Released reusable connection [{id(self._reusable_connection)}]"
        # )

    def keep_alive(self):
        while True:
            with self._reusable_connection_lock:
                logger.debug(
                    f"[{self._ds_id}] Keep alive for connection[{id(self._reusable_connection)}]"
                )
                try:
                    self._reusable_connection.ping()
                except MySQLError:
                    try:
                        logger.error(
                            f"[{self._ds_id}] Connection[{id(self._reusable_connection)}] ping failed, recreating connection."
                        )
                        self._reusable_connection = self.create_connection()
                    finally:
                        logger.error(
                            f"[{self._ds_id}] Connection[{id(self._reusable_connection)}] closed due to error."
                        )
                        self._reusable_connection = self.create_connection()
            sleep(30)
