import time

import pymysql
from loguru import logger
from pymysql.connections import Connection
from pymysql.cursors import DictCursor


class MysqlDataSource:
    def __init__(self, id_, host, port, user, password, database):
        self._id = id_
        self._dialect = 'mysql'
        self._host = host
        self._port = port
        self._user = user
        self._password = password
        self._default_database = database
        self._conn: Connection | None = None
        self._conn_create_time = time.time()

    def get_id(self) -> str:
        return self._id

    def get_host(self) -> str:
        return self._host

    def get_port(self) -> int:
        return self._port

    def get_user(self) -> str:
        return self._user

    def get_password(self):
        return self._password

    def get_default_database(self) -> str:
        return self._default_database

    def get_connection(self):
        if self._conn is None:
            return self._create_connection()

        # ping every 60 seconds and reconnect if necessary
        if time.time() - self._conn_create_time > 60:
            self._conn.ping(True)
            self._conn_create_time = time.time()
        return self._conn

    def _create_connection(self):
        conn = pymysql.connect(host=self._host,
                               port=self._port,
                               user=self._user,
                               password=self._password.encode('utf-8'),
                               database=self._default_database,
                               cursorclass=DictCursor)
        logger.info(f'create connection [{id(conn)}]')

        self._conn = conn
        self._conn_create_time = time.time()
        return self._conn
