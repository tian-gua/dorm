import pymysql
from loguru import logger
from pymysql.cursors import DictCursor


class MysqlDataSource:
    def __init__(self, ds_id, host, port, user, password, database):
        self._ds_id = ds_id
        self._dialect = "mysql"
        self._host = host
        self._port = port
        self._user = user
        self._password = password
        self._default_database = database

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
