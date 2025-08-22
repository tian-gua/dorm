from dataclasses import field, make_dataclass
from dataclasses import field, make_dataclass
from typing import Dict, List, Any, Type

from loguru import logger
from pymysql.connections import Connection

from pydorm.mysql._reusable_mysql_connection import ReusableMysqlConnection
from ._mysql_executor import mysql_executor, MysqlExecutor
from ._mysql_table_inspector import mysql_table_inspector


class MysqlDataSource:
    def __init__(
        self,
        data_source_id: str,
        host: str,
        port: int,
        user: str,
        password: str,
        database: str,
        **options: Any,
    ):
        self._data_source_id: str = data_source_id
        self._dialect: str = "mysql"
        self._host: str = host
        self._port: int = port
        self._user: str = user
        self._password: str = password
        self._database: str = database
        self._reusable_connection: ReusableMysqlConnection = ReusableMysqlConnection(
            self._data_source_id,
            self.create_connection,
        )
        self._executor: MysqlExecutor = mysql_executor
        self._models: Dict[str, Type[Any]] = {}
        self._options: Dict[str, Any] = options

    def get_id(self) -> str:
        return self._data_source_id

    def get_dialect(self) -> str:
        return self._dialect

    def get_database(self) -> str:
        return self._database

    def create_connection(self) -> Connection:
        import pymysql
        from pymysql.cursors import DictCursor

        try:
            conn = pymysql.connect(
                host=self._host,
                port=self._port,
                user=self._user,
                password=self._password,  # 移除编码
                database=self._database,
                cursorclass=DictCursor,
                charset="utf8mb4",  # 添加字符集
                autocommit=False,  # 设置自动提交为False,
                **self._options,  # 传递其他选项
            )
            logger.info(f"[{self._data_source_id}] create connection [{id(conn)}]")
            return conn
        except Exception as e:
            logger.error(f"[{self._data_source_id}] Failed to create connection: {e}")
            raise

    def get_reusable_connection(self) -> ReusableMysqlConnection:
        return self._reusable_connection

    def close(self):
        """关闭数据源和相关连接"""
        if self._reusable_connection:
            self._reusable_connection.close()
            logger.info(f"[{self._data_source_id}] DataSource closed")

    def get_executor(self) -> MysqlExecutor:
        return self._executor

    def get_model(self, database: str | None, table: str) -> Type[Any]:
        key = f"{database or self._database}.{table}"
        if key not in self._models:
            table_structure: List[Dict] = mysql_table_inspector.load_structure(
                self._reusable_connection, database or self._database, table
            )
            fields = [
                (table_field["field_"], any, field(default=None)) for table_field in table_structure
            ]
            self._models[key] = make_dataclass(key, fields=fields)
        return self._models[key]

    def remove_model(self, database: str | None, table: str):
        key = f"{database or self._database}.{table}"
        if key in self._models:
            del self._models[key]
            logger.info(f"[{self._data_source_id}] Model {key} removed")
        else:
            logger.warning(f"[{self._data_source_id}] Model {key} not found")

    def load_structure(self, database: str, table: str = "") -> List[Dict[str, Any]]:
        """加载数据源的表结构"""
        if not table:
            raise ValueError("Table name must be provided to load structure")
        return mysql_table_inspector.load_structure(
            self._reusable_connection, database or self._database, table
        )
