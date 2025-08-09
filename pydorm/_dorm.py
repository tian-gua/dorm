from typing import Type, TypeVar, List, Dict, Any

from loguru import logger
from pymysql.cursors import DictCursor

from ._data_source_storage import DataSourceStorage
from ._insert import Insert
from ._query import Query, DictQuery
from ._update import Update
from .mysql import ReusableMysqlConnection

T = TypeVar("T")


class Dorm:
    def __init__(self):
        self._init = False
        self._config_dict = None
        self._dss = DataSourceStorage()

    def is_initialized(self):
        return self._init

    def init(self, config_dict):
        self._config_dict = config_dict
        self._dss.load(config_dict)
        self._init = True
        logger.info("dorm initialized")

    def query(
        self, cls: Type[T], database: str | None = None, data_source_id="default"
    ):
        return Query(cls, database=database, data_source=self._dss.get(data_source_id))

    def dict_query(
        self, cls: Type[T], database: str | None = None, data_source_id="default"
    ):
        return DictQuery(
            cls, database=database, data_source=self._dss.get(data_source_id)
        )

    def raw_query(
        self,
        sql: str,
        args: tuple[any, ...],
        conn: ReusableMysqlConnection | None,
        data_source_id="default",
    ) -> List[Dict]:

        sql = sql.replace("?", "%s")

        if conn is None:
            new_conn = self._dss.get(data_source_id).get_reusable_connection()
            try:
                new_conn.acquire()
                new_conn.begin()
                cursor: DictCursor = new_conn.cursor()
                result = cursor.execute(sql, args)
                if result is None:
                    return []
                rows = cursor.fetchall()
                if rows is None:
                    return []
                return list(rows)
            finally:
                new_conn.release()
        else:
            cursor: DictCursor = conn.cursor()
            result = cursor.execute(sql, args)
            if result is None:
                return []
            rows = cursor.fetchall()
            if rows is None:
                return []
            return list(rows)

    def update(
        self,
        table_or_cls: str | Type[T],
        database: str | None = None,
        data_source_id="default",
    ) -> Update:
        if isinstance(table_or_cls, str):
            table = table_or_cls
        else:
            table = table_or_cls.__table_name__
        return Update(table, database, data_source=self._dss.get(data_source_id))

    def insert(
        self,
        table_or_cls: str | Type[T],
        database: str | None = None,
        data_source_id="default",
    ) -> Insert:
        if isinstance(table_or_cls, str):
            table = table_or_cls
        else:
            table = table_or_cls.__table_name__
        return Insert(table, database, data_source=self._dss.get(data_source_id))

    def begin(self, data_source_id="default") -> ReusableMysqlConnection:
        data_source = self._dss.get(data_source_id)
        conn = data_source.get_reusable_connection()
        conn.acquire()
        try:
            conn.begin()
            return conn
        except Exception as e:
            logger.error(
                f"Failed to begin transaction on data source '{data_source_id}': {e}"
            )
            conn.release()
            raise e

    # noinspection PyMethodMayBeStatic
    def commit(self, conn: ReusableMysqlConnection):
        if conn is None:
            raise RuntimeError("No connection to commit")
        try:
            conn.commit()
        except Exception as e:
            logger.error(f"Failed to commit transaction: {e}")
            raise e
        finally:
            conn.release()

    # noinspection PyMethodMayBeStatic
    def rollback(self, conn: ReusableMysqlConnection):
        if conn is None:
            raise RuntimeError("No connection to rollback")
        try:
            conn.rollback()
        except Exception as e:
            logger.error(f"Failed to rollback transaction: {e}")
            raise e
        finally:
            conn.release()

    def add_data_source(
        self,
        data_source_id: str,
        dialect: str,
        host: str,
        port: int,
        user: str,
        password: str,
        database: str,
    ):
        self._dss.add_datasource(
            data_source_id, dialect, host, port, user, password, database
        )

    def get_data_source(self, data_source_id="default"):
        return self._dss.get(data_source_id)


dorm = Dorm()
