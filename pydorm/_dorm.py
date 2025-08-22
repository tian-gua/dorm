from dataclasses import asdict
from typing import Any, Dict, List, Literal, Tuple, Type, TypeVar

from loguru import logger
from pymysql.cursors import DictCursor

from ._data_source_storage import DataSourceStorage
from ._delete import delete
from ._delete_wrapper import DeleteWrapper
from ._insert import insert, insert_bulk
from ._insert_wrapper import InsertWrapper
from ._query import find, find_dict
from ._query import list as list_obj
from ._query import list_dict
from ._query_wrapper import QueryWrapper
from ._update import update
from ._update_wrapper import UpdateWrapper
from .mysql import ReusableMysqlConnection
from .utils.random_utils import generate_random_string

T = TypeVar("T", bound=Any)


class Dorm:
    def __init__(self):
        self._init = False
        self._config_dict = None
        self._dss = DataSourceStorage()

        self._tx_id = None

    def is_initialized(self):
        return self._init

    def init(self, config_dict: Dict[str, Any]):
        self._config_dict = config_dict
        self._dss.load(config_dict)
        self._init = True
        logger.info("dorm initialized")

    def qw(self, cls: Type[T]) -> QueryWrapper[T]:
        return QueryWrapper[T](cls)

    def uw(self, cls: Type[T]) -> UpdateWrapper[T]:
        return UpdateWrapper[T](cls)

    def dw(self, cls: Type[T]) -> DeleteWrapper[T]:
        return DeleteWrapper[T](cls)

    def find(
        self,
        wrapper: QueryWrapper[T],
        conn: ReusableMysqlConnection | None = None,
        data_source_id="default",
    ) -> T | None:
        ds = self._dss.get(data_source_id)
        if ds is None:
            raise ValueError(f"Data source with ID '{data_source_id}' not found")
        return find(wrapper, conn=conn, data_source=ds)

    def find_dict(
        self,
        wrapper: QueryWrapper[T],
        conn: ReusableMysqlConnection | None = None,
        data_source_id="default",
    ) -> Dict[str, Any] | None:
        ds = self._dss.get(data_source_id)
        if ds is None:
            raise ValueError(f"Data source with ID '{data_source_id}' not found")
        return find_dict(wrapper, conn=conn, data_source=ds)

    def list(
        self,
        wrapper: QueryWrapper[T],
        conn: ReusableMysqlConnection | None = None,
        data_source_id="default",
    ) -> List[T]:
        ds = self._dss.get(data_source_id)
        if ds is None:
            raise ValueError(f"Data source with ID '{data_source_id}' not found")
        return list_obj(wrapper, conn=conn, data_source=ds)

    def list_dict(
        self,
        wrapper: QueryWrapper[T],
        conn: ReusableMysqlConnection | None = None,
        data_source_id="default",
    ) -> List[Dict[str, Any]]:
        ds = self._dss.get(data_source_id)
        if ds is None:
            raise ValueError(f"Data source with ID '{data_source_id}' not found")
        return list_dict(wrapper, conn=conn, data_source=ds)

    def insert(
        self,
        cls: Type[T],
        data: Dict[str, Any] | T,
        duplicate_key_update: List[str] | Literal["all"] | None = None,
        conn: ReusableMysqlConnection | None = None,
        data_source_id="default",
    ) -> Tuple[int, int]:
        ds = self._dss.get(data_source_id)
        if ds is None:
            raise ValueError(f"Data source with ID '{data_source_id}' not found")
        wrapper = InsertWrapper[T](cls)
        dict_data: Dict[str, Any] = data if isinstance(data, Dict) else asdict(data)
        return insert(wrapper, dict_data, duplicate_key_update, conn=conn, data_source=ds)

    def insert_bulk(
        self,
        cls: Type[T],
        data: List[Dict[str, Any]],
        duplicate_key_update: List[str] | Literal["all"] | None = None,
        conn: ReusableMysqlConnection | None = None,
        data_source_id="default",
    ) -> int:
        ds = self._dss.get(data_source_id)
        if ds is None:
            raise ValueError(f"Data source with ID '{data_source_id}' not found")

        wrapper = InsertWrapper[T](cls)
        return insert_bulk(wrapper, data, duplicate_key_update, conn=conn, data_source=ds)

    def update(
        self,
        wrapper: UpdateWrapper[T],
        conn: ReusableMysqlConnection | None = None,
        data_source_id="default",
    ) -> int:
        ds = self._dss.get(data_source_id)
        if ds is None:
            raise ValueError(f"Data source with ID '{data_source_id}' not found")
        return update(wrapper, conn=conn, data_source=ds)

    def delete(
        self,
        wrapper: DeleteWrapper[T],
        conn: ReusableMysqlConnection | None = None,
        data_source_id="default",
    ) -> int:
        ds = self._dss.get(data_source_id)
        if ds is None:
            raise ValueError(f"Data source with ID '{data_source_id}' not found")
        return delete(wrapper, conn=conn, data_source=ds)

    def raw_query(
        self,
        sql: str,
        args: tuple[Any, ...],
        conn: ReusableMysqlConnection | None = None,
        data_source_id="default",
    ) -> List[Dict[str, Any]]:
        raw_query_id = generate_random_string("raw-query-", 10)

        sql = sql.replace("?", "%s")

        if conn is None:
            ds = self._dss.get(data_source_id)
            if ds is None:
                raise ValueError(f"Data source with ID '{data_source_id}' not found")
            new_conn = ds.get_reusable_connection()
            try:
                new_conn.acquire(operation_id=raw_query_id)
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
                new_conn.release(operation_id=raw_query_id)
        else:
            cursor: DictCursor = conn.cursor()
            result = cursor.execute(sql, args)
            if result is None:
                return []
            rows = cursor.fetchall()
            if rows is None:
                return []
            return list(rows)

    def begin(self, data_source_id="default") -> ReusableMysqlConnection:
        self._tx_id = generate_random_string("tx-", 10)

        data_source = self._dss.get(data_source_id)
        if data_source is None:
            raise ValueError(f"Data source with ID '{data_source_id}' not found")
        conn = data_source.get_reusable_connection()
        conn.acquire(operation_id=self._tx_id)
        try:
            conn.begin()
            return conn
        except Exception as e:
            logger.error(f"Failed to begin transaction on data source '{data_source_id}': {e}")
            conn.release(operation_id=self._tx_id)
            raise e

    def commit(self, conn: ReusableMysqlConnection):
        if conn is None:
            raise RuntimeError("No connection to commit")
        try:
            conn.commit()
        except Exception as e:
            logger.error(f"Failed to commit transaction: {e}")
            raise e
        finally:
            conn.release(operation_id=self._tx_id)

    def rollback(self, conn: ReusableMysqlConnection):
        if conn is None:
            raise RuntimeError("No connection to rollback")
        try:
            conn.rollback()
        except Exception as e:
            logger.error(f"Failed to rollback transaction: {e}")
            raise e
        finally:
            conn.release(operation_id=self._tx_id)

    def add_data_source(
        self,
        data_source_id: str,
        dialect: str,
        host: str,
        port: int,
        user: str,
        password: str,
        database: str,
        **options: Any,
    ):
        self._dss.add_datasource(
            data_source_id, dialect, host, port, user, password, database, **options
        )

    def get_data_source(self, data_source_id="default"):
        return self._dss.get(data_source_id)


dorm = Dorm()
