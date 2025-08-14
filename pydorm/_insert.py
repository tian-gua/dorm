from dataclasses import fields
from typing import Any, TypeVar, Tuple, List, Dict

from ._middlewares import get_middlewares
from .enums import Middleware
from .mysql import MysqlDataSource, ReusableMysqlConnection
from .utils.random_utils import generate_random_string

T = TypeVar("T")


class Insert:
    def __init__(self, table: str, database: str | None, data_source: MysqlDataSource):
        self._operation_id = generate_random_string("update-", 10)

        self._table: str = table
        self._database = database
        self._data_source = data_source

        if self._table is None or self._table == "":
            raise ValueError("table is required")

        self._model = data_source.get_model(self._database, self._table)
        self._model_field_names = [f.name for f in fields(self._model)]  # type: ignore

    def insert(
        self, data: Any, conn: ReusableMysqlConnection | None = None
    ) -> Tuple[int, int]:
        if data is None:
            raise ValueError("null data")

        data = self._process_data(data)
        self._apply_before_insert_middlewares(data)
        sql, args = self._build_insert(data)
        return self._execute(sql, args, conn)

    def _execute(
        self,
        sql: str,
        args: Tuple[Any, ...],
        conn: ReusableMysqlConnection | None = None,
    ) -> Tuple[int, int]:
        if conn is None:
            new_conn = self._data_source.get_reusable_connection()
            try:
                new_conn.acquire(operation_id=self._operation_id)
                new_conn.begin()
                row_affected, last_row_id = self._data_source.get_executor().execute(
                    new_conn, sql, args
                )
                new_conn.commit()
                return row_affected, last_row_id
            finally:
                new_conn.release(operation_id=self._operation_id)
        return self._data_source.get_executor().execute(conn, sql, args)

    # noinspection PyMethodMayBeStatic
    def _process_data(self, data):
        # 判断 data 是否为dict，如果不是，则转换成dict
        if data is not None and not isinstance(data, dict):
            data = data.__dict__ if hasattr(data, "__dict__") else data.__dict__
        # 移除 value 为 None 的字段
        data = {k: v for k, v in data.items() if v is not None}
        return data

    def _build_insert(self, data: Dict) -> Tuple[str, Tuple[Any, ...]]:
        placeholder = []
        keys = []
        values = []
        for k, v in data.items():
            if k in self._model_field_names:
                keys.append(k)
                values.append(v)
                placeholder.append("?")

        if len(keys) == 0:
            raise ValueError("no valid field found")

        table = (
            self._table if self._database is None else f"{self._database}.{self._table}"
        )
        sql = f'INSERT INTO {table}({",".join(keys)}) VALUES({",".join(placeholder)})'
        args = tuple(values)
        return sql, args

    def insert_bulk(
        self, data_list: List[Dict], conn: ReusableMysqlConnection | None = None
    ) -> int:
        if data_list is None or len(data_list) == 0:
            raise ValueError("data is required")

        sql, args = self._build_insert_bulk(data_list)
        return self._execute_many(sql, args, conn)

    def _execute_many(
        self, sql, args, conn: ReusableMysqlConnection | None = None
    ) -> int:
        if conn is None:
            new_conn = self._data_source.get_reusable_connection()
            try:
                new_conn.acquire(operation_id=self._operation_id)
                new_conn.begin()
                row_affected = self._data_source.get_executor().executemany(
                    new_conn, sql, args
                )
                new_conn.commit()
                return row_affected
            finally:
                new_conn.release(operation_id=self._operation_id)
        return self._data_source.get_executor().executemany(conn, sql, args)

    def _build_insert_bulk(self, data: List[Dict]) -> Tuple[str, List[Tuple[Any, ...]]]:
        placeholder = []
        keys = []
        for k, v in data[0].items():
            if k in self._model_field_names:
                keys.append(k)
                placeholder.append("?")

        if len(keys) == 0:
            raise ValueError("no valid field found")

        table = (
            self._table if self._database is None else f"{self._database}.{self._table}"
        )
        sql = f'INSERT INTO {table}({",".join(keys)}) VALUES({",".join(placeholder)})'
        args = [tuple(datum[k] for k in keys) for datum in data]
        return sql, args

    def upsert(
        self,
        data: any,
        update_fields: list[str] | None = None,
        conn: ReusableMysqlConnection | None = None,
    ) -> (int, int):
        if data is None:
            raise ValueError("null data")

        data = self._process_data(data)
        sql, args = self._build_upsert(data, update_fields)
        return self._execute(sql, args, conn)

    def _build_upsert(
        self, data: dict, update_fields: List[str] | None = None
    ) -> tuple[str, tuple[Any, ...]]:
        placeholder = []
        keys = []
        values = []
        for k, v in data.items():
            if k in self._model_field_names:
                keys.append(k)
                values.append(v)
                placeholder.append("?")

        if len(keys) == 0:
            raise ValueError("no valid field found")

        # check if update_fields is a sub list of keys
        if update_fields is None:
            update_fields = keys
        else:
            if len(update_fields) == 0 or not all(
                field in keys for field in update_fields
            ):
                raise ValueError("update fields are not valid")

        table = (
            self._table if self._database is None else f"{self._database}.{self._table}"
        )
        sql = f'INSERT INTO `{table}`({",".join(keys)}) VALUES({",".join(placeholder)})'
        if update_fields:
            sql += f' ON DUPLICATE KEY UPDATE {",".join([f"{k}=VALUES({k})" for k in update_fields])}'
        else:
            sql += f' ON DUPLICATE KEY UPDATE {",".join([f"{k}=VALUES({k})" for k in keys])}'

        args = tuple(values)
        return sql, args

    def upsert_bulk(
        self,
        data: List[Dict],
        update_fields: List[str] | None = None,
        conn: ReusableMysqlConnection | None = None,
    ) -> int:
        if data is None or len(data) == 0:
            raise ValueError("data is required")

        sql, args = self._build_upsert_bulk(data, update_fields)
        return self._execute_many(sql, args, conn)

    def _build_upsert_bulk(
        self, data: List[Dict], update_fields: List[str] | None = None
    ) -> Tuple[str, List[Tuple[Any, ...]]]:
        placeholder = []
        keys = []
        for k in data[0].keys():
            if k in self._model_field_names:
                keys.append(k)
                placeholder.append("%s")

        if len(keys) == 0:
            raise ValueError("no valid field found")

        # check if update_fields is a sub list of keys
        if update_fields is None:
            update_fields = keys
        else:
            if len(update_fields) == 0 or not all(
                field in keys for field in update_fields
            ):
                raise ValueError("update fields are not valid")

        table = (
            self._table if self._database is None else f"{self._database}.{self._table}"
        )
        sql = f'INSERT INTO `{table}`({",".join(keys)}) VALUES({",".join(placeholder)})'
        if update_fields:
            sql += f' ON DUPLICATE KEY UPDATE {",".join([f"{k}=VALUES({k})" for k in update_fields])}'
        else:
            sql += f' ON DUPLICATE KEY UPDATE {",".join([f"{k}=VALUES({k})" for k in keys])}'

        args = [tuple(datum[k] for k in keys) for datum in data]
        return sql, args

    def _apply_before_insert_middlewares(self, data: Dict):
        middlewares = get_middlewares(Middleware.BEFORE_INSERT)
        if middlewares is None or len(middlewares) == 0:
            return

        for middleware in middlewares:
            middleware(self, data)
