from dataclasses import fields
from typing import Any, TypeVar

from ._middlewares import get_middlewares
from ._where import Where, Or
from .enums import Middleware
from .mysql import MysqlDataSource, ReusableMysqlConnection
from .utils.random_utils import generate_random_string

T = TypeVar("T")


class Update:
    def __init__(self, table: str, database: str | None, data_source: MysqlDataSource):
        self._operation_id = generate_random_string("update-", 10)

        self._table: str = table
        self._database = database

        if self._table is None or self._table == "":
            raise ValueError("table is required")

        self._where = Where()
        self._update_fields: dict = {}

        self._data_source = data_source

        self._model = data_source.get_model(self._database, self._table)
        self._model_field_names = [f.name for f in fields(self._model)]  # type: ignore

    def check_field(self, field: str):
        if field not in self._model_field_names:
            raise ValueError(f"invalid field [{field}]")

    def has_field(self, field: str):
        return field in self._model_field_names

    def eq(self, field: str, value: Any) -> "Update":
        self.check_field(field)
        self._where.eq(field, value)
        return self

    def ne(self, field: str, value: Any) -> "Update":
        self.check_field(field)
        self._where.ne(field, value)
        return self

    def gt(self, field: str, value: Any) -> "Update":
        self.check_field(field)
        self._where.gt(field, value)
        return self

    def ge(self, field: str, value: Any) -> "Update":
        self.check_field(field)
        self._where.ge(field, value)
        return self

    def lt(self, field: str, value: Any) -> "Update":
        self.check_field(field)
        self._where.lt(field, value)
        return self

    def le(self, field: str, value: Any) -> "Update":
        self.check_field(field)
        self._where.le(field, value)
        return self

    def in_(self, field: str, value: Any) -> "Update":
        self.check_field(field)
        self._where.in_(field, value)
        return self

    def l_like(self, field: str, value: Any) -> "Update":
        self.check_field(field)
        self._where.l_like(field, value)
        return self

    def r_like(self, field: str, value: Any) -> "Update":
        self.check_field(field)
        self._where.r_like(field, value)
        return self

    def like(self, field: str, value: Any) -> "Update":
        self.check_field(field)
        self._where.like(field, value)
        return self

    def or_(self, or_: Or) -> "Update":
        self._where.or_(or_)
        return self

    def set(self, set_args: dict = None, **args) -> "Update":
        if set_args is None:
            set_args = {}

        set_args.update(args)

        valid_fields = {}
        for k, v in set_args.items():
            if self.has_field(k):
                valid_fields[k] = v

        if len(valid_fields) == 0:
            raise ValueError("valid fields is required")

        self._update_fields = valid_fields
        return self

    def update(self, conn: ReusableMysqlConnection | None = None) -> int:
        if self._where.count() == 0:
            # danger operation
            raise ValueError("update all is not supported")

        if len(self._update_fields) == 0:
            raise ValueError("update fields is required")

        sql, args = self._build_update()
        return self._execute(sql, args, conn)

    def _build_update(self) -> tuple[str, tuple[Any, ...]]:
        table = (
            self._table if self._database is None else f"{self._database}.{self._table}"
        )
        sql = f'UPDATE {table} SET {",".join([f"{k}=?" for k in self._update_fields.keys()])}'
        args = tuple(self._update_fields.values())

        exp, args2 = self._where.tree().parse()
        sql += " WHERE " + exp
        args += args2
        return sql, args

    def delete(self, conn: ReusableMysqlConnection | None = None) -> int:
        if self._where.count() == 0:
            # danger operation
            raise ValueError("delete all is not supported")

        sql, args = self._build_delete()
        return self._execute(sql, args, conn)

    def _execute(self, sql, args, conn: ReusableMysqlConnection | None = None) -> int:
        if conn is None:
            new_conn = self._data_source.get_reusable_connection()
            try:
                new_conn.acquire(operation_id=self._operation_id)
                new_conn.begin()
                row_affected, last_row_id = self._data_source.get_executor().execute(
                    new_conn, sql, args
                )
                new_conn.commit()
                return row_affected or 0
            finally:
                new_conn.release(operation_id=self._operation_id)
        row_affected, last_row_id = self._data_source.get_executor().execute(
            conn, sql, args
        )
        return row_affected or 0

    def _build_delete(self) -> tuple[str, tuple[Any, ...]]:
        table = (
            self._table if self._database is None else f"{self._database}.{self._table}"
        )
        sql = f"DELETE FROM {table}"

        exp, args = self._where.tree().parse()
        sql += " WHERE " + exp
        return sql, args

    def _apply_before_update_middlewares(self):
        middlewares = get_middlewares(Middleware.BEFORE_UPDATE)
        if middlewares is None or len(middlewares) == 0:
            return
        for middleware in middlewares:
            middleware(self)
