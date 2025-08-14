from .utils.random_utils import generate_random_string
from dataclasses import fields
from typing import List, Any, TypeVar, Type, Generic

from ._middlewares import get_middlewares
from ._where import Where, Or
from .enums import Middleware
from .mysql import MysqlDataSource, ReusableMysqlConnection

T = TypeVar("T")


class DictQuery:
    def __init__(
        self,
        table: str,
        database: str | None,
        data_source: MysqlDataSource,
    ):
        self._operation_id = generate_random_string("query-", 10)

        self._table = table
        self._database = database

        if self._table is None or self._table == "":
            raise ValueError("table is required")

        self._data_source = data_source
        self._where = Where()
        self._select_fields = []
        self._ignore_fields = []
        self._order_by = None
        self._limit = None
        self._offset = None
        self._distinct = False

        self._model = data_source.get_model(self._database, self._table)
        self._model_field_names = [f.name for f in fields(self._model)]  # type: ignore

    def select(self, *select_fields, distinct=False) -> "DictQuery":
        for select_field in select_fields:
            if select_field not in self._model_field_names:
                raise ValueError(f"invalid field [{select_field}]")

        self._select_fields = select_fields
        self._distinct = distinct
        return self

    def ignore(self, *ignore_fields) -> "DictQuery":

        for ignore_field in ignore_fields:
            if ignore_field not in self._model_field_names:
                raise ValueError(f"invalid field [{ignore_field}]")

        self._ignore_fields = ignore_fields
        return self

    def check_field(self, field: str):
        if field not in self._model_field_names:
            raise ValueError(f"invalid field [{field}]")

    def eq(self, field: str, value: Any) -> "DictQuery":
        self.check_field(field)
        self._where.eq(field, value)
        return self

    def ne(self, field: str, value: Any) -> "DictQuery":
        self.check_field(field)
        self._where.ne(field, value)
        return self

    def gt(self, field: str, value: Any) -> "DictQuery":
        self.check_field(field)
        self._where.gt(field, value)
        return self

    def ge(self, field: str, value: Any) -> "DictQuery":
        self.check_field(field)
        self._where.ge(field, value)
        return self

    def lt(self, field: str, value: Any) -> "DictQuery":
        self.check_field(field)
        self._where.lt(field, value)
        return self

    def le(self, field: str, value: Any) -> "DictQuery":
        self.check_field(field)
        self._where.le(field, value)
        return self

    def in_(self, field: str, value: Any) -> "DictQuery":
        self.check_field(field)
        self._where.in_(field, value)
        return self

    def l_like(self, field: str, value: Any) -> "DictQuery":
        self.check_field(field)
        self._where.l_like(field, value)
        return self

    def r_like(self, field: str, value: Any) -> "DictQuery":
        self.check_field(field)
        self._where.r_like(field, value)
        return self

    def like(self, field: str, value: Any) -> "DictQuery":
        self.check_field(field)
        self._where.like(field, value)
        return self

    def or_(self, or_: Or) -> "DictQuery":
        self._where.or_(or_)
        return self

    def desc(self, *order_by) -> "DictQuery":
        self._order_by = [f"{field} desc" for field in order_by]
        return self

    def asc(self, *order_by) -> "DictQuery":
        self._order_by = [f"{field} asc" for field in order_by]
        return self

    def limit(self, limit: int) -> "DictQuery":
        self._limit = limit
        return self

    def offset(self, offset: int) -> "DictQuery":
        self._offset = offset
        return self

    def one(self, conn: ReusableMysqlConnection | None = None) -> dict[str, Any] | None:
        if len(self._select_fields) == 0:
            self._select_fields = self._model_field_names

        self._apply_before_query_middlewares()

        sql, args = self._build_select()

        if conn is None:
            new_conn = self._data_source.get_reusable_connection()
            try:
                new_conn.acquire(operation_id=self._operation_id)
                new_conn.begin()
                result = self._data_source.get_executor().select_one(
                    new_conn, sql, args
                )
                new_conn.commit()
                return result
            finally:
                new_conn.release(operation_id=self._operation_id)
        else:
            return self._data_source.get_executor().select_one(conn, sql, args)

    def list(self, conn: ReusableMysqlConnection | None = None) -> list[dict[str, Any]]:
        if len(self._select_fields) == 0:
            self._select_fields = self._model_field_names

        self._apply_before_query_middlewares()

        sql, args = self._build_select()

        if conn is None:
            new_conn = self._data_source.get_reusable_connection()
            try:
                new_conn.acquire(operation_id=self._operation_id)
                new_conn.begin()
                rows = self._data_source.get_executor().select_many(new_conn, sql, args)
                new_conn.commit()
                return rows
            finally:
                new_conn.release(operation_id=self._operation_id)

        return self._data_source.get_executor().select_many(conn, sql, args)

    def page(
        self, page: int, page_size: int, conn: ReusableMysqlConnection | None = None
    ) -> tuple[List[dict[str, Any]], int]:
        if len(self._select_fields) == 0:
            self._select_fields = self._model_field_names

        self._apply_before_query_middlewares()

        self._limit = page_size
        self._offset = (page - 1) * page_size

        sql, args = self._build_select()

        if conn is None:
            new_conn = self._data_source.get_reusable_connection()
            count = self._count(new_conn)
            if count == 0:
                return [], count
            try:
                new_conn.acquire(operation_id=self._operation_id)
                new_conn.begin()
                rows = self._data_source.get_executor().select_many(new_conn, sql, args)
                new_conn.commit()
                return rows, count
            finally:
                new_conn.release(operation_id=self._operation_id)

        count = self._count(conn)
        if count == 0:
            return [], count
        rows = self._data_source.get_executor().select_many(conn, sql, args)
        return rows, count

    def _count(self, conn: ReusableMysqlConnection) -> int:
        table = (
            self._table if self._database is None else f"{self._database}.{self._table}"
        )
        sql = f"SELECT COUNT(*) FROM {table}"
        args = ()
        tree = self._where.tree()
        if len(tree.conditions) > 0:
            exp, args = tree.parse()
            sql += " WHERE " + exp

        return self._data_source.get_executor().select_one(conn, sql, args)["COUNT(*)"]

    def count(self, conn: ReusableMysqlConnection | None = None) -> int:
        self._apply_before_query_middlewares()

        if conn is None:
            new_conn = self._data_source.get_reusable_connection()
            try:
                new_conn.acquire()
                new_conn.begin()
                count = self._count(new_conn)
                new_conn.commit()
                return count
            finally:
                new_conn.release(operation_id=self._operation_id)
        return self._count(conn)

    def _build_select(self) -> tuple[str, tuple[Any, ...]]:
        select_fields = [
            field for field in self._select_fields if field not in self._ignore_fields
        ]

        table = (
            self._table if self._database is None else f"{self._database}.{self._table}"
        )
        sql = f'SELECT {"DISTINCT " if self._distinct and self._select_fields else ""}{",".join(select_fields)} FROM {table}'
        args = ()
        tree = self._where.tree()
        if len(tree.conditions) > 0:
            exp, args = tree.parse()
            sql += " WHERE " + exp
        if self._order_by is not None:
            sql += f' ORDER BY {",".join(self._order_by)}'
        if self._limit is not None:
            sql += f" LIMIT {self._limit}"
        if self._offset is not None:
            sql += f" OFFSET {self._offset}"

        return sql, args

    def _apply_before_query_middlewares(self):
        middlewares = get_middlewares(Middleware.BEFORE_QUERY)
        if middlewares is None or len(middlewares) == 0:
            return
        for middleware in middlewares:
            if callable(middleware):
                middleware(self)


class Query(DictQuery, Generic[T]):
    def __init__(
        self, cls: Type[T], database: str | None, data_source: MysqlDataSource
    ):
        if not hasattr(cls, "__table_name__"):
            raise ValueError("invalid model class")
        super().__init__(cls.__table_name__, database, data_source)
        self._cls = cls

    def eq(self, field: str, value: Any) -> "Query":
        super().eq(field, value)
        return self

    def ne(self, field: str, value: Any) -> "Query":
        super().ne(field, value)
        return self

    def gt(self, field: str, value: Any) -> "Query":
        super().gt(field, value)
        return self

    def ge(self, field: str, value: Any) -> "Query":
        super().ge(field, value)
        return self

    def lt(self, field: str, value: Any) -> "Query":
        super().lt(field, value)
        return self

    def le(self, field: str, value: Any) -> "Query":
        super().le(field, value)
        return self

    def in_(self, field: str, value: Any) -> "Query":
        super().in_(field, value)
        return self

    def l_like(self, field: str, value: Any) -> "Query":
        super().l_like(field, value)
        return self

    def r_like(self, field: str, value: Any) -> "Query":
        super().r_like(field, value)
        return self

    def like(self, field: str, value: Any) -> "Query":
        super().like(field, value)
        return self

    def or_(self, or_: Or) -> "Query":
        super().or_(or_)
        return self

    def desc(self, *order_by) -> "Query":
        super().desc(*order_by)
        return self

    def asc(self, *order_by) -> "Query":
        super().asc(*order_by)
        return self

    def limit(self, limit: int) -> "Query":
        self._limit = limit
        return self

    def offset(self, offset: int) -> "Query":
        self._offset = offset
        return self

    def one(self, conn: ReusableMysqlConnection | None = None) -> T | None:
        if len(self._select_fields) == 0:
            self._select_fields = [f.name for f in fields(self._cls)]

        row = super().one(conn)
        if row is None:
            return None
        return self._cls(**row)

    def list(self, conn: ReusableMysqlConnection | None = None) -> List[T]:
        if len(self._select_fields) == 0:
            self._select_fields = [f.name for f in fields(self._cls)]

        rows = super().list(conn)
        return [self._cls(**row) for row in rows]

    def page(
        self, page: int, page_size: int, conn: ReusableMysqlConnection | None = None
    ) -> tuple[List, int]:
        if len(self._select_fields) == 0:
            self._select_fields = [f.name for f in fields(self._cls)]

        rows, count = super().page(page, page_size, conn)
        return [self._cls(**row) for row in rows], count
