from dataclasses import fields
from typing import List, Any, TypeVar, Type, Generic

from ._middlewares import get_middlewares
from ._models import models
from ._mysql_executor import select_one, select_many
from ._where import Where, Or
from .enums import Middleware

T = TypeVar("T")


class RawQuery:
    def __init__(self, table: str, database: str | None, ds_id: str):
        self._table = table
        self._database = database

        if self._table is None or self._table == "":
            raise ValueError("table is required")

        self._ds_id = ds_id
        self._where = Where()
        self._select_fields = []
        self._ignore_fields = []
        self._order_by = None
        self._limit = None
        self._offset = None
        self._distinct = False

        self._model: callable = models.get(
            ds_id=self._ds_id, database=self._database, table=self._table
        )
        self._model_fields = [f.name for f in fields(self._model)]

    def select(self, *select_fields, distinct=False) -> "RawQuery":
        model_fields = [f.name for f in fields(self._model)]
        for select_field in select_fields:
            if select_field not in model_fields:
                raise ValueError(f"invalid field [{select_field}]")

        self._select_fields = select_fields
        self._distinct = distinct
        return self

    def ignore(self, *ignore_fields) -> "RawQuery":
        model_fields = [f.name for f in fields(self._model)]
        for ignore_field in ignore_fields:
            if ignore_field not in model_fields:
                raise ValueError(f"invalid field [{ignore_field}]")

        self._ignore_fields = ignore_fields
        return self

    def check_field(self, field: str):
        if field not in self._model_fields:
            raise ValueError(f"invalid field [{field}]")

    def eq(self, field: str, value: Any) -> "RawQuery":
        self.check_field(field)
        self._where.eq(field, value)
        return self

    def ne(self, field: str, value: Any) -> "RawQuery":
        self.check_field(field)
        self._where.ne(field, value)
        return self

    def gt(self, field: str, value: Any) -> "RawQuery":
        self.check_field(field)
        self._where.gt(field, value)
        return self

    def ge(self, field: str, value: Any) -> "RawQuery":
        self.check_field(field)
        self._where.ge(field, value)
        return self

    def lt(self, field: str, value: Any) -> "RawQuery":
        self.check_field(field)
        self._where.lt(field, value)
        return self

    def le(self, field: str, value: Any) -> "RawQuery":
        self.check_field(field)
        self._where.le(field, value)
        return self

    def in_(self, field: str, value: Any) -> "RawQuery":
        self.check_field(field)
        self._where.in_(field, value)
        return self

    def l_like(self, field: str, value: Any) -> "RawQuery":
        self.check_field(field)
        self._where.l_like(field, value)
        return self

    def r_like(self, field: str, value: Any) -> "RawQuery":
        self.check_field(field)
        self._where.r_like(field, value)
        return self

    def like(self, field: str, value: Any) -> "RawQuery":
        self.check_field(field)
        self._where.like(field, value)
        return self

    def or_(self, or_: Or) -> "RawQuery":
        self._where.or_(or_)
        return self

    def desc(self, *order_by) -> "RawQuery":
        self._order_by = [f"{field} desc" for field in order_by]
        return self

    def asc(self, *order_by) -> "RawQuery":
        self._order_by = [f"{field} asc" for field in order_by]
        return self

    def limit(self, limit: int) -> "RawQuery":
        self._limit = limit
        return self

    def offset(self, offset: int) -> "RawQuery":
        self._offset = offset
        return self

    def one(self) -> dict[str, Any] | None:
        if len(self._select_fields) == 0:
            self._select_fields = [f.name for f in fields(self._model)]

        self._apply_before_query_middlewares()

        sql, args = self._build_select()

        return select_one(self._ds_id, sql, args)

    def list(self) -> list[dict[str, Any]]:
        if len(self._select_fields) == 0:
            self._select_fields = [f.name for f in fields(self._model)]

        self._apply_before_query_middlewares()

        sql, args = self._build_select()

        rows = select_many(self._ds_id, sql, args)
        if rows is None:
            return []
        return rows

    def page(self, page: int, page_size: int) -> tuple[List[dict[str, Any]], int]:
        if len(self._select_fields) == 0:
            self._select_fields = [f.name for f in fields(self._model)]

        self._apply_before_query_middlewares()

        self._limit = page_size
        self._offset = (page - 1) * page_size

        sql, args = self._build_select()
        count = self._count()
        if count == 0:
            return [], count

        rows = select_many(self._ds_id, sql, args)
        if rows is None:
            return [], count
        return rows, count

    def _count(self) -> int:
        table = (
            self._table if self._database is None else f"{self._database}.{self._table}"
        )
        sql = f"SELECT COUNT(*) FROM {table}"
        args = ()
        tree = self._where.tree()
        if len(tree.conditions) > 0:
            exp, args = tree.parse()
            sql += " WHERE " + exp

        return select_one(self._ds_id, sql, args)["COUNT(*)"]

    def count(self) -> int:
        self._apply_before_query_middlewares()
        return self._count()

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


class Query(RawQuery, Generic[T]):
    def __init__(self, cls: Type[T], database: str | None, ds_id: str):
        if not hasattr(cls, "__table_name__"):
            raise ValueError("invalid model class")
        super().__init__(cls.__table_name__, database, ds_id)
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

    def one(self) -> T | None:
        if len(self._select_fields) == 0:
            self._select_fields = [f.name for f in fields(self._cls)]

        row = super().one()
        if row is None:
            return None
        return self._cls(**row)

    def list(self) -> List[T]:
        if len(self._select_fields) == 0:
            self._select_fields = [f.name for f in fields(self._cls)]

        rows = super().list()
        if len(rows) == 0:
            return rows
        return [self._cls(**row) for row in rows]

    def page(self, page: int, page_size: int) -> tuple[List, int]:
        if len(self._select_fields) == 0:
            self._select_fields = [f.name for f in fields(self._cls)]

        rows, count = super().page(page, page_size)
        if len(rows) == 0:
            return rows, count
        return [self._cls(**row) for row in rows], count


def query(cls: Type[T], database: str | None = None, ds_id: str = "default"):
    return Query(cls, database, ds_id)


def raw_query(table: str, database: str | None = None, ds_id: str = "default"):
    return RawQuery(table, database, ds_id)
