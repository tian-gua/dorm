from dataclasses import fields
from typing import List, Any

from ._dorm import dorm
from ._models import models
from ._mysql_executor import select_one, select_many
from ._where import Where, Or
from .protocols import IEntity, IDataSource


class DictQuery:
    def __init__(self, table: str | IEntity, datasource: IDataSource, database: str):
        if isinstance(table, str):
            self._table: str = table
            self._entity: IEntity | None = None
        else:
            self._table: str = table.__table_name__
            self._entity: IEntity | None = table

        self._datasource = datasource
        self._database = database

        if self._table is None or self._table == '':
            raise ValueError('table is required')
        if self._datasource is None or self._datasource == '':
            raise ValueError('datasource is required')
        if not isinstance(self._datasource, IDataSource):
            raise ValueError('datasource must be an instance of IDataSource')
        if self._database is None or self._database == '':
            raise ValueError('database is required')

        self._where = Where(self._entity)
        self._select_fields = []
        self._ignore_fields = []
        self._order_by = None
        self._limit = None
        self._offset = None
        self._distinct = False

        self._model: callable = models.get(data_source=self._datasource, database=self._database, table=self._table)

    def select(self, *select_fields, distinct=False) -> 'DictQuery':
        if self._entity is not None:
            for field in select_fields:
                if not hasattr(self._entity, field):
                    raise ValueError(f'table {self._table} has no field {field}')

        self._select_fields = select_fields
        self._distinct = distinct
        return self

    def ignore(self, *ignore_fields) -> 'DictQuery':
        if self._entity is not None:
            for field in ignore_fields:
                if not hasattr(self._entity, field):
                    raise ValueError(f'table {self._table} has no field {field}')

        self._ignore_fields = ignore_fields
        return self

    def eq(self, field: str, value: Any) -> 'DictQuery':
        self._where.eq(field, value)
        return self

    def ne(self, field: str, value: Any) -> 'DictQuery':
        self._where.ne(field, value)
        return self

    def gt(self, field: str, value: Any) -> 'DictQuery':
        self._where.gt(field, value)
        return self

    def ge(self, field: str, value: Any) -> 'DictQuery':
        self._where.ge(field, value)
        return self

    def lt(self, field: str, value: Any) -> 'DictQuery':
        self._where.lt(field, value)
        return self

    def le(self, field: str, value: Any) -> 'DictQuery':
        self._where.le(field, value)
        return self

    def in_(self, field: str, value: Any) -> 'DictQuery':
        self._where.in_(field, value)
        return self

    def l_like(self, field: str, value: Any) -> 'DictQuery':
        self._where.l_like(field, value)
        return self

    def r_like(self, field: str, value: Any) -> 'DictQuery':
        self._where.r_like(field, value)
        return self

    def like(self, field: str, value: Any) -> 'DictQuery':
        self._where.like(field, value)
        return self

    def or_(self, or_: Or) -> 'DictQuery':
        self._where.or_(or_)
        return self

    def desc(self, *order_by) -> 'DictQuery':
        self._order_by = [f'{field} desc' for field in order_by]
        return self

    def asc(self, *order_by) -> 'DictQuery':
        self._order_by = [f'{field} asc' for field in order_by]
        return self

    def limit(self, limit: int) -> 'DictQuery':
        self._limit = limit
        return self

    def offset(self, offset: int) -> 'DictQuery':
        self._offset = offset
        return self

    def one(self) -> dict[str, Any] | None:
        sql, args = self._build_select()
        conn = self._datasource.get_connection()
        return select_one(sql, args, conn)

    def list(self) -> list[dict[str, Any]]:
        sql, args = self._build_select()
        conn = self._datasource.get_connection()
        rows = select_many(sql, args, conn)
        if rows is None:
            return []
        return rows

    def page(self, page: int, page_size: int) -> tuple[List[dict[str, Any]], int]:
        self._limit = page_size
        self._offset = (page - 1) * page_size
        sql, args = self._build_select()
        conn = self._datasource.get_connection()
        count = self.count()
        if count == 0:
            return [], count
        rows = select_many(sql, args, conn)
        if rows is None:
            return [], count
        return rows, count

    def count(self) -> int:
        sql = f'SELECT COUNT(*) FROM {self._database}.{self._table}'
        args = ()
        tree = self._where.tree()
        if len(tree.conditions) > 0:
            exp, args = tree.parse()
            sql += ' WHERE ' + exp
        conn = self._datasource.get_connection()
        return select_one(sql, args, conn)['COUNT(*)']

    def _build_select(self) -> tuple[str, tuple[Any, ...]]:
        # select_fields exclude ignore_fields
        if not self._select_fields:
            self._select_fields = [field.name for field in fields(self._model)]

        select_fields = [field for field in self._select_fields if field not in self._ignore_fields]

        sql = f'SELECT {"DISTINCT " if self._distinct and self._select_fields else ""}{",".join(select_fields)} FROM {self._database}.{self._table}'
        args = ()
        tree = self._where.tree()
        if len(tree.conditions) > 0:
            exp, args = tree.parse()
            sql += ' WHERE ' + exp
        if self._order_by is not None:
            sql += f' ORDER BY {",".join(self._order_by)}'
        if self._limit is not None:
            sql += f' LIMIT {self._limit}'
        if self._offset is not None:
            sql += f' OFFSET {self._offset}'

        return sql, args


class Query(DictQuery):
    def __init__(self, table: str | IEntity, datasource: IDataSource, database: str):
        super().__init__(table, datasource, database)

    def eq(self, field: str, value: Any) -> 'Query':
        self._where.eq(field, value)
        return self

    def ne(self, field: str, value: Any) -> 'Query':
        self._where.ne(field, value)
        return self

    def gt(self, field: str, value: Any) -> 'Query':
        self._where.gt(field, value)
        return self

    def ge(self, field: str, value: Any) -> 'Query':
        self._where.ge(field, value)
        return self

    def lt(self, field: str, value: Any) -> 'Query':
        self._where.lt(field, value)
        return self

    def le(self, field: str, value: Any) -> 'Query':
        self._where.le(field, value)
        return self

    def in_(self, field: str, value: Any) -> 'Query':
        self._where.in_(field, value)
        return self

    def l_like(self, field: str, value: Any) -> 'Query':
        self._where.l_like(field, value)
        return self

    def r_like(self, field: str, value: Any) -> 'Query':
        self._where.r_like(field, value)
        return self

    def like(self, field: str, value: Any) -> 'Query':
        self._where.like(field, value)
        return self

    def or_(self, or_: Or) -> 'Query':
        self._where.or_(or_)
        return self

    def desc(self, *order_by) -> 'Query':
        self._order_by = [f'{field} desc' for field in order_by]
        return self

    def asc(self, *order_by) -> 'Query':
        self._order_by = [f'{field} asc' for field in order_by]
        return self

    def limit(self, limit: int) -> 'Query':
        self._limit = limit
        return self

    def offset(self, offset: int) -> 'Query':
        self._offset = offset
        return self

    def one(self) -> object | None:
        row = super().one()
        if row is None:
            return None
        return self._model(**row)

    def list(self) -> List:
        rows = super().list()
        if len(rows) == 0:
            return rows
        return [self._model(**row) for row in rows]

    def page(self, page: int, page_size: int) -> tuple[List, int]:
        rows, count = super().page(page, page_size)
        if len(rows) == 0:
            return rows, count
        return [self._model(**row) for row in rows], count


def query(table: str | IEntity, database: str | None = None, data_source: IDataSource | None = None):
    return Query(table, data_source or dorm.default_datasource(), database or dorm.default_datasource().get_default_database())


def dict_query(table: str | IEntity, database: str | None = None, data_source: IDataSource | None = None):
    return DictQuery(table, data_source or dorm.default_datasource(), database or dorm.default_datasource().get_default_database())
