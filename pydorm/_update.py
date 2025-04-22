from dataclasses import fields
from typing import Any, TypeVar, Type

from ._dorm import dorm
from ._models import models
from ._mysql_executor import execute
from ._where import Where, Or
from .protocols import IDataSource

T = TypeVar('T')


class Update:
    def __init__(self, table: str, datasource: IDataSource, database: str, cls: Type[T] | None):
        self._table: str = table

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

        self._cls = cls
        self._where = Where()
        self._update_fields: dict = {}

        self._model: callable = models.get(data_source=self._datasource, database=self._database, table=self._table)
        self._model_fields = [f.name for f in fields(self._model)]

    def check_field(self, field: str):
        if field not in self._model_fields:
            raise ValueError(f'invalid field [{field}]')

    def has_field(self, field: str):
        return field in self._model_fields

    def eq(self, field: str, value: Any) -> 'Update':
        self.check_field(field)
        self._where.eq(field, value)
        return self

    def ne(self, field: str, value: Any) -> 'Update':
        self.check_field(field)
        self._where.ne(field, value)
        return self

    def gt(self, field: str, value: Any) -> 'Update':
        self.check_field(field)
        self._where.gt(field, value)
        return self

    def ge(self, field: str, value: Any) -> 'Update':
        self.check_field(field)
        self._where.ge(field, value)
        return self

    def lt(self, field: str, value: Any) -> 'Update':
        self.check_field(field)
        self._where.lt(field, value)
        return self

    def le(self, field: str, value: Any) -> 'Update':
        self.check_field(field)
        self._where.le(field, value)
        return self

    def in_(self, field: str, value: Any) -> 'Update':
        self.check_field(field)
        self._where.in_(field, value)
        return self

    def l_like(self, field: str, value: Any) -> 'Update':
        self.check_field(field)
        self._where.l_like(field, value)
        return self

    def r_like(self, field: str, value: Any) -> 'Update':
        self.check_field(field)
        self._where.r_like(field, value)
        return self

    def like(self, field: str, value: Any) -> 'Update':
        self.check_field(field)
        self._where.like(field, value)
        return self

    def or_(self, or_: Or) -> 'Update':
        self._where.or_(or_)
        return self

    def set(self, set_args: dict = None, **args) -> 'Update':
        if set_args is None:
            set_args = {}

        set_args.update(args)

        valid_fields = {}
        for k, v in set_args.items():
            if self.has_field(k):
                valid_fields[k] = v

        if len(valid_fields) == 0:
            raise ValueError('valid fields is required')

        self._update_fields = valid_fields
        return self

    def update(self) -> int:
        if self._where.count() == 0:
            # danger operation
            raise ValueError('update all is not supported')

        if len(self._update_fields) == 0:
            raise ValueError('update fields is required')

        sql, args = self._build_update()
        conn = self._datasource.get_connection()
        affected, last_row_id = execute(sql, args, conn, False)
        return affected or 0

    def _build_update(self) -> tuple[str, tuple[Any, ...]]:
        sql = f'UPDATE {self._database}.{self._table} SET {",".join([f"{k}=?" for k in self._update_fields.keys()])}'
        args = tuple(self._update_fields.values())

        exp, args2 = self._where.tree().parse()
        sql += ' WHERE ' + exp
        args += args2
        return sql, args

    def delete(self) -> int:
        if self._where.count() == 0:
            # danger operation
            raise ValueError('delete all is not supported')

        sql, args = self._build_delete()
        conn = self._datasource.get_connection()
        affected, last_row_id = execute(sql, args, conn, False)
        return affected or 0

    def _build_delete(self) -> tuple[str, tuple[Any, ...]]:
        sql = f'DELETE FROM {self._database}.{self._table}'

        exp, args = self._where.tree().parse()
        sql += ' WHERE ' + exp
        return sql, args


def update(table_or_cls: str | Type[T], database: str | None = None, data_source: IDataSource | None = None) -> Update:
    if isinstance(table_or_cls, str):
        table = table_or_cls
        cls = None
    else:
        table = table_or_cls.__table_name__
        cls = table_or_cls
    return Update(table, data_source or dorm.default_datasource(), database or dorm.default_datasource().get_default_database(), cls=cls)
