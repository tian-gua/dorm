from typing import Any

from ._dorm import dorm
from ._models import models
from ._mysql_executor import execute
from ._where import Where, Or
from .protocols import IEntity, IDataSource


class Update:
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
        self._update_fields = {}

        self._model: callable = models.get(data_source=self._datasource, database=self._database, table=self._table)

    def eq(self, field: str, value: Any) -> 'Update':
        self._where.eq(field, value)
        return self

    def ne(self, field: str, value: Any) -> 'Update':
        self._where.ne(field, value)
        return self

    def gt(self, field: str, value: Any) -> 'Update':
        self._where.gt(field, value)
        return self

    def ge(self, field: str, value: Any) -> 'Update':
        self._where.ge(field, value)
        return self

    def lt(self, field: str, value: Any) -> 'Update':
        self._where.lt(field, value)
        return self

    def le(self, field: str, value: Any) -> 'Update':
        self._where.le(field, value)
        return self

    def in_(self, field: str, value: Any) -> 'Update':
        self._where.in_(field, value)
        return self

    def l_like(self, field: str, value: Any) -> 'Update':
        self._where.l_like(field, value)
        return self

    def r_like(self, field: str, value: Any) -> 'Update':
        self._where.r_like(field, value)
        return self

    def like(self, field: str, value: Any) -> 'Update':
        self._where.like(field, value)
        return self

    def or_(self, or_: Or) -> 'Update':
        self._where.or_(or_)
        return self

    def set(self, **sets) -> 'Update':
        for k, v in sets.items():
            if self._entity is not None and not hasattr(self._entity, k):
                raise ValueError(f'entity {self._entity} has no field {k}')
        self._update_fields: dict = sets
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


def update(table: str | IEntity, database: str | None = None, data_source: IDataSource | None = None) -> Update:
    return Update(table, data_source or dorm.default_datasource(), database or dorm.default_datasource().get_default_database())
