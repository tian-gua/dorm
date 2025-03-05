from dataclasses import fields
from typing import Any

from ._dorm import dorm
from ._models import models
from ._mysql_executor import execute, executemany
from .protocols import IEntity, IDataSource


class Insert:
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

        self._model: callable = models.get(data_source=self._datasource, database=self._database, table=self._table)

    def insert(self, data: dict) -> (int, int):
        if data is None:
            raise ValueError('null data')

        sql, args = self._build_insert(data)
        conn = self._datasource.get_connection()
        return execute(sql, args, conn, True)

    def _build_insert(self, data: dict) -> tuple[str, tuple[Any, ...]]:
        placeholder = []
        keys = []
        values = []
        model_fields = [f.name for f in fields(self._model)]
        for k, v in data.items():
            if k in model_fields:
                keys.append(k)
                values.append(v)
                placeholder.append('?')

        if len(keys) == 0:
            raise ValueError('no valid field found')

        sql = f'INSERT INTO {self._database}.{self._table} ({",".join(keys)}) VALUES ({",".join(placeholder)})'
        args = tuple(values)
        return sql, args

    def insert_bulk(self, data: list[dict]) -> int:
        if data is None or len(data) == 0:
            raise ValueError('data is required')

        sql, args = self._build_insert_bulk(data)
        conn = self._datasource.get_connection()
        return executemany(sql, args, conn)

    def _build_insert_bulk(self, data: list[dict]) -> tuple[str, list[tuple[any, ...]]]:
        placeholder = []
        keys = []
        values = []
        model_fields = [f.name for f in fields(self._model)]
        for k, v in data[0].items():
            if k in model_fields:
                keys.append(k)
                values.append(v)
                placeholder.append('?')
        sql = f'INSERT INTO {self._database}.{self._table} ({",".join(keys)}) VALUES ({",".join(placeholder)})'
        args = [tuple(datum.values()) for datum in data]
        return sql, args


def insert(table: str | IEntity, data: dict = None, database: str | None = None, data_source: IDataSource | None = None) -> (int, int):
    return Insert(table, data_source or dorm.default_datasource(), database or dorm.default_datasource().get_default_database()).insert(data)


def insert_bulk(table: str | IEntity, data: list[dict] = None, database: str | None = None, data_source: IDataSource | None = None):
    return Insert(table, data_source or dorm.default_datasource(), database or dorm.default_datasource().get_default_database()).insert_bulk(data)
