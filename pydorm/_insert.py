from dataclasses import fields
from typing import Any, TypeVar, Type

from ._dorm import dorm
from ._models import models
from ._mysql_executor import execute, executemany
from .protocols import IDataSource

T = TypeVar('T')


class Insert:
    def __init__(self, table: str, datasource: IDataSource, database: str):
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
        model_fields = [f.name for f in fields(self._model)]
        for k, v in data[0].items():
            if k in model_fields:
                keys.append(k)
                placeholder.append('?')
        sql = f'INSERT INTO {self._database}.{self._table} ({",".join(keys)}) VALUES ({",".join(placeholder)})'
        args = [tuple(datum[k] for k in keys) for datum in data]
        return sql, args

    def upsert(self, data, update_fields: list[str] | None = None) -> (int, int):
        if data is None:
            raise ValueError('null data')

        sql, args = self._build_upsert(data, update_fields)
        conn = self._datasource.get_connection()
        return execute(sql, args, conn, True)

    def _build_upsert(self, data: dict, update_fields: list[str] | None = None) -> tuple[str, tuple[Any, ...]]:
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

        # check if update_fields is a sub list of keys
        if update_fields is None:
            update_fields = keys
        else:
            if len(update_fields) == 0 or not all(field in keys for field in update_fields):
                raise ValueError('update fields are not valid')

        sql = f'INSERT INTO `{self._database}`.`{self._table}` ({",".join(keys)}) VALUES ({",".join(placeholder)})'
        if update_fields:
            sql += f' ON DUPLICATE KEY UPDATE {",".join([f"{k}=VALUES({k})" for k in update_fields])}'
        else:
            sql += f' ON DUPLICATE KEY UPDATE {",".join([f"{k}=VALUES({k})" for k in keys])}'

        args = tuple(values)
        return sql, args

    def upsert_bulk(self, data: list[dict], update_fields: list[str] | None = None) -> int:
        if data is None or len(data) == 0:
            raise ValueError('data is required')

        sql, args = self._build_upsert_bulk(data, update_fields)
        conn = self._datasource.get_connection()
        return executemany(sql, args, conn)

    def _build_upsert_bulk(self, data: list[dict], update_fields: list[str] | None = None) -> tuple[str, list[tuple[any, ...]]]:
        placeholder = []
        keys = []
        model_fields = [f.name for f in fields(self._model)]

        for k in data[0].keys():
            if k in model_fields:
                keys.append(k)
                placeholder.append('%s')

        if len(keys) == 0:
            raise ValueError('no valid field found')

        # check if update_fields is a sub list of keys
        if update_fields is None:
            update_fields = keys
        else:
            if len(update_fields) == 0 or not all(field in keys for field in update_fields):
                raise ValueError('update fields are not valid')

        sql = f'INSERT INTO `{self._database}`.`{self._table}` ({",".join(keys)}) VALUES ({",".join(placeholder)})'
        if update_fields:
            sql += f' ON DUPLICATE KEY UPDATE {",".join([f"{k}=VALUES({k})" for k in update_fields])}'
        else:
            sql += f' ON DUPLICATE KEY UPDATE {",".join([f"{k}=VALUES({k})" for k in keys])}'

        args = [tuple(datum[k] for k in keys) for datum in data]
        return sql, args


def insert(table_or_cls: str | Type[T], data: dict = None, database: str | None = None, data_source: IDataSource | None = None) -> (int, int):
    if isinstance(table_or_cls, str):
        table = table_or_cls
    else:
        table = table_or_cls.__table_name__
    return Insert(table, data_source or dorm.default_datasource(), database or dorm.default_datasource().get_default_database()).insert(data)


def upsert(table_or_cls: str | Type[T], data: dict = None, update_fields: list = None, database: str | None = None,
           data_source: IDataSource | None = None) -> (int, int):
    if isinstance(table_or_cls, str):
        table = table_or_cls
    else:
        table = table_or_cls.__table_name__
    return (Insert(table, data_source or dorm.default_datasource(), database or dorm.default_datasource().get_default_database())
            .upsert(data, update_fields))


def insert_bulk(table_or_cls: str | Type[T], data: list[dict] = None, database: str | None = None, data_source: IDataSource | None = None):
    if isinstance(table_or_cls, str):
        table = table_or_cls
    else:
        table = table_or_cls.__table_name__
    return Insert(table, data_source or dorm.default_datasource(), database or dorm.default_datasource().get_default_database()).insert_bulk(data)


def upsert_bulk(table_or_cls: str | Type[T], data: list[dict] = None, update_fields: list = None, database: str | None = None,
                data_source: IDataSource | None = None):
    if isinstance(table_or_cls, str):
        table = table_or_cls
    else:
        table = table_or_cls.__table_name__
    return (Insert(table, data_source or dorm.default_datasource(), database or dorm.default_datasource().get_default_database())
            .upsert_bulk(data, update_fields))
