from dataclasses import fields
from typing import Any, TypeVar, Type

from ._context import dorm_context
from ._datasources import default_datasource
from ._middlewares import get_middlewares
from ._models import models
from ._mysql_executor import execute, executemany
from .enums import Middleware
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

        self._apply_before_insert_middlewares(data)

        sql, args = self._build_insert(data)
        conn = dorm_context.get().tx()
        if conn is None:
            conn = self._datasource.get_connection()
            return execute(sql, args, conn, True)
        else:
            return execute(sql, args, conn, True, False)

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
        conn = dorm_context.get().tx()
        if conn is None:
            conn = self._datasource.get_connection()
            return executemany(sql, args, conn)
        else:
            return executemany(sql, args, conn, False)

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
        conn = dorm_context.get().tx()
        if conn is None:
            conn = self._datasource.get_connection()
            return execute(sql, args, conn, True)
        else:
            return execute(sql, args, conn, True, False)

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
        conn = dorm_context.get().tx()
        if conn is None:
            conn = self._datasource.get_connection()
            return executemany(sql, args, conn)
        else:
            return executemany(sql, args, conn, False)

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

    def _apply_before_insert_middlewares(self, data: dict):
        middlewares = get_middlewares(Middleware.BEFORE_INSERT)
        if middlewares is None or len(middlewares) == 0:
            return

        for middleware in middlewares:
            middleware(self, data)


def insert(table_or_cls: str | Type[T], data: dict | T = None, database: str | None = None, data_source: IDataSource | None = None) -> (int, int):
    if isinstance(table_or_cls, str):
        table = table_or_cls
    else:
        table = table_or_cls.__table_name__

    # 判断 data 是否为dict，如果不是，则转换成dict
    if data is not None and not isinstance(data, dict):
        data = data.__dict__ if hasattr(data, '__dict__') else data.__dict__
    # 移除 value 为 None 的字段
    data = {k: v for k, v in data.items() if v is not None}

    return Insert(table, data_source or default_datasource(), database or default_datasource().get_default_database()).insert(data)


def upsert(table_or_cls: str | Type[T], data: dict = None, update_fields: list = None, database: str | None = None,
           data_source: IDataSource | None = None) -> (int, int):
    if isinstance(table_or_cls, str):
        table = table_or_cls
    else:
        table = table_or_cls.__table_name__
    return (Insert(table, data_source or default_datasource(), database or default_datasource().get_default_database())
            .upsert(data, update_fields))


def insert_bulk(table_or_cls: str | Type[T], data: list[dict] = None, database: str | None = None, data_source: IDataSource | None = None):
    if isinstance(table_or_cls, str):
        table = table_or_cls
    else:
        table = table_or_cls.__table_name__
    return Insert(table, data_source or default_datasource(), database or default_datasource().get_default_database()).insert_bulk(data)


def upsert_bulk(table_or_cls: str | Type[T], data: list[dict] = None, update_fields: list = None, database: str | None = None,
                data_source: IDataSource | None = None):
    if isinstance(table_or_cls, str):
        table = table_or_cls
    else:
        table = table_or_cls.__table_name__
    return (Insert(table, data_source or default_datasource(), database or default_datasource().get_default_database())
            .upsert_bulk(data, update_fields))
