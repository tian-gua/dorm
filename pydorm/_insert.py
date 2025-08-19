from typing import Any, Dict, List, Literal, Tuple, TypeVar

from ._insert_wrapper import InsertWrapper
from ._middlewares import before_insert_middlewares
from .mysql._mysql_data_source import MysqlDataSource
from .mysql._reusable_mysql_connection import ReusableMysqlConnection
from .utils.random_utils import generate_random_string

T = TypeVar("T", bound=Any)


def insert(
    wrapper: InsertWrapper[T],
    data: Dict[str, Any],
    duplicate_key_update: List[str] | Literal["all"] | None = None,
    conn: ReusableMysqlConnection | None = None,
    data_source: MysqlDataSource | None = None,
) -> Tuple[int, int]:

    if data_source is None:
        raise ValueError("data_source must be provided")

    operation_id = generate_random_string("D-", 10)

    for middleware in before_insert_middlewares:
        if callable(middleware):
            middleware(data)

    sql, args = wrapper.build_insert_sql(data, duplicate_key_update)
    if conn is None:
        new_conn = data_source.get_reusable_connection()
        try:
            new_conn.acquire(operation_id=operation_id)
            new_conn.begin()
            row_affected, last_row_id = data_source.get_executor().execute(new_conn, sql, args)
            new_conn.commit()
            return row_affected, last_row_id
        finally:
            new_conn.release(operation_id=operation_id)
    return data_source.get_executor().execute(conn, sql, args)


def insert_bulk(
    wrapper: InsertWrapper[T],
    data: List[Dict[str, Any]],
    duplicate_key_update: List[str] | Literal["all"] | None = None,
    conn: ReusableMysqlConnection | None = None,
    data_source: MysqlDataSource | None = None,
) -> int:

    if data_source is None:
        raise ValueError("data_source must be provided")

    operation_id = generate_random_string("D-", 10)

    for middleware in before_insert_middlewares:
        if callable(middleware):
            middleware(data)

    sql, args = wrapper.build_insert_bulk_sql(data, duplicate_key_update)
    if conn is None:
        new_conn = data_source.get_reusable_connection()
        try:
            new_conn.acquire(operation_id=operation_id)
            new_conn.begin()
            row_affected = data_source.get_executor().executemany(new_conn, sql, args)
            new_conn.commit()
            return row_affected
        finally:
            new_conn.release(operation_id=operation_id)
    return data_source.get_executor().executemany(conn, sql, args)
