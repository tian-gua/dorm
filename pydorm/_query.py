from typing import Any, Dict, List, Tuple, TypeVar

from ._middlewares import before_query_middlewares
from ._query_wrapper import QueryWrapper
from .mysql._mysql_data_source import MysqlDataSource
from .mysql._reusable_mysql_connection import ReusableMysqlConnection
from .utils.random_utils import generate_random_string

T = TypeVar("T", bound=Any)


def find(
    wrapper: QueryWrapper[T],
    conn: ReusableMysqlConnection | None = None,
    data_source: MysqlDataSource | None = None,
) -> T | None:
    result = find_dict(wrapper, conn, data_source)
    if result is None:
        return None
    return wrapper.get_type()(**result)


def find_dict(
    wrapper: QueryWrapper[T],
    conn: ReusableMysqlConnection | None = None,
    data_source: MysqlDataSource | None = None,
) -> Dict[str, Any] | None:

    if data_source is None:
        raise ValueError("data_source must be provided")

    operation_id = generate_random_string("R-", 10)

    for middleware in before_query_middlewares:
        if callable(middleware):
            middleware(wrapper)

    sql, args = wrapper.build_sql()

    if conn is None:
        new_conn = data_source.get_reusable_connection()
        try:
            new_conn.acquire(operation_id=operation_id)
            new_conn.begin()
            result: Dict[str, Any] | None = data_source.get_executor().select_one(
                new_conn, sql, args
            )
            new_conn.commit()
            return result
        finally:
            new_conn.release(operation_id=operation_id)
    else:
        return data_source.get_executor().select_one(conn, sql, args)


def list(
    wrapper: QueryWrapper[T],
    conn: ReusableMysqlConnection | None = None,
    data_source: MysqlDataSource | None = None,
) -> List[T]:
    result = list_dict(wrapper, conn, data_source)
    if result is None:
        return []
    return [wrapper.get_type()(**item) for item in result]


def list_dict(
    wrapper: QueryWrapper[T],
    conn: ReusableMysqlConnection | None = None,
    data_source: MysqlDataSource | None = None,
) -> List[Dict[str, Any]]:
    if data_source is None:
        raise ValueError("data_source must be provided")

    operation_id = generate_random_string("R-", 10)

    for middleware in before_query_middlewares:
        if callable(middleware):
            middleware(wrapper)

    sql, args = wrapper.build_sql()
    if conn is None:
        new_conn = data_source.get_reusable_connection()
        try:
            new_conn.acquire(operation_id=operation_id)
            new_conn.begin()
            result: List[Dict[str, Any]] | None = data_source.get_executor().select_many(
                new_conn, sql, args
            )
            new_conn.commit()
            return result
        finally:
            new_conn.release(operation_id=operation_id)
    else:
        return data_source.get_executor().select_many(conn, sql, args)


def count(
    wrapper: QueryWrapper[T],
    conn: ReusableMysqlConnection | None = None,
    data_source: MysqlDataSource | None = None,
    load_middlewares: bool = True,
) -> int:
    if data_source is None:
        raise ValueError("data_source must be provided")

    operation_id = generate_random_string("R-", 10)

    if load_middlewares:
        for middleware in before_query_middlewares:
            if callable(middleware):
                middleware(wrapper)

    sql, args = wrapper.build_count_sql()

    if conn is None:
        new_conn = data_source.get_reusable_connection()
        try:
            new_conn.acquire(operation_id=operation_id)
            new_conn.begin()
            result = data_source.get_executor().select_one(new_conn, sql, args)
            new_conn.commit()

            if result is None:
                return 0
            return result["COUNT(*)"]
        finally:
            new_conn.release(operation_id=operation_id)
    else:
        result = data_source.get_executor().select_one(conn, sql, args)
        if result is None:
            return 0
        return result["COUNT(*)"]


def page(
    wrapper: QueryWrapper[T],
    conn: ReusableMysqlConnection | None = None,
    data_source: MysqlDataSource | None = None,
    current: int = 1,
    page_size: int = 10,
) -> Tuple[List[T], int]:
    if data_source is None:
        raise ValueError("data_source must be provided")

    rows, total = page_dict(wrapper, conn, data_source, current, page_size)
    if rows is None:
        return [], 0
    return [wrapper.get_type()(**row) for row in rows], total


def page_dict(
    wrapper: QueryWrapper[T],
    conn: ReusableMysqlConnection | None = None,
    data_source: MysqlDataSource | None = None,
    current: int = 1,
    page_size: int = 10,
) -> Tuple[List[Dict[str, Any]], int]:
    if data_source is None:
        raise ValueError("data_source must be provided")

    operation_id = generate_random_string("R-", 10)

    for middleware in before_query_middlewares:
        if callable(middleware):
            middleware(wrapper)

    wrapper.limit(page_size).offset((current - 1) * page_size)
    sql, args = wrapper.build_sql()

    if conn is None:
        new_conn = data_source.get_reusable_connection()
        total = count(wrapper, new_conn, data_source, load_middlewares=False)
        if total == 0:
            return [], total
        try:
            new_conn.acquire(operation_id=operation_id)
            new_conn.begin()
            rows = data_source.get_executor().select_many(new_conn, sql, args)
            new_conn.commit()
            return rows, total
        finally:
            new_conn.release(operation_id=operation_id)

    total = count(wrapper, conn, data_source, load_middlewares=False)
    if total == 0:
        return [], total
    rows = data_source.get_executor().select_many(conn, sql, args)
    return rows, total
