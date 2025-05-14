from typing import Any

from loguru import logger
from pymysql.connections import Connection
from pymysql.cursors import DictCursor


def debug(conn, sql, args):
    logger.debug(f'[{id(conn)}] {sql}')
    if args is not None and len(args) > 0:
        logger.debug(f'### {args}')


def select_one(sql: str, args: tuple[Any, ...], conn: Connection, is_default_transaction=True) -> dict[str, Any] | None:
    debug(conn, sql, args)

    if is_default_transaction:
        conn.begin()
    cursor: DictCursor = conn.cursor()
    try:
        sql = sql.replace('?', '%s')
        result = cursor.execute(sql, args)
        if result is None:
            return None

        row = cursor.fetchone()
        if is_default_transaction:
            conn.commit()
        return row
    except Exception as e:
        if is_default_transaction:
            conn.rollback()
        raise e
    finally:
        cursor.close()


def select_many(sql: str, args: tuple[Any, ...], conn: Connection, is_default_transaction=True) -> list[dict[str, Any]] | None:
    debug(conn, sql, args)

    if is_default_transaction:
        conn.begin()
    cursor: DictCursor = conn.cursor()
    try:
        sql = sql.replace('?', '%s')
        result = cursor.execute(sql, args)
        if result is None:
            return None

        rows = cursor.fetchall()
        if rows is None:
            return []

        row_list = list(rows)
        if is_default_transaction:
            conn.commit()
        return row_list
    except Exception as e:
        if is_default_transaction:
            conn.rollback()
        raise e
    finally:
        cursor.close()


def execute(sql: str, args: tuple[Any, ...], conn: Connection, is_insert=False, is_default_transaction=True) -> (int, int):
    debug(conn, sql, args)

    if is_default_transaction:
        conn.begin()
    cursor: DictCursor = conn.cursor()
    try:
        sql = sql.replace('?', '%s')
        row_affected = cursor.execute(sql, args)
        last_row_id = 0
        if is_insert:
            last_row_id = cursor.lastrowid
        if is_default_transaction:
            conn.commit()
        return row_affected, last_row_id
    except Exception as e:
        if is_default_transaction:
            conn.rollback()
        raise e
    finally:
        cursor.close()


def executemany(sql: str, args: list[tuple[any, ...]], conn: Connection, is_default_transaction=True) -> int | None:
    debug(conn, sql, args)

    if is_default_transaction:
        conn.begin()
    cursor = conn.cursor()
    try:
        sql = sql.replace('?', '%s')
        row_affected = cursor.executemany(sql, args)
        if is_default_transaction:
            conn.commit()
        return row_affected
    except Exception as e:
        if is_default_transaction:
            conn.rollback()
        raise e
    finally:
        cursor.close()
