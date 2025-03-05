from typing import Any

from loguru import logger
from pymysql.connections import Connection
from pymysql.cursors import DictCursor


def debug(sql, args):
    logger.debug(f'#### sql: {sql}')
    logger.debug(f'#### args: {args}')


def select_one(sql: str, args: tuple[Any, ...], conn: Connection) -> dict[str, Any] | None:
    debug(sql, args)

    cursor: DictCursor = conn.cursor()
    try:
        sql = sql.replace('?', '%s')
        result = cursor.execute(sql, args)
        if result is None:
            return None

        row = cursor.fetchone()
        if row is None:
            return None

        return row
    except Exception as e:
        raise e
    finally:
        cursor.close()
        conn.close()


def select_many(sql: str, args: tuple[Any, ...], conn: Connection) -> tuple[dict[str, Any], ...] | None:
    debug(sql, args)

    cursor: DictCursor = conn.cursor()
    try:
        sql = sql.replace('?', '%s')
        result = cursor.execute(sql, args)
        if result is None:
            return None

        rows = cursor.fetchall()
        if rows is None:
            return None

        return rows
    except Exception as e:
        raise e
    finally:
        cursor.close()
        conn.close()


def execute(sql: str, args: tuple[Any, ...], conn: Connection, is_insert=False) -> (int, int):
    debug(sql, args)

    conn.begin()
    cursor: DictCursor = conn.cursor()
    try:
        sql = sql.replace('?', '%s')
        row_affected = cursor.execute(sql, args)
        last_row_id = 0
        if is_insert:
            last_row_id = cursor.lastrowid
        conn.commit()
        return row_affected, last_row_id
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cursor.close()
        conn.close()


def executemany(sql: str, args: list[tuple[any, ...]], conn) -> int:
    debug(sql, args)
    conn.begin()
    cursor = conn.cursor()
    try:
        sql = sql.replace('?', '%s')
        row_affected = cursor.executemany(sql, args)
        conn.commit()
        return row_affected
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cursor.close()
        conn.close()
