from typing import Any

from loguru import logger
from pymysql.cursors import DictCursor

from ._tx import Tx
from ._tx import tx_context


def debug(conn, sql, args):
    logger.debug(f"[{id(conn)}] {sql}")
    if args is not None and len(args) > 0:
        logger.debug(f"### {args}")


def select_one(ds_id: str, sql: str, args: tuple[Any, ...]) -> dict[str, Any] | None:
    tx = _get_tx(ds_id)
    conn = tx.begin()
    cursor: DictCursor | None = None
    debug(conn, sql, args)
    try:
        conn.begin()
        cursor = conn.cursor()
        sql = sql.replace("?", "%s")
        result = cursor.execute(sql, args)
        if result is None:
            return None

        row = cursor.fetchone()
        if tx.is_auto_commit():
            conn.commit()
        return row
    except Exception as e:
        if tx.is_auto_commit():
            conn.rollback()
        raise e
    finally:
        if cursor is not None:
            cursor.close()
        if tx.is_auto_commit():
            conn.close()


def select_many(
    ds_id: str, sql: str, args: tuple[Any, ...]
) -> list[dict[str, Any]] | None:
    tx = _get_tx(ds_id)
    conn = tx.begin()
    cursor: DictCursor | None = None
    debug(conn, sql, args)
    try:
        conn.begin()
        cursor = conn.cursor()
        sql = sql.replace("?", "%s")
        result = cursor.execute(sql, args)
        if result is None:
            return None

        rows = cursor.fetchall()
        if rows is None:
            return []

        row_list = list(rows)
        if tx.is_auto_commit():
            conn.commit()
        return row_list
    except Exception as e:
        if tx.is_auto_commit():
            conn.rollback()
        raise e
    finally:
        if cursor is not None:
            cursor.close()
        if tx.is_auto_commit():
            conn.close()


def execute(ds_id: str, sql: str, args: tuple[Any, ...]) -> (int, int):
    tx = _get_tx(ds_id)
    conn = tx.begin()
    cursor: DictCursor | None = None
    debug(conn, sql, args)

    try:
        conn.begin()
        cursor = conn.cursor()
        sql = sql.replace("?", "%s")
        row_affected = cursor.execute(sql, args)
        last_row_id = cursor.lastrowid
        if tx.is_auto_commit():
            conn.commit()
        return row_affected, last_row_id
    except Exception as e:
        if tx.is_auto_commit():
            conn.rollback()
        raise e
    finally:
        if cursor is not None:
            cursor.close()
        if tx.is_auto_commit():
            conn.close()


def executemany(ds_id: str, sql: str, args: list[tuple[any, ...]]) -> int | None:
    tx = _get_tx(ds_id)
    conn = tx.begin()
    cursor: DictCursor | None = None
    debug(conn, sql, args)

    try:
        conn.begin()
        cursor = conn.cursor()
        sql = sql.replace("?", "%s")
        row_affected = cursor.executemany(sql, args)
        if tx.is_auto_commit():
            conn.commit()
        return row_affected
    except Exception as e:
        if tx.is_auto_commit():
            conn.rollback()
        raise e
    finally:
        if cursor is not None:
            cursor.close()
        if tx.is_auto_commit():
            conn.close()


def _get_tx(ds_id: str) -> Tx:
    tx = tx_context.get()
    if tx is None or not tx.is_valid() or ds_id != tx.ds_id():
        tx = Tx(ds_id, auto_commit=True)
    return tx
