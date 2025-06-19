from pymysql.cursors import DictCursor

from ._datasources import get_datasource, default_datasource


def raw_select(sql: str, args: tuple[any, ...], ds_id: str | None = None):
    ds = default_datasource() if ds_id is None else get_datasource(ds_id)
    conn = ds.create_connection()
    try:
        cursor: DictCursor = conn.cursor()
        sql = sql.replace("?", "%s")
        result = cursor.execute(sql, args)
        if result is None:
            return None
        rows = cursor.fetchall()
        if rows is None:
            return []

        return list(rows)
    finally:
        conn.close()
