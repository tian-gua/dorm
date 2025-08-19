from typing import List, Dict

from ._reusable_mysql_connection import ReusableMysqlConnection


class MysqlTableInspector:

    # noinspection PyMethodMayBeStatic
    def load_structure(
        self, conn: ReusableMysqlConnection, database: str, table: str
    ) -> List[Dict[str, str]]:
        need_acquire = not conn.is_locked()
        if need_acquire:
            conn.acquire()
        try:
            cursor = conn.cursor()
            try:
                sql = f"show full columns from {database}.{table}"
                cursor.execute(sql)
                rows = cursor.fetchall()
                table_fields: List[Dict[str, str]] = []
                for row in rows:
                    table_field = dict(
                        field_=row["Field"],
                        type_=row["Type"],
                        null_=row["Null"],
                        key_=row["Key"],
                        default_=row["Default"],
                        extra=row["Extra"],
                        comment=row.get("Comment", ""),
                    )
                    table_fields.append(table_field)
                return table_fields
            finally:
                cursor.close()
        finally:
            if need_acquire:
                conn.release()


mysql_table_inspector = MysqlTableInspector()
