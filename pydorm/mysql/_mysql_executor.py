from contextlib import contextmanager
from typing import Any, Dict, List, Tuple

from loguru import logger

from ._reusable_mysql_connection import ReusableMysqlConnection


class MysqlExecutor:
    """MySQL数据库执行器，提供常用的数据库操作方法"""

    def __init__(self, log_sql: bool = True):
        """
        初始化MySQL执行器

        Args:
            log_sql: 是否记录SQL日志
        """
        self.log_sql = log_sql

    def _log_execution(self, conn: ReusableMysqlConnection, sql: str, args: Any) -> None:
        """记录SQL执行日志"""
        if not self.log_sql:
            return

        logger.debug(f"[{id(conn)}] {sql}")
        # if args is not None and len(args) > 0:
        #     logger.debug(f"### {args}")

    # noinspection PyMethodMayBeStatic
    def _prepare_sql(self, sql: str) -> str:
        """将占位符从?转换为%s"""
        return sql.replace("?", "%s")

    @contextmanager
    def _get_cursor(self, conn: ReusableMysqlConnection):
        """获取游标的上下文管理器"""
        cursor = conn.cursor()
        try:
            yield cursor
        finally:
            cursor.close()

    def select_one(
        self,
        conn: ReusableMysqlConnection,
        sql: str,
        args: Tuple[Any, ...] = (),
    ) -> Dict[str, Any] | None:
        """
        执行查询并返回单行结果

        Args:
            conn: 数据库连接
            sql: SQL语句
            args: 参数元组

        Returns:
            查询结果字典或None
        """
        self._log_execution(conn, sql, args)

        with self._get_cursor(conn) as cursor:
            prepared_sql = self._prepare_sql(sql)
            cursor.execute(prepared_sql, args)

            if cursor.rowcount == 0:
                return None
            return cursor.fetchone()

    def select_many(
        self,
        conn: ReusableMysqlConnection,
        sql: str,
        args: tuple[Any, ...] = (),
    ) -> list[dict[str, Any]]:
        """
        执行查询并返回多行结果

        Args:
            conn: 数据库连接
            sql: SQL语句
            args: 参数元组

        Returns:
            查询结果列表
        """
        self._log_execution(conn, sql, args)

        with self._get_cursor(conn) as cursor:
            prepared_sql = self._prepare_sql(sql)
            cursor.execute(prepared_sql, args)

            if cursor.rowcount == 0:
                return []

            rows = cursor.fetchall()
            return list(rows) if rows else []

    def execute(
        self,
        conn: ReusableMysqlConnection,
        sql: str,
        args: Tuple[Any, ...] = (),
    ) -> Tuple[int, int]:
        """
        执行SQL语句（INSERT、UPDATE、DELETE）

        Args:
            conn: 数据库连接
            sql: SQL语句
            args: 参数元组

        Returns:
            (受影响的行数, 最后插入的行ID)
        """
        self._log_execution(conn, sql, args)

        with self._get_cursor(conn) as cursor:
            prepared_sql = self._prepare_sql(sql)
            row_affected = cursor.execute(prepared_sql, args)
            last_row_id = cursor.lastrowid
            return row_affected, last_row_id

    def executemany(
        self,
        conn: ReusableMysqlConnection,
        sql: str,
        args: List[Tuple[Any, ...]],
    ) -> int:
        """
        批量执行SQL语句

        Args:
            conn: 数据库连接
            sql: SQL语句
            args: 参数列表

        Returns:
            受影响的总行数
        """
        self._log_execution(conn, sql, args)

        with self._get_cursor(conn) as cursor:
            prepared_sql = self._prepare_sql(sql)
            row_affected = cursor.executemany(prepared_sql, args)
            return row_affected or 0


mysql_executor = MysqlExecutor()
