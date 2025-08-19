import threading
from typing import Callable

from loguru import logger
from pymysql import MySQLError
from pymysql.connections import Connection
from pymysql.cursors import DictCursor
from ..errors import ConnectionException

from .. import settings


class ReusableMysqlConnection:
    def __init__(self, data_source_id: str, create_connection: Callable[[], Connection]):
        self._data_source_id = data_source_id
        self._lock = threading.Lock()
        self._create_connection = create_connection
        self._conn: Connection | None = None
        self._active = False
        self._closed = False
        self._in_use = False  # 添加使用中标记

        self._stop_event = threading.Event()  # 添加停止事件
        # 其他初始化代码
        self._keep_alive_thread = threading.Thread(target=self._keep_alive_worker, daemon=True)
        self._keep_alive_thread.start()

    def is_locked(self) -> bool:
        """
        检查锁是否已经被占用。

        Returns:
            bool: 如果锁被占用，返回 True；否则返回 False。
        """
        return self._lock.locked()

    def acquire(self, timeout: float = 5, operation_id: str | None = None):
        if settings.enable_connection_lock_log:
            logger.debug(
                f"[{operation_id}] try to lock connection[{id(self._conn)}] with timeout {timeout} seconds."
            )
        if self._lock.acquire(timeout=timeout):
            try:
                self._in_use = True
                if not self._active:
                    self._conn = self._create_connection()
                    self._active = True
                    logger.info(f"[{self._data_source_id}] Connection created.")
                if settings.enable_connection_lock_log:
                    logger.debug(f"'[{operation_id}] Connection[{id(self._conn)}] locked.")
            except Exception as e:
                self._in_use = False  # 添加这行
                self._lock.release()  # 添加这行
                raise ConnectionException(f"Failed to create connection: {e}")
        else:
            raise ConnectionException(f"Failed to acquire connection within {timeout} seconds.")

    def release(self, operation_id: str | None = None):
        self._in_use = False  # 取消使用中标记
        self._lock.release()
        if settings.enable_connection_lock_log:
            logger.debug(f"[{operation_id}] Connection[{id(self._conn)}] released.")

    def _check_connection(self):
        if not self._in_use:
            raise ConnectionException(
                f"[{self._data_source_id}] Connection must be acquired before use."
            )
        if self._closed:
            raise ConnectionException(f"[{self._data_source_id}] Connection is closed.")
        if not self._active:
            raise ConnectionException(f"[{self._data_source_id}] Connection is not active.")

    def cursor(self) -> DictCursor:
        self._check_connection()
        try:
            if self._conn is None:
                raise ConnectionException(
                    f"[{self._data_source_id}] Connection is not initialized."
                )
            return self._conn.cursor()  # type: ignore
        except MySQLError as e:
            logger.error(
                f"[{self._data_source_id}] Connection[{id(self._conn)}] cursor creation failed: {e}"
            )
            try:
                self._conn = self._create_connection()
                logger.info(
                    f"[{self._data_source_id}] Connection[{id(self._conn)}] recreated after cursor failure."
                )
                return self._conn.cursor()  # type: ignore
            except Exception as recreate_error:
                logger.error(
                    f"[{self._data_source_id}] Failed to recreate connection: {recreate_error}"
                )
                self._active = False
                raise ConnectionException(f"Connection recreation failed: {recreate_error}")

    def begin(self):
        self._check_connection()
        try:
            if self._conn is None:
                raise ConnectionException(
                    f"[{self._data_source_id}] Connection is not initialized."
                )
            self._conn.begin()
        except MySQLError as e:
            logger.error(f"[{self._data_source_id}] Connection[{id(self._conn)}] begin failed: {e}")
            self._recreate_connection()
            raise ConnectionException(f"Transaction begin failed: {e}")

    def commit(self):
        self._check_connection()
        try:
            if self._conn is None:
                raise ConnectionException(
                    f"[{self._data_source_id}] Connection is not initialized."
                )
            self._conn.commit()
        except MySQLError as e:
            logger.error(
                f"[{self._data_source_id}] Connection[{id(self._conn)}] commit failed: {e}"
            )
            self._recreate_connection()
            raise ConnectionException(f"Transaction commit failed: {e}")

    def rollback(self):
        self._check_connection()
        try:
            self._conn.rollback()
        except MySQLError as e:
            logger.error(
                f"[{self._data_source_id}] Connection[{id(self._conn)}] rollback failed: {e}"
            )
            self._recreate_connection()
            raise ConnectionException(f"Transaction rollback failed: {e}")

    def _recreate_connection(self):
        try:
            old_conn_id = id(self._conn) if self._conn else None
            self._conn = self._create_connection()
            logger.info(
                f"[{self._data_source_id}] Connection[{old_conn_id}] -> [{id(self._conn)}] recreated."
            )
        except Exception as recreate_error:
            logger.error(
                f"[{self._data_source_id}] Failed to recreate connection: {recreate_error}"
            )
            self._active = False
            raise ConnectionException(f"Connection recreation failed: {recreate_error}")

    def close(self):
        if self._closed:
            return
        self._closed = True
        self._stop_event.set()  # 通知保活线程停止

        # 等待保活线程结束
        if hasattr(self, "_keep_alive_thread"):
            self._keep_alive_thread.join(timeout=5)

        with self._lock:
            try:
                if self._conn:
                    self._conn.close()
                    logger.info(f"[{self._data_source_id}] Connection[{id(self._conn)}] closed.")
            except MySQLError as e:
                logger.error(
                    f"[{self._data_source_id}] Connection[{id(self._conn)}] close failed: {e}"
                )
            finally:
                self._active = False

    def _keep_alive_worker(self):
        while not self._stop_event.wait(30):
            if self._closed:
                break
            if self._active and not self._in_use:  # 添加使用中检查
                if self._lock.acquire(timeout=1):  # 使用超时避免阻塞
                    try:
                        if self._conn and not self._in_use:  # 双重检查
                            self._conn.ping(reconnect=True)
                    except MySQLError as e:
                        logger.error(f"[{self._data_source_id}] ping failed: {e}")
                        self._recreate_connection()
                    finally:
                        self._lock.release()
