import uuid
from contextvars import ContextVar

from loguru import logger
from pymysql.connections import Connection

from ._datasources import default_datasource, get_datasource
from .protocols import IDataSource


class Tx:
    def __init__(self, ds_id: str | None = None, auto_commit: bool = True):
        self._auto_commit = auto_commit
        self._ds_id = ds_id
        self._datasource: IDataSource = (
            default_datasource() if ds_id is None else get_datasource(ds_id)
        )
        self._conn: Connection | None = None
        self._state = "new"
        self._tx_id = uuid.uuid4().hex
        logger.debug(
            f"Transaction [{self._tx_id}] created with auto_commit={auto_commit}"
        )

    def begin(self):
        if self._state == "new":
            if self._conn is not None:
                raise RuntimeError("Transaction already started")
            self._conn = self._datasource.create_connection()
            self._state = "begin"
        elif self._state == "begin" and not self._auto_commit:
            logger.debug(f"reuse existing transaction [{self._tx_id}]")
        elif self._state == "committed" or self._state == "rolled_back":
            raise RuntimeError("Transaction already completed")
        return self._conn

    def commit(self):
        if self._state != "begin":
            raise RuntimeError("Transaction not in a valid state to commit")
        if self._conn is None:
            raise RuntimeError("No connection to commit")
        self._conn.commit()
        logger.debug(f"Transaction [{self._tx_id}] committed")
        self._state = "committed"
        self._conn.close()

    def rollback(self):
        if self._state != "begin":
            raise RuntimeError("Transaction not in a valid state to rollback")
        if self._conn is None:
            raise RuntimeError("No connection to rollback")
        self._conn.rollback()
        logger.debug(f"Transaction [{self._tx_id}] rolled back")
        self._state = "rolled_back"
        self._conn.close()

    def is_auto_commit(self) -> bool:
        return self._auto_commit

    def is_valid(self) -> bool:
        return self._state == "new" or self._state == "begin"

    def ds_id(self) -> str:
        return self._ds_id


tx_context = ContextVar("sql_context", default=None)
