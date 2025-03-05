import uuid
from contextvars import ContextVar

from loguru import logger
from pymysql.connections import Connection as MysqlConnection

from .protocols import IDataSource


class DormContext:
    def __init__(self):
        self._conn: MysqlConnection | None = None
        self._tx_id = None

    def begin(self, ds: IDataSource):
        self._tx_id = uuid.uuid4()
        self._conn = ds.get_connection()
        self._conn.begin()

        logger.debug(f'begin transaction: {self._tx_id}')

    def commit(self):
        self._conn.commit()
        logger.debug(f'commit transaction: {self._tx_id}')
        self._conn.close()

    def rollback(self):
        self._conn.rollback()
        logger.debug(f'rollback transaction: {self._tx_id}')
        self._conn.close()

    def tx(self):
        return self._conn

    def tx_id(self):
        return self._tx_id


dorm_context = ContextVar('sql_context', default=DormContext())
