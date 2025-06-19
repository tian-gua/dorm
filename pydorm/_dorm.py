from loguru import logger

from ._datasources import load_data_sources
from ._tx import tx_context, Tx


class Dorm:
    def __init__(self):
        self._init = False
        self._config_dict = None

    def is_initialized(self):
        return self._init

    def init(self, config_dict):
        self._config_dict = config_dict
        load_data_sources(config_dict)
        self._init = True
        logger.info("dorm initialized")

    # noinspection PyMethodMayBeStatic
    def begin(self, ds_id: str | None = None):
        tx = tx_context.get()
        if tx is not None and tx.is_valid():
            raise RuntimeError("Transaction already started")
        new_tx = Tx(ds_id=ds_id, auto_commit=False)
        tx_context.set(new_tx)

    # noinspection PyMethodMayBeStatic
    def commit(self):
        tx = tx_context.get()
        if tx is None:
            raise RuntimeError("No transaction to commit")

        if not tx.is_auto_commit():
            tx.commit()
            tx_context.set(None)
        else:
            logger.warning("Auto-commit is enabled, no explicit commit needed.")

    # noinspection PyMethodMayBeStatic
    def rollback(self):
        tx = tx_context.get()
        if tx is None:
            raise RuntimeError("No transaction to rollback")

        if not tx.is_auto_commit():
            tx.rollback()
            tx_context.set(None)
        else:
            logger.warning("Auto-commit is enabled, no explicit rollback needed.")


dorm = Dorm()
