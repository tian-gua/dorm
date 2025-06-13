from loguru import logger

from ._datasources import load_data_sources


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
        logger.info('dorm initialized')


dorm = Dorm()
