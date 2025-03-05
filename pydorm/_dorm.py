from loguru import logger

from ._mysql_datasource import MysqlDataSource
from .protocols import IDataSource


class Dorm:
    def __init__(self):
        self._init = False
        self._config_dict = None
        self._datasource = []

    def is_initialized(self):
        return self._init

    def init(self, config_dict):
        self._config_dict = config_dict

        data_source_config = self.get_config('data_source')
        for ds_id, ds_conf in data_source_config.items():
            if 'dialect' not in ds_conf:
                raise ValueError('dialect is required')

            if 'mysql' == ds_conf['dialect']:
                mysql_ds = MysqlDataSource(id_=ds_id,
                                           host=ds_conf['host'],
                                           port=ds_conf['port'],
                                           user=ds_conf['user'],
                                           password=ds_conf['password'],
                                           database=ds_conf['database'])
                self._datasource.append(mysql_ds)
            else:
                raise ValueError(f'unsupported dialect: {ds_conf["dialect"]}')

        self._init = True
        logger.info('dorm initialized')

    def get_config(self, *keys):
        conf = self._config_dict
        if not conf:
            raise ValueError('uninitialized')
        for key in keys:
            conf = conf.get(key)
            if not conf:
                raise ValueError(f'config not found: {keys}')
        return conf

    def default_datasource(self) -> IDataSource | None:
        for ds in self._datasource:
            if ds.get_id() == 'default':
                return ds
        return None

    def begin(self):
        raise NotImplementedError()

    def commit(self):
        raise NotImplementedError()


dorm = Dorm()
