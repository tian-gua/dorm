from pydorm.protocols import IDataSource
from ._mysql_datasource import MysqlDataSource

data_sources: list[IDataSource] = []


def default_datasource() -> IDataSource | None:
    for ds in data_sources:
        if ds.get_id() == 'default':
            return ds
    return None


def load_data_sources(config_dict: dict):
    data_source_config = config_dict.get('data_source')
    if data_source_config is None:
        raise ValueError('data_source config not found')

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
            data_sources.append(mysql_ds)
        else:
            raise ValueError(f'unsupported dialect: {ds_conf["dialect"]}')
