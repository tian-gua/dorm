from ._mysql_datasource import MysqlDataSource
from .protocols import IDataSource

data_sources: list[IDataSource] = []


def default_datasource() -> IDataSource | None:
    for ds in data_sources:
        if ds.get_id() == "default":
            return ds
    return None


def load_data_sources(config_dict: dict):
    data_source_config = config_dict.get("data_source")
    if data_source_config is None:
        raise ValueError("data_source config not found")

    for ds_id, ds_conf in data_source_config.items():
        if "dialect" not in ds_conf:
            raise ValueError("dialect is required")

        if "mysql" == ds_conf["dialect"]:
            mysql_ds = MysqlDataSource(
                ds_id=ds_id,
                host=ds_conf["host"],
                port=ds_conf["port"],
                user=ds_conf["user"],
                password=ds_conf["password"],
                database=ds_conf["database"],
            )
            data_sources.append(mysql_ds)
        else:
            raise ValueError(f'unsupported dialect: {ds_conf["dialect"]}')


def add_datasource(
    ds_id: str,
    dialect: str,
    host: str,
    port: int,
    user: str,
    password: str,
    database: str,
):
    if dialect is None or dialect == "":
        raise ValueError("dialect is required")
    if host is None or host == "":
        raise ValueError("host is required")
    if port is None or port <= 0:
        raise ValueError("port is required and must be greater than 0")
    if user is None or user == "":
        raise ValueError("user is required")
    if password is None or password == "":
        raise ValueError("password is required")
    if database is None or database == "":
        raise ValueError("database is required")

    for ds in data_sources:
        if ds.get_id() == ds_id:
            raise ValueError(f"data source with id {ds_id} already exists")

    if "mysql" == dialect:
        mysql_ds = MysqlDataSource(
            ds_id=ds_id,
            host=host,
            port=port,
            user=user,
            password=password,
            database=database,
        )
        data_sources.append(mysql_ds)
    else:
        raise ValueError(f"unsupported dialect: {dialect}")


def get_datasource(ds_id: str) -> IDataSource | None:
    for ds in data_sources:
        if ds.get_id() == ds_id:
            return ds
    return None
