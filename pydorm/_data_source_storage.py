from typing import Any, Dict

from .mysql import MysqlDataSource


class DataSourceStorage:
    def __init__(self):
        self._data_sources: Dict[str, MysqlDataSource] = {}

    def default(self) -> MysqlDataSource | None:
        return self._data_sources.get("default", None)

    def load(self, config_dict: Dict[str, Any]):
        data_source_config = config_dict.get("data_source")
        if data_source_config is None:
            raise ValueError("data_source config not found")

        for data_source_id, conf in data_source_config.items():
            if "dialect" not in conf:
                raise ValueError("dialect is required")

            if "mysql" == conf["dialect"]:
                mysql_ds = MysqlDataSource(
                    data_source_id=data_source_id,
                    host=conf["host"],
                    port=conf["port"],
                    user=conf["user"],
                    password=conf["password"],
                    database=conf["database"],
                )
                self._data_sources[data_source_id] = mysql_ds
            else:
                raise ValueError(f'unsupported dialect: {conf["dialect"]}')

    def add_datasource(
        self,
        data_source_id: str,
        dialect: str,
        host: str,
        port: int,
        user: str,
        password: str,
        database: str,
        **options: Any,
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

        if data_source_id in self._data_sources:
            raise ValueError(f"data source with id {data_source_id} already exists")

        if "mysql" == dialect:
            mysql_ds = MysqlDataSource(
                data_source_id=data_source_id,
                host=host,
                port=port,
                user=user,
                password=password,
                database=database,
                **options,
            )
            self._data_sources[data_source_id] = mysql_ds
        else:
            raise ValueError(f"unsupported dialect: {dialect}")

    def get(self, data_source_id: str) -> MysqlDataSource | None:
        return self._data_sources.get(data_source_id, None)
