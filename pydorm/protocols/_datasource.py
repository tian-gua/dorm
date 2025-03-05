from typing import Protocol, runtime_checkable


@runtime_checkable
class IDataSource(Protocol):
    def get_id(self) -> str:
        ...

    def get_host(self) -> str:
        ...

    def get_port(self) -> int:
        ...

    def get_user(self) -> str:
        ...

    def get_password(self):
        ...

    def get_default_database(self) -> str:
        ...

    def get_connection(self):
        ...
