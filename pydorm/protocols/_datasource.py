from typing import Protocol, runtime_checkable


@runtime_checkable
class IDataSource(Protocol):
    def get_id(self) -> str: ...

    def get_default_database(self) -> str: ...

    def create_connection(self): ...
