from typing import Protocol


class EntityProtocol(Protocol):
    __table_name__: str
