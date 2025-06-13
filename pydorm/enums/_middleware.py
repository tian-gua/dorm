from enum import Enum


class Middleware(Enum):
    BEFORE_QUERY = 'before_query'
    BEFORE_INSERT = 'before_insert'
    BEFORE_UPDATE = 'before_update'

    def __str__(self):
        return self.value

    def __repr__(self):
        return f"Middleware.{self.name}"
