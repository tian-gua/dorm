from typing import Any, Generic, Type, TypeVar, get_type_hints
from pydorm._where import Or, Where
from .protocols import EntityProtocol

T = TypeVar("T", bound=EntityProtocol)


class DeleteWrapper(Generic[T]):
    def __init__(
        self,
        entity_type: Type[T],
    ):

        self._entity_type = entity_type
        self._table = entity_type.__table_name__

        self._where = Where()

        fields = list(get_type_hints(entity_type).keys())
        self._fields = [field for field in fields if not field.startswith("__")]

    def get_type(self) -> Type[T]:
        return self._entity_type

    def check_field(self, field: str):
        if not hasattr(self._entity_type, field):
            raise ValueError(f"invalid field [{field}] in entity [{self._entity_type}]")

    def eq(self, field: str, value: Any) -> "DeleteWrapper[T]":
        self.check_field(field)
        self._where.eq(field, value)
        return self

    def ne(self, field: str, value: Any) -> "DeleteWrapper[T]":
        self.check_field(field)
        self._where.ne(field, value)
        return self

    def gt(self, field: str, value: Any) -> "DeleteWrapper[T]":
        self.check_field(field)
        self._where.gt(field, value)
        return self

    def ge(self, field: str, value: Any) -> "DeleteWrapper[T]":
        self.check_field(field)
        self._where.ge(field, value)
        return self

    def lt(self, field: str, value: Any) -> "DeleteWrapper[T]":
        self.check_field(field)
        self._where.lt(field, value)
        return self

    def le(self, field: str, value: Any) -> "DeleteWrapper[T]":
        self.check_field(field)
        self._where.le(field, value)
        return self

    def in_(self, field: str, value: Any) -> "DeleteWrapper[T]":
        self.check_field(field)
        self._where.in_(field, value)
        return self

    def l_like(self, field: str, value: Any) -> "DeleteWrapper[T]":
        self.check_field(field)
        self._where.l_like(field, value)
        return self

    def r_like(self, field: str, value: Any) -> "DeleteWrapper[T]":
        self.check_field(field)
        self._where.r_like(field, value)
        return self

    def like(self, field: str, value: Any) -> "DeleteWrapper[T]":
        self.check_field(field)
        self._where.like(field, value)
        return self

    def or_(self, or_: Or) -> "DeleteWrapper[T]":
        self._where.or_(or_)
        return self

    def build_sql(self) -> tuple[str, tuple[Any, ...]]:
        sql = f"DELETE FROM {self._table}"
        args = ()

        exp, args2 = self._where.tree().parse()
        sql += " WHERE " + exp
        args += args2
        return sql, args
