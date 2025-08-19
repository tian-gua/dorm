from typing import Any, Generic, List, Type, TypeVar, get_type_hints
from pydorm._where import Or, Where
from .protocols import EntityProtocol

T = TypeVar("T", bound=EntityProtocol)


class QueryWrapper(Generic[T]):
    def __init__(
        self,
        entity_type: Type[T],
    ):

        self._entity_type = entity_type
        self._table = entity_type.__table_name__

        self._where = Where()
        self._select_fields: List[str] = []
        self._ignore_fields: List[str] = []
        self._order_by = None
        self._limit = None
        self._offset = None
        self._distinct = False

        fields = list(get_type_hints(entity_type).keys())
        self._fields = [field for field in fields if not field.startswith("__")]

    def get_type(self) -> Type[T]:
        return self._entity_type

    def select(self, *select_fields: str, distinct: bool = False) -> "QueryWrapper[T]":
        for select_field in select_fields:
            self.check_field(select_field)

        self._select_fields = list(select_fields)
        self._distinct = distinct
        return self

    def ignore(self, *ignore_fields: str) -> "QueryWrapper[T]":
        for ignore_field in ignore_fields:
            self.check_field(ignore_field)

        self._ignore_fields = list(ignore_fields)
        return self

    def check_field(self, field: str):
        if not hasattr(self._entity_type, field):
            raise ValueError(f"invalid field [{field}] in entity [{self._entity_type}]")

    def eq(self, field: str, value: Any) -> "QueryWrapper[T]":
        self.check_field(field)
        self._where.eq(field, value)
        return self

    def ne(self, field: str, value: Any) -> "QueryWrapper[T]":
        self.check_field(field)
        self._where.ne(field, value)
        return self

    def gt(self, field: str, value: Any) -> "QueryWrapper[T]":
        self.check_field(field)
        self._where.gt(field, value)
        return self

    def ge(self, field: str, value: Any) -> "QueryWrapper[T]":
        self.check_field(field)
        self._where.ge(field, value)
        return self

    def lt(self, field: str, value: Any) -> "QueryWrapper[T]":
        self.check_field(field)
        self._where.lt(field, value)
        return self

    def le(self, field: str, value: Any) -> "QueryWrapper[T]":
        self.check_field(field)
        self._where.le(field, value)
        return self

    def in_(self, field: str, value: Any) -> "QueryWrapper[T]":
        self.check_field(field)
        self._where.in_(field, value)
        return self

    def l_like(self, field: str, value: Any) -> "QueryWrapper[T]":
        self.check_field(field)
        self._where.l_like(field, value)
        return self

    def r_like(self, field: str, value: Any) -> "QueryWrapper[T]":
        self.check_field(field)
        self._where.r_like(field, value)
        return self

    def like(self, field: str, value: Any) -> "QueryWrapper[T]":
        self.check_field(field)
        self._where.like(field, value)
        return self

    def or_(self, or_: Or) -> "QueryWrapper[T]":
        self._where.or_(or_)
        return self

    def desc(self, *order_by: str) -> "QueryWrapper[T]":
        self._order_by = [f"{field} desc" for field in order_by]
        return self

    def asc(self, *order_by: str) -> "QueryWrapper[T]":
        self._order_by = [f"{field} asc" for field in order_by]
        return self

    def limit(self, limit: int) -> "QueryWrapper[T]":
        self._limit = limit
        return self

    def offset(self, offset: int) -> "QueryWrapper[T]":
        self._offset = offset
        return self

    def build_sql(self) -> tuple[str, tuple[Any, ...]]:
        if len(self._select_fields) == 0:
            self._select_fields = self._fields

        select_fields = [field for field in self._select_fields if field not in self._ignore_fields]

        sql = f'SELECT {"DISTINCT " if self._distinct and self._select_fields else ""}{",".join(select_fields)} FROM {self._table}'
        args = ()
        tree = self._where.tree()
        if len(tree.conditions) > 0:
            exp, args = tree.parse()
            sql += " WHERE " + exp
        if self._order_by is not None:
            sql += f' ORDER BY {",".join(self._order_by)}'
        if self._limit is not None:
            sql += f" LIMIT {self._limit}"
        if self._offset is not None:
            sql += f" OFFSET {self._offset}"

        return sql, args

    def build_count_sql(self) -> tuple[str, tuple[Any, ...]]:
        sql = f"SELECT COUNT(*) FROM {self._table}"
        args = ()
        tree = self._where.tree()
        if len(tree.conditions) > 0:
            exp, args = tree.parse()
            sql += " WHERE " + exp
        return sql, args
