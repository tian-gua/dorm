from typing import Any, Dict, Generic, List, Literal, Tuple, Type, TypeVar, get_type_hints
from .protocols import EntityProtocol

T = TypeVar("T", bound=EntityProtocol)


class InsertWrapper(Generic[T]):
    def __init__(self, entity_type: Type[T]):
        self._entity_type = entity_type
        self._table = entity_type.__table_name__

        fields = list(get_type_hints(entity_type).keys())
        self._fields = [field for field in fields if not field.startswith("__")]

    def build_insert_sql(
        self, data: Dict[str, Any], duplicate_key_update: List[str] | Literal["all"] | None = None
    ) -> Tuple[str, Tuple[Any, ...]]:
        placeholder: List[str] = []
        keys: List[str] = []
        values: List[Any] = []
        for k, v in data.items():
            if k in self._fields:
                if v is None:
                    continue
                keys.append(k)
                values.append(v)
                placeholder.append("?")

        if len(keys) == 0:
            raise ValueError("no valid field found")

        sql = f'INSERT INTO {self._table}({",".join(keys)}) VALUES({",".join(placeholder)})'
        if duplicate_key_update is not None:
            if isinstance(duplicate_key_update, str) and duplicate_key_update == "all":
                sql += f' ON DUPLICATE KEY UPDATE {",".join([f"{k}=VALUES({k})" for k in keys])}'
            elif len(duplicate_key_update) > 0:
                sql += f' ON DUPLICATE KEY UPDATE {",".join([f"{k}=VALUES({k})" for k in duplicate_key_update])}'

        args = tuple(values)

        return sql, args

    def build_insert_bulk_sql(
        self,
        data: List[Dict[str, Any]],
        duplicate_key_update: List[str] | Literal["all"] | None = None,
    ) -> Tuple[str, List[Tuple[Any, ...]]]:
        placeholder: List[str] = []
        keys: List[str] = []
        for k, _ in data[0].items():
            if k in self._fields:
                keys.append(k)
                placeholder.append("?")

        if len(keys) == 0:
            raise ValueError("no valid field found")

        sql = f'INSERT INTO {self._table}({",".join(keys)}) VALUES({",".join(placeholder)})'

        if duplicate_key_update is not None:
            if isinstance(duplicate_key_update, str) and duplicate_key_update == "all":
                sql += f' ON DUPLICATE KEY UPDATE {",".join([f"{k}=VALUES({k})" for k in keys])}'
            elif len(duplicate_key_update) > 0:
                sql += f' ON DUPLICATE KEY UPDATE {",".join([f"{k}=VALUES({k})" for k in duplicate_key_update])}'

        args = [tuple(datum[k] for k in keys) for datum in data]
        return sql, args
