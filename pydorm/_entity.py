from dataclasses import make_dataclass, field
from typing import get_type_hints, Optional


def entity(table: str):
    def _entity(cls):
        annotations = get_type_hints(cls)
        new_fields = []

        for name, typ in annotations.items():
            if name.startswith('__'):
                continue
            # 将所有字段默认值设为 None，类型设为 Optional[原类型]
            new_fields.append((name, Optional[typ], field(default=None)))

        new_cls = make_dataclass(cls.__name__, new_fields, bases=cls.__bases__)
        new_cls.__table_name__ = table

        for new_field in new_fields:
            setattr(new_cls, new_field[0], new_field[0])

        return new_cls

    return _entity


def vo(cls):
    annotations = get_type_hints(cls)
    new_fields = []

    for name, typ in annotations.items():
        if name.startswith('__'):
            continue
        # 将所有字段默认值设为 None，类型设为 Optional[原类型]
        new_fields.append((name, Optional[typ], field(default=None)))

    new_cls = make_dataclass(cls.__name__, new_fields, bases=cls.__bases__)
    return new_cls
