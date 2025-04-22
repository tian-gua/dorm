from dataclasses import make_dataclass
from typing import get_type_hints


def model(table: str):
    def _model(cls):
        annotations = get_type_hints(cls)
        new_fields = []
        for name, typ in annotations.items():
            if name.startswith('__'):
                continue
            new_fields.append((name, typ, None))
        new_cls = make_dataclass(cls.__name__, new_fields, bases=cls.__bases__)
        new_cls.__table_name__ = table
        return new_cls

    return _model
