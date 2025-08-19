from typing import Any, TypeVar

from ._condition import ConditionTree, Condition
from .enums import Operator

T = TypeVar("T")


class Where:
    def __init__(self, logic="and") -> None:
        self._condition_tree = ConditionTree(logic)

    def tree(self) -> ConditionTree:
        return self._condition_tree

    def eq(self, field: str, value: Any) -> "Where":
        self._condition_tree.add_condition(Condition(field, value))
        return self

    def ne(self, field: str, value: Any) -> "Where":
        self._condition_tree.add_condition(Condition(field, value, Operator.NE))
        return self

    def gt(self, field: str, value: Any) -> "Where":
        self._condition_tree.add_condition(Condition(field, value, Operator.GT))
        return self

    def ge(self, field: str, value: Any) -> "Where":
        self._condition_tree.add_condition(Condition(field, value, Operator.GE))
        return self

    def lt(self, field: str, value: Any) -> "Where":
        self._condition_tree.add_condition(Condition(field, value, Operator.LT))
        return self

    def le(self, field: str, value: Any) -> "Where":
        self._condition_tree.add_condition(Condition(field, value, Operator.LE))
        return self

    def in_(self, field: str, value: Any) -> "Where":
        self._condition_tree.add_condition(Condition(field, value, Operator.IN))
        return self

    def l_like(self, field: str, value: Any) -> "Where":
        self._condition_tree.add_condition(Condition(field, f"%{value}", Operator.LIKE))
        return self

    def r_like(self, field: str, value: Any) -> "Where":
        self._condition_tree.add_condition(Condition(field, f"{value}%", Operator.LIKE))
        return self

    def like(self, field: str, value: Any) -> "Where":
        self._condition_tree.add_condition(
            Condition(field, f"%{value}%", Operator.LIKE)
        )
        return self

    def or_(self, or_conditions: "Or") -> "Where":
        self._condition_tree.add_tree(or_conditions._condition_tree)
        return self

    def count(self):
        return self._condition_tree.count()


class Or(Where):
    def __init__(self) -> None:
        super().__init__("or")


def where():
    return Where()


def or_():
    return Or()
