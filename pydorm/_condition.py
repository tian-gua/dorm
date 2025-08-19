from typing import Any, List, Tuple
from .enums import Operator


class Condition:
    def __init__(self, field: str, value: Any, operator: Operator = Operator.EQ):
        self.field: str = field
        self.value: Any = value
        self.operator: Operator = operator

    def parse(self) -> tuple[str, Any]:
        return f"{self.field} {self.operator.value} ?", self.value


class ConditionTree:
    def __init__(self, logic="and"):
        self.conditions: List[Condition | ConditionTree] = []
        self.logic = logic

    def count(self):
        return len(self.conditions)

    def add_condition(self, condition: Condition):
        self.conditions.append(condition)
        return self

    def add_tree(self, condition_tree: "ConditionTree"):
        self.conditions.append(condition_tree)
        return self

    def parse(self) -> Tuple[str, Tuple[Any, ...]]:
        if len(self.conditions) == 0:
            return "", ()
        args: List[Any] = []
        exps: List[str] = []
        for condition in self.conditions:
            if isinstance(condition, ConditionTree):
                exp, arg = condition.parse()
                exps.append(f"({exp})")
                args.extend(arg)
            else:
                exp, arg = condition.parse()
                exps.append(exp)
                args.append(arg)
        return f" {self.logic} ".join(exps), tuple(args)
