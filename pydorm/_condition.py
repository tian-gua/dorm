from .enums import Operator


class Condition:
    def __init__(self, field: str, value: any, operator: Operator = Operator.EQ):
        self.field: str = field
        self.value: any = value
        self.operator: Operator = operator

    def parse(self):
        return f"{self.field} {self.operator.value} ?", self.value


class ConditionTree:
    def __init__(self, logic="and"):
        self.conditions = []
        self.logic = logic

    def count(self):
        return len(self.conditions)

    def add_condition(self, condition: Condition):
        self.conditions.append(condition)
        return self

    def add_tree(self, condition_tree):
        self.conditions.append(condition_tree)
        return self

    def parse(self):
        if len(self.conditions) == 0:
            return None
        args = []
        exps = []
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
