from ._condition import ConditionTree, Condition
from .enums import Operator
from .protocols import IEntity


class Where:
    def __init__(self, entity: IEntity | None = None, logic='and') -> None:
        self._condition_tree = ConditionTree(logic)
        self._entity: IEntity = entity

    def tree(self) -> ConditionTree:
        return self._condition_tree

    def check_field(self, field: str):
        if self._entity is not None and not hasattr(self._entity, field):
            raise ValueError(f'entity {self._entity} has no field {field}')

    def eq(self, field: str, value: any) -> 'Where':
        self.check_field(field)
        self._condition_tree.add_condition(Condition(field, value))
        return self

    def ne(self, field: str, value: any) -> 'Where':
        self.check_field(field)
        self._condition_tree.add_condition(Condition(field, value, Operator.NE))
        return self

    def gt(self, field: str, value: any) -> 'Where':
        self.check_field(field)
        self._condition_tree.add_condition(Condition(field, value, Operator.GT))
        return self

    def ge(self, field: str, value: any) -> 'Where':
        self.check_field(field)
        self._condition_tree.add_condition(Condition(field, value, Operator.GE))
        return self

    def lt(self, field: str, value: any) -> 'Where':
        self.check_field(field)
        self._condition_tree.add_condition(Condition(field, value, Operator.LT))
        return self

    def le(self, field: str, value: any) -> 'Where':
        self.check_field(field)
        self._condition_tree.add_condition(Condition(field, value, Operator.LE))
        return self

    def in_(self, field: str, value: any) -> 'Where':
        self.check_field(field)
        self._condition_tree.add_condition(Condition(field, value, Operator.IN))
        return self

    def l_like(self, field: str, value: any) -> 'Where':
        self.check_field(field)
        self._condition_tree.add_condition(Condition(field, f'%{value}', Operator.LIKE))
        return self

    def r_like(self, field: str, value: any) -> 'Where':
        self.check_field(field)
        self._condition_tree.add_condition(Condition(field, f'{value}%', Operator.LIKE))
        return self

    def like(self, field: str, value: any) -> 'Where':
        self.check_field(field)
        self._condition_tree.add_condition(Condition(field, f'%{value}%', Operator.LIKE))
        return self

    def or_(self, or_conditions: 'Or') -> 'Where':
        self._condition_tree.add_tree(or_conditions._condition_tree)
        return self

    def count(self):
        return self._condition_tree.count()


class Or(Where):
    def __init__(self, entity: IEntity | None = None) -> None:
        super().__init__(entity, 'or')


def where(entity: IEntity | None = None):
    return Where(entity)


def or_(entity: IEntity | None = None):
    return Or(entity)
