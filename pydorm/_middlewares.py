from typing import Any, Callable, Dict, List

from ._delete_wrapper import DeleteWrapper
from ._query_wrapper import QueryWrapper
from ._update_wrapper import UpdateWrapper

before_query_middlewares: List[
    Callable[[QueryWrapper[Any] | UpdateWrapper[Any] | DeleteWrapper[Any]], None]
] = []
before_insert_middlewares: List[Callable[[Dict[str, Any] | List[Dict[str, Any]]], None]] = []


def use_query_middleware(
    middleware: Callable[[QueryWrapper[Any] | UpdateWrapper[Any] | DeleteWrapper[Any]], None],
):
    before_query_middlewares.append(middleware)


def use_insert_middleware(middleware: Callable[[Dict[str, Any] | List[Dict[str, Any]]], None]):
    before_insert_middlewares.append(middleware)
