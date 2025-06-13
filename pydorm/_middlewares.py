from typing import Callable

from pydorm.enums import Middleware

before_query_middlewares: list[Callable[..., None]] = []
before_update_middlewares: list[Callable[..., None]] = []
before_insert_middlewares: list[Callable[[..., dict], None]] = []


def use_middleware(middleware_type: Middleware, middleware):
    if middleware_type == Middleware.BEFORE_QUERY:
        before_query_middlewares.append(middleware)
    elif middleware_type == Middleware.BEFORE_UPDATE:
        before_update_middlewares.append(middleware)
    elif middleware_type == Middleware.BEFORE_INSERT:
        before_insert_middlewares.append(middleware)
    else:
        raise ValueError(f'unsupported middleware type: {middleware_type}')


def get_middlewares(middleware_type: Middleware):
    if middleware_type == Middleware.BEFORE_QUERY:
        return before_query_middlewares
    elif middleware_type == Middleware.BEFORE_UPDATE:
        return before_update_middlewares
    elif middleware_type == Middleware.BEFORE_INSERT:
        return before_insert_middlewares
    else:
        raise ValueError(f'unsupported middleware type: {middleware_type}')
