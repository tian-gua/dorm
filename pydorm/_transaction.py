from . import dorm
from ._context import dorm_context
from .protocols import IDataSource


def begin(data_source: IDataSource | None = None):
    dorm_context.get().begin(data_source or dorm.default_datasource())


def commit():
    dorm_context.get().commit()


def rollback():
    dorm_context.get().rollback()
