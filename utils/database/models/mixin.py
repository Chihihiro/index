from sqlalchemy import Column, String, DateTime, INTEGER
from sqlalchemy.ext.declarative import declared_attr


class TablenameMixin(object):
    """
        Basic Mixin Class for table name declaration.
    """

    @declared_attr
    def __tablename__(cls):
        name = ""
        for char in cls.__name__:
            name += "_" * int(char.isupper()) + char.lower()
        return name[1:]


class TimeMixin(object):
    """
        Mixin Class for table with `update_time` and `entry_time` column.
    """
    entry_time = Column("entry_time", DateTime)
    update_time = Column("update_time", DateTime)


class MultiSourceMixin(object):
    """
        Mixin Class for table with multi-source data.
    """

    data_source = Column("data_source", String, primary_key=True)


class GeneralMetaMixin(object):
    """
        Mixin Class for table with row meta data.
    """

    is_used = Column("is_used", INTEGER)
    is_del = Column("is_del", INTEGER)
