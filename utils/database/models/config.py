from sqlalchemy import Column, String, Integer, Date, DateTime, DECIMAL, Enum, create_engine, Table
from utils.database.models import mixin
from sqlalchemy.ext.declarative import declarative_base, DeferredReflection

Base = declarative_base()


class ConfigSource(mixin.TimeMixin, mixin.TablenameMixin, Base):
    __tablename__ = "config_source"
    __table_args__ = {"schema": "config"}

    schema_ = Column("schema_", Enum("crawl_public", "base_public"), primary_key=True)
    table_ = Column("table_", Enum("fund_nv"), primary_key=True)
    pk = Column("pk", String, primary_key=True)
    data_sources = Column("data_sources", String)
    method = Column("method", Integer)
