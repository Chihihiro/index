from sqlalchemy import Column, String, Integer, Date, DateTime, DECIMAL, Enum, create_engine, Table
from utils.database.models import mixin
from sqlalchemy.ext.declarative import declarative_base, DeferredReflection

Base = declarative_base()


class SyncSource(mixin.TimeMixin, mixin.GeneralMetaMixin, Base):
    __tablename__ = "sync_source"
    __table_args__ = {"schema": "config_private"}

    target_table = Column("target_table", primary_key=True)
    pk = Column("pk", String, primary_key=True)
    source_id = Column("source_id", String)
    priority = Column("priority", Integer)


class SourceInfo(mixin.GeneralMetaMixin, mixin.TimeMixin, Base):
    __tablename__ = "source_info"
    __table_args__ = {"schema": "config_private"}

    source_id = Column("source_id", String, primary_key=True)
    name = Column("name", String, primary_key=True)
    name_detail = Column("name_detail", String)
    is_granted = Column("is_granted", Integer)


