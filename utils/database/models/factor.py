from sqlalchemy import Column, String, Date, DECIMAL, Integer
from sqlalchemy.ext.declarative import declarative_base
from utils.database.models import mixin

Base = declarative_base()


class FactorInfo(mixin.TimeMixin, Base):
    __tablename__ = "fund_info"
    __table_args__ = {"schema": "base"}

    fund_id = Column("fund_id", String, primary_key=True)
    fund_name = Column("fund_name", String(255))
    fund_name_1 = Column("fund_name_1", String(255))
    reg_time = Column("reg_time", Date)
    reg_code = Column("reg_code", String(255))
    foundation_date = Column("foundation_date", Date)
    currency = Column("currency", Date)
    is_reg = Column("is_reg", Integer)
    is_private = Column("is_private", Integer)
    fund_type_strategy = Column("fund_type_strategy", String)
    fund_member = Column("fund_member", String)
    fund_manager = Column("fund_manager", String)
    fund_consultant = Column("fund_consultant", String)
    fund_manager_nominal = Column("fund_manager_nominal", String)
    fund_custodian = Column("fund_custodian", String)
    fund_stockbroker = Column("fund_stockbroker", String)
    fund_status = Column("fund_status", String)
    liquidation_cause = Column("liquidation_cause", String)
    init_total_asset = Column("init_total_asset", String)
    data_freq = Column("data_freq", String)


class FactorStyleD(mixin.TimeMixin, Base):
    __tablename__ = "factor_style_d"
    __table_args__ = {"schema": "factor"}

    statistic_date = Column("statistic_date", Date, primary_key=True)
    factor_id = Column("factor_id", String)
    factor_name = Column("factor_name", String)
    date = Column("date", Date)
    factor_value = Column("factor_value", DECIMAL)


class FactorStyleW(mixin.TimeMixin, Base):
    __tablename__ = "factor_style_w"
    __table_args__ = {"schema": "factor"}

    statistic_date = Column("statistic_date", Date, primary_key=True)
    factor_id = Column("factor_id", String)
    factor_name = Column("factor_name", String)
    date = Column("date", Date)
    factor_value = Column("factor_value", DECIMAL)
