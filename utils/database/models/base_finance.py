from sqlalchemy import Column, String, Date, DECIMAL, Integer
from sqlalchemy.ext.declarative import declarative_base
from utils.database.models import mixin


Base = declarative_base()


class BondInfo(mixin.TimeMixin, mixin.GeneralMetaMixin, Base):
    __tablename__ = "bond_info"
    __table_args__ = {"schema": "base_finance"}

    bond_id = Column("bond_id", String, primary_key=True)
    bond_name = Column("bond_name", String)
    bond_full_name = Column("bond_full_name", String)

    issue_price = Column("issue_price", DECIMAL)
    issue_amount = Column("issue_amount", DECIMAL)
    issue_way = Column("issue_way", String)
    par_value = Column("par_value", DECIMAL)
    term = Column("term", DECIMAL)
    coupon_rate = Column("coupon_rate", DECIMAL)
    yield_to_maturity = Column("yield_to_maturity", DECIMAL)
    maturity_date = Column("maturity_date", Date)
    listing_date = Column("listing_date", Date)
    value_date = Column("value_date", Date)
    issue_date_start = Column("issue_date_start", Date)
    issue_date_end = Column("issue_date_end", Date)
    trading_market = Column("trading_market", String)
    consigner = Column("consigner", String)
    consignee = Column("consignee", String)
    interest_freq = Column("interest_freq", String)
    interest_type = Column("interest_type", String)


class BondRating(mixin.TimeMixin, mixin.GeneralMetaMixin, Base):
    __tablename__ = "bond_rating"
    __table_args__ = {"schema": "base_finance"}

    bond_id = Column("bond_id", String, primary_key=True)

    statistic_date = Column("statistic_date", Date)
    rating_type = Column("rating_type", String)
    credit_rating = Column("credit_rating", String)
    rating_agency = Column("rating_agency", String)
    rating_outlook = Column("rating_outlook", String)


class BondTypeSource(mixin.TimeMixin, mixin.GeneralMetaMixin, Base):
    __tablename__ = "bond_type_source"
    bond_id = Column("bond_id", String, primary_key=True)
    source_id = Column("source_id", String, primary_key=True)
    dimension = Column("dimension", String)
    type = Column("type", String)
    stype = Column("stype", String)


class BondDescription(mixin.TimeMixin, mixin.GeneralMetaMixin, Base):
    __tablename__ = "bond_description"
    __table_args__ = {"schema": "base_finance"}

    bond_id = Column("bond_id", String, primary_key=True)
    bond_name = Column("fund_name", String)

    # interest_date = Column("interest_date", String)
    # interest_type = Column("interest_type", String)
    # interest_freq = Column("interest_freq", String)
    # bond_type = Column("bond_type", String)

    source_id = Column("source_id", String)
    remark = Column("remark", String)
