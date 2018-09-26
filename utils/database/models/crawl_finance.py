from sqlalchemy import Column, String, Date, DECIMAL, Integer
from sqlalchemy.ext.declarative import declarative_base
from utils.database.models import mixin


Base = declarative_base()


class YBondInfo(mixin.TimeMixin, mixin.GeneralMetaMixin, Base):
    __tablename__ = "y_bond_info"
    __table_args__ = {"schema": "crawl_finance"}

    bond_id = Column("bond_id", String, primary_key=True)
    source_id = Column("source_id", String, primary_key=True)
    bond_name = Column("bond_name", String)
    bond_full_name = Column("bond_full_name", String)

    interest_type = Column("interest_type", String)
    interest_freq = Column("interest_freq", String)


class DBondRating(mixin.TimeMixin, mixin.GeneralMetaMixin, Base):
    __tablename__ = "d_bond_rating"
    __table_args__ = {"schema": "crawl_finance"}

    bond_id = Column("bond_id", String, primary_key=True)

    source_id = Column("source_id", String, primary_key=True)

    statistic_date = Column("statistic_date", Date)
    rating_type = Column("rating_type", String)
    credit_rating = Column("credit_rating", String)
    rating_agency = Column("rating_agency", String)
    rating_outlook = Column("rating_outlook", String)


class DBondInfo(mixin.TimeMixin, mixin.GeneralMetaMixin, Base):
    __tablename__ = "d_bond_info"
    __table_args__ = {"schema": "crawl_finance"}

    bond_id = Column("bond_id", String, primary_key=True)
    bond_name = Column("bond_name", String)
    bond_full_name = Column("bond_full_name", String)

    source_id = Column("source_id", String, primary_key=True)
    issue_price = Column("issue_price", DECIMAL)
    issue_amount = Column("issue_amount", DECIMAL)
    issue_way = Column("issue_way", String)
    par_value = Column("par_value", DECIMAL)
    term = Column("term", DECIMAL)
    coupon_rate = Column("coupon_rate", DECIMAL)
    yield_to_maturity = Column("yield_to_maturity", DECIMAL)
    maturity_date = Column("maturity_date", Date)
    interest_date = Column("interest_date", String)
    value_date = Column("value_date", String)
    interest_type = Column("interest_type", String)
    interest_freq = Column("interest_freq", String)
    listing_date = Column("listing_date", Date)
    issue_date_start = Column("issue_date_start", Date)
    issue_date_end = Column("issue_date_end", Date)
    release_date = Column("release_date", Date)
    bond_type = Column("bond_type", String)
    trading_market = Column("trading_market", String)
    consigner = Column("consigner", String)
    consignee = Column("consignee", String)


class YBondDescription(mixin.TimeMixin, mixin.GeneralMetaMixin, Base):
    __tablename__ = "y_bond_description"
    __table_args__ = {"schema": "crawl_finance"}

    bond_id = Column("bond_id", String, primary_key=True)
    source_id = Column("source_id", String, primary_key=True)

    remark = Column("remark", String)


class DBondDescription(mixin.TimeMixin, mixin.GeneralMetaMixin, Base):
    __tablename__ = "d_bond_description"
    __table_args__ = {"schema": "crawl_finance"}

    bond_id = Column("bond_id", String, primary_key=True)
    bond_name = Column("fund_name", String)

    source_id = Column("source_id", String)
    remark = Column("remark", String)
