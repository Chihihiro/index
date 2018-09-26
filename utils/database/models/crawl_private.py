from sqlalchemy import Column, String, Date, DECIMAL, Integer, Numeric
from sqlalchemy.ext.declarative import declarative_base
from utils.database.models import mixin


Base = declarative_base()


class YFundNv(mixin.GeneralMetaMixin, mixin.TimeMixin, Base):
    __tablename__ = "y_fund_nv"
    __table_args__ = {"schema": "crawl_private"}

    fund_id = Column("fund_id", String, primary_key=True)
    fund_name = Column("fund_name", String)
    source_id = Column("source_id", String, primary_key=True)
    statistic_date = Column("statistic_date", Date, primary_key=True)
    nav = Column("nav", DECIMAL)
    added_nav = Column("added_nav", DECIMAL)
    adjusted_nav = Column("adjusted_nav", DECIMAL)


class DFundInfo(mixin.GeneralMetaMixin, mixin.TimeMixin, Base):
    __tablename__ = "d_fund_info"
    __table_args__ = {"schema": "crawl_private"}

    uuid = Column("uuid", String, primary_key=True)
    fund_id = Column("fund_id", String)
    fund_name = Column("fund_name", String)
    fund_full_name = Column("fund_full_name", String)
    foundation_date = Column("foundation_date", Date)
    fund_status = Column("fund_status", String)
    purchase_status = Column("purchase_status", String)
    purchase_range = Column("purchase_range", String)
    redemption_status = Column("redemption_status", String)
    aip_status = Column("aip_status", String)
    recommendation_start = Column("recommendation_start", Date)
    recommendation_end = Column("recommendation_end", Date)
    fund_type = Column("fund_type", String)
    fund_manager = Column("fund_manager", String)
    fund_manager_nominal = Column("fund_manager_nominal", String)
    fund_custodian = Column("fund_custodian", String)
    init_raise = Column("init_raise", String)


class DFundNv(mixin.GeneralMetaMixin, mixin.TimeMixin, Base):
    __tablename__ = "d_fund_nv"
    __table_args__ = {"schema": "crawl_private"}

    fund_id = Column("fund_id", String, primary_key=True)
    fund_name = Column("fund_name", String)
    source_id = Column("source_id", String, primary_key=True)
    statistic_date = Column("statistic_date", Date, primary_key=True)
    nav = Column("nav", DECIMAL)
    added_nav = Column("added_nav", DECIMAL)
    adjusted_nav = Column("adjusted_nav", DECIMAL)


class SFundNv(mixin.GeneralMetaMixin, mixin.TimeMixin, Base):
    __tablename__ = "s_fund_nv"
    __table_args__ = {"schema": "crawl_private"}

    fund_id = Column("fund_id", String, primary_key=True)
    fund_name = Column("fund_name", String)
    source_id = Column("source_id", String, primary_key=True)
    statistic_date = Column("statistic_date", Date, primary_key=True)
    nav = Column("nav", DECIMAL)
    added_nav = Column("added_nav", DECIMAL)



class TFundNv(mixin.GeneralMetaMixin, mixin.TimeMixin, Base):
    __tablename__ = "t_fund_nv"
    __table_args__ = {"schema": "crawl_private"}

    fund_id = Column("fund_id", String, primary_key=True)
    fund_name = Column("fund_name", String)
    source_id = Column("source_id", String, primary_key=True)
    statistic_date = Column("statistic_date", Date, primary_key=True)
    nav = Column("nav", DECIMAL)
    added_nav = Column("added_nav", DECIMAL)


class GFundNv(mixin.GeneralMetaMixin, mixin.TimeMixin, Base):
    __tablename__ = "g_fund_nv"
    __table_args__ = {"schema": "crawl_private"}

    fund_id = Column("fund_id", String, primary_key=True)
    fund_name = Column("fund_name", String)
    source_id = Column("source_id", String, primary_key=True)
    statistic_date = Column("statistic_date", Date, primary_key=True)

    nav = Column("nav", DECIMAL)
    added_nav = Column("added_nav", DECIMAL)
    adjusted_nav = Column("adjusted_nav", DECIMAL)


class DFundYield(mixin.GeneralMetaMixin, mixin.TimeMixin, Base):
    __tablename__ = "d_fund_yield"
    __table_args__ = {"schema": "crawl_private"}

    fund_id = Column("fund_id", String, primary_key=True)
    fund_name = Column("fund_name", String)
    statistic_date = Column("statistic_date", Date, primary_key=True)
    return_10k = Column("return_10k", Numeric, primary_key=True)
    d7_return_a = Column("d7_return_a", Numeric, primary_key=True)


class XOrgInfo(mixin.GeneralMetaMixin, mixin.TimeMixin, Base):
    __tablename__ = "x_org_info"
    __table_args__ = {"schema": "crawl_private"}

    version = Column("version", Integer, primary_key=True)
    org_id = Column("org_id", String, primary_key=True)
    org_name = Column("org_name", String)
    org_full_name = Column("org_full_name", String)
    org_name_en = Column("org_name_en", String)
    org_name_py = Column("org_name_py", String)
    org_category = Column("org_category", String)  # 可能为冗余字段, 考虑删除
    found_date = Column("found_date", Date)
    org_code = Column("org_code", String)
    reg_code = Column("reg_code", String)
    reg_time = Column("reg_time", Date)
    manage_type = Column("manage_type", String)
    other_manage_type = Column("other_manage_type", Date)  # 可能为冗余字段, 考虑删除
    fund_num = Column("fund_num", Integer)
    fund_scale = Column("fund_scale", Numeric)
    manage_fund = Column("manage_fund", String)
    property_ = Column("property", String)
    currency = Column("currency", String)
    reg_capital = Column("reg_capital", Numeric)
    real_captial = Column("real_captial", Numeric)
    real_capital_proportion = Column("real_capital_proportion", String)
    legal_person = Column("legal_person", String)
    legal_person_resume = Column("legal_person_resume", String)
    is_qualified = Column("is_qualified", String)
    qualifying_way = Column("qualifying_way", String)
    legal_opinion_status = Column("legal_option_status", String)
    integrity_info = Column("integrity_info", String)
    law_firm_name = Column("law_firm_name", String)
    lawyer_name = Column("lawyer_name", String)
    is_member = Column("is_member", String)
    member_type = Column("member_type", String)
    initiation_time = Column("initiation_time", String)
    address = Column("address", String)
    reg_address = Column("reg_address", String)
    org_web = Column("org_web", String)
    employee_scale = Column("employeescale", String)  # need to modify
    final_report_time = Column("final_report_time", String)
    special_tips = Column("special_tips", String)


class XOrgFund(mixin.GeneralMetaMixin, mixin.TimeMixin, Base):
    __tablename__ = "x_org_fund"
    __table_args__ = {"schema": "crawl_private"}

    version = Column("version", Integer, primary_key=True)
    org_id = Column("org_id", String, primary_key=True)
    org_name = Column("org_name", String)
    fund_id = Column("fund_id", String)
    fund_name = Column("fund_name", String)
    org_type = Column("org_type", String)
