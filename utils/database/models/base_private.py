from sqlalchemy import Column, String, Date, DECIMAL, Integer
from sqlalchemy.ext.declarative import declarative_base
from utils.database.models import mixin

Base = declarative_base()


class FundInfo(mixin.TimeMixin, Base):
    __tablename__ = "fund_info"
    __table_args__ = {"schema": "base"}

    fund_id = Column("fund_id", String, primary_key=True)
    fund_name = Column("fund_name", String(255))
    fund_name_py = Column("fund_name_py", String(255))
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


class IdMatch(mixin.GeneralMetaMixin, mixin.TimeMixin, Base):
    __tablename__ = "id_match"
    __table_args__ = {"schema": "base"}

    id_type = Column("id_type", Integer, primary_key=True)
    matched_id = Column("matched_id", String(255), primary_key=True)
    source = Column("source", String(255), primary_key=True)
    source_id = Column("source_id", String(255), primary_key=True)


class FundIdMatch(mixin.TimeMixin, Base):
    __tablename__ = "fund_id_match"
    __table_args__ = {"schema": "base"}

    fund_ID = Column("fund_ID", String, primary_key=True)
    source_ID = Column("source_ID", String(255), primary_key=True)
    match_type = Column("match_type", Integer, primary_key=True)


class FundNvUpdateSource(mixin.TimeMixin, Base):
    __tablename__ = "fund_nv_updata_source"
    __table_args__ = {"schema": "base"}

    fund_id = Column("fund_ID", String, primary_key=True)
    data_source = Column("data_source", Integer, primary_key=True)
    is_updata = Column("is_updata", Integer)


class FundAllocationData(mixin.TimeMixin, Base):
    __tablename__ = "fund_allocation_data"
    __table_args__ = {"schema": "base"}

    fund_id = Column("fund_id", String, primary_key=True)
    statistic_date = Column("statistic_date", Date, primary_key=True)
    source_code = Column("source_code", Integer)
    source = Column("source", String)
    fund_allocation_category = Column("fund_allocation_category", Integer)
    after_tax_bonux = Column("after_tax_bonux", DECIMAL)
    split_ratio = Column("split_ratio", DECIMAL)
    remark = Column("remark", String)


class FundAssetScale(mixin.GeneralMetaMixin, mixin.TimeMixin, Base):
    __tablename__ = "fund_asset_scale"
    __table_args__ = {"schema": "base"}

    fund_id = Column("fund_id", String, primary_key=True)
    fund_name = Column("fund_name", String)
    statistic_date = Column("statistic_date", Date, primary_key=True)
    asset_scale = Column("asset_scale", DECIMAL)
    number_clients = Column("number_clients", Integer)


class FundFeeData(mixin.TimeMixin, Base):
    __tablename__ = "fund_fee_data"
    __table_args = {"schema": "base"}

    fund_id = Column("fund_id", String, primary_key=True)
    min_purchase_amount = Column("min_purchase_amount", DECIMAL)
    min_append_amount = Column("min_append_amount", DECIMAL)
    currency = Column("currency", String)
    fee_type_code = Column("fee_type_code", String)
    fee_type = Column("fee_type", String)
    fee_level = Column("fee_level", Integer)
    fee_ratio = Column("fee_ratio", DECIMAL)
    fee = Column("fee", DECIMAL)
    upper_limit = Column("upper_limit", DECIMAL)
    lower_limit = Column("lower_limit", DECIMAL)
    limit_units = Column("limit_units", String)
    limit_name = Column("limit_name", String)
    start_date = Column("start_date", Date)
    end_date = Column("end_date", Date)
    remark = Column("remark", String)


class FundInfoSubsidiary(mixin.TimeMixin, Base):
    __tablename__ = "fund_info_subsidiary"
    __table_args__ = {"schema": "base"}

    fund_id = Column("fund_id", String, primary_key=True)
    issuing_scale = Column("issuing_scale", DECIMAL)
    investment_range = Column("investment_range", String)


class FundNvDataStandard(mixin.TimeMixin, Base):
    __tablename__ = "fund_nv_data_standard"
    __table_args__ = {"schema": "base"}

    fund_id = Column("fund_id", String, primary_key=True)
    statistic_date = Column("statistic_date", Date, primary_key=True)
    nav = Column("nav", DECIMAL)
    added_nav = Column("added_nav", DECIMAL)
    swanav = Column("swanav", DECIMAL)
    source_code = Column("source_code", Integer)
    source = Column("source", String)


class FundNvStandardM(mixin.TimeMixin, Base):
    __tablename__ = "fund_nv_standard_m"
    __table_args__ = {"schema": "base"}

    fund_id = Column("fund_id", String, primary_key=True)
    statistic_date = Column("statistic_date", Date, primary_key=True)
    statistic_date_std = Column("statistic_date_std", Date)
    nav = Column("nav", DECIMAL)
    added_nav = Column("added_nav", DECIMAL)
    swanav = Column("swanav", DECIMAL)


class FundNvStandardW(mixin.TimeMixin, Base):
    __tablename__ = "fund_nv_standard_w"
    __table_args__ = {"schema": "base"}

    fund_id = Column("fund_id", String, primary_key=True)
    statistic_date = Column("statistic_date", Date, primary_key=True)
    statistic_date_std = Column("statistic_date_std", Date)
    nav = Column("nav", DECIMAL)
    added_nav = Column("added_nav", DECIMAL)
    swanav = Column("swanav", DECIMAL)


class FundNvDataSource(mixin.GeneralMetaMixin, mixin.TimeMixin, Base):
    __tablename__ = "fund_nv_data_source_copy2"
    __table_args__ = {"schema": "base"}

    fund_id = Column("fund_id", String, primary_key=True)
    statistic_date = Column("statistic_date", Date)
    source_id = Column("source_id", String, primary_key=True)
    nav = Column("nav", DECIMAL)
    added_nav = Column("added_nav", DECIMAL)
    adjusted_nav = Column("adjusted_nav", DECIMAL)


class FundPortfolio(mixin.TimeMixin, Base):
    __tablename__ = "fund_portfolio"
    __table_args__ = {"schema": "base"}

    fund_id = Column("fund_id", String, primary_key=True)
    fund_name = Column("fund_name", String)
    subfund_id = Column("subfund_id", String, primary_key=True)
    subfund_name = Column("subfund_name", String)
    portfolio_type = Column("portfolio_type", Integer)
    portfolio_date = Column("portfolio_date", Date)
    portfolio_ratio = Column("portfolio_ratio", DECIMAL)


class FundSecurityData(mixin.TimeMixin, Base):
    __tablename__ = "fund_security_data"
    __table_args__ = {"schema": "base"}

    fund_id = Column("fund_id", String, primary_key=True)
    fund_name = Column("fund_name", String)
    statistic_date = Column("statistic_date", Date, primary_key=True)
    source_code = Column("source_code", Integer, primary_key=True)
    source = Column("source", String)
    security_category = Column("security_category", String)
    id = Column("id", String, primary_key=True)
    name = Column("name", String, primary_key=True)
    sum = Column("sum", DECIMAL)
    ratio = Column("ratio", DECIMAL)
    value = Column("value", DECIMAL)
    remark = Column("remark", String)


class FundTypeMapping(mixin.TimeMixin, Base):
    __tablename__ = "fund_type_mapping"
    __table_args__ = {"schema": "base"}

    fund_id = Column("fund_id", String, primary_key=True)
    fund_name = Column("fund_name", String)
    typestandard_code = Column("typestandard_code", Integer, primary_key=True)
    typestandard_name = Column("typestandard_name", String)
    type_code = Column("type_code", Integer, primary_key=True)
    type_name = Column("type_name", String)
    stype_code = Column("stype_code", Integer, primary_key=True)
    stype_name = Column("stype_name", String)
    flag = Column("flag", Integer)
    comfirmed = Column("comfirmed", String)
    classified_by = Column("classified_by", String)
    is_checked = Column("is_checked", Integer)


class FundTypeMappingImport(mixin.TimeMixin, Base):
    __tablename__ = "fund_type_mapping_import"
    __table_args__ = {"schema": "base"}

    fund_id = Column("fund_id", String, primary_key=True)
    fund_name = Column("fund_name", String)
    typestandard_code = Column("typestandard_code", Integer, primary_key=True)
    typestandard_name = Column("typestandard_name", String)
    type_code = Column("type_code", Integer, primary_key=True)
    type_name = Column("type_name", String)
    stype_code = Column("stype_code", Integer, primary_key=True)
    stype_name = Column("stype_name", String)
    comfirmed = Column("comfirmed", String)
    classified_by = Column("classified_by", String)


class FundTypeSource(mixin.TimeMixin, Base):
    __tablename__ = "fund_type_source"
    __table_args__ = {"schema": "base"}

    fund_id = Column("fund_id", String, primary_key=True)
    fund_name = Column("fund_name", String)
    source_id = Column("source_id", Integer, primary_key=True)
    typestandard_code = Column("typestandard_code", String, primary_key=True)
    typestandard_name = Column("typestandard_name", String)
    type_code = Column("type_code", String)
    type_name = Column("type_name", String)
    stype_code = Column("stype_code", String)
    stype_name = Column("stype_name", String)


class FundWeeklyReturn(mixin.TimeMixin, Base):
    __tablename__ = "fund_weekly_return"
    __table_args__ = {"schema": "base"}

    fund_id = Column("fund_id", String, primary_key=True)
    statistic_date = Column("statistic_date", Date, primary_key=True)
    benchmark = Column("benchmark", Integer, primary_key=True)


class FundWeeklyRisk(mixin.TimeMixin, Base):
    __tablename__ = "fund_weekly_risk"
    __table_args__ = {"schema": "base"}

    fund_id = Column("fund_id", String, primary_key=True)
    statistic_date = Column("statistic_date", Date, primary_key=True)
    benchmark = Column("benchmark", Integer, primary_key=True)


class FundSubsidiaryWeeklyIndex(mixin.TimeMixin, Base):
    __tablename__ = "fund_subsidiary_weekly_index"
    __table_args__ = {"schema": "base"}

    fund_id = Column("fund_id", String, primary_key=True)
    statistic_date = Column("statistic_date", Date, primary_key=True)
    benchmark = Column("benchmark", Integer, primary_key=True)


class FundMonthReturn(mixin.TimeMixin, Base):
    __tablename__ = "fund_month_return"
    __table_args__ = {"schema": "base"}

    fund_id = Column("fund_id", String, primary_key=True)
    statistic_date = Column("statistic_date", Date, primary_key=True)
    benchmark = Column("benchmark", Integer, primary_key=True)


class FundMonthRisk(mixin.TimeMixin, Base):
    __tablename__ = "fund_month_risk"
    __table_args__ = {"schema": "base"}

    fund_id = Column("fund_id", String, primary_key=True)
    statistic_date = Column("statistic_date", Date, primary_key=True)
    benchmark = Column("benchmark", Integer, primary_key=True)


class FundSubsidiaryMonthIndex(mixin.TimeMixin, Base):
    __tablename__ = "fund_subsidiary_month_index"
    __table_args__ = {"schema": "base"}

    fund_id = Column("fund_id", String, primary_key=True)
    statistic_date = Column("statistic_date", Date, primary_key=True)
    benchmark = Column("benchmark", Integer, primary_key=True)


class OrgInfo(mixin.MultiSourceMixin, mixin.TimeMixin, Base):
    __tablename__ = "org_info"
    __table_args__ = {"schema": "base"}

    org_id = Column("org_id", String, primary_key=True)
    org_name = Column("org_name", String)
    org_name_en = Column("org_name_en", String)
    org_name_py = Column("org_name_py", String)
    org_full_name = Column("org_full_name", String)
    org_category = Column("org_category", String)
    org_code = Column("org_code", Integer)
    reg_code = Column("reg_code", Integer)
    found_date = Column("found_date", Date)
    reg_time = Column("reg_time", Integer)
    manage_type = Column("manage_type", Integer)
    asset_mgt_scale = Column("asset_mgt_scale", Integer)
    property = Column("property", Integer)
    reg_capital = Column("reg_capital", String)
    real_capital = Column("real_capital", String)
    real_capital_proportion = Column("real_capital_proportion", String)
    legal_person = Column("legal_person", String)
    is_qualified = Column("is_qualified", String)
    qualifying_way = Column("qualifying_way", String)
    reg_address = Column("reg_address", String)
    address = Column("address", String)
    org_web = Column("org_web", String)
    employee_scale = Column("employee_scale", String)
    final_report_time = Column("final_report_time", String)
    currency = Column("currency", String)
    is_member = Column("is_member", String)
    member_type = Column("member_type", String)
    initiation_time = Column("initiation_time", String)
    law_firm_name = Column("law_firm_name", String)
    lawyer_name = Column("lawyer_name", String)
    region = Column("region", String)
    prov = Column("prov", String)
    city = Column("city", String)
    area = Column("area", String)
    is_reg_now = Column("is_reg_now", String)
    fund_num = Column("fund_num", String)
    fund_total_num = Column("fund_total_num", String)
    total_asset_mgt_scale = Column("total_asset_mgt_scale", String)
    master_strategy = Column("master_strategy", String)
    phone = Column("phone", String)
    email = Column("email", String)


class OrgAssetScale(mixin.GeneralMetaMixin, mixin.TimeMixin, Base):
    __tablename__ = "org_asset_scale"
    __table_args__ = {"schema": "base"}

    org_id = Column("org_id", String, primary_key=True)
    data_time = Column("data_time", Date, primary_key=True)
    statistic_date = Column("statistic_date", Date)
    asset_scale = Column("asset_scale", DECIMAL)
    funds_num = Column("funds_num", Integer)


class OrgTimeseries(mixin.GeneralMetaMixin, mixin.TimeMixin, Base):
    __tablename__ = "org_timeseries"
    __table_args__ = {"schema": "base"}

    org_id = Column("org_id", String, primary_key=True)
    data_time = Column("data_time", Date, primary_key=True)
    statistic_date = Column("statistic_date", Date)
    employee_scale = Column("employee_scale", DECIMAL)
    real_capital = Column("real_capital", DECIMAL)
    reg_capital = Column("reg_capital", DECIMAL)


class PersonInfo(mixin.GeneralMetaMixin, mixin.TimeMixin, Base):
    __tablename__ = "person_info"
    __table_args__ = {"schema": "base"}
    __schema_table__ = ".".join([__table_args__["schema"], __tablename__])

    person_id = Column("person_id", String, primary_key=True)
    person_name = Column("person_name", String)
    person_name_py = Column("person_name_py", String)
    region = Column("region", String)
    gender = Column("gender", String)
    is_fund_qualification = Column("is_fund_qualification", String)
    fund_qualification_way = Column("fund_qualification_way", String)
    background = Column("background", String)
    education = Column("education", String)
    graduate_school = Column("graduate_school", String)


class PersonDescription(mixin.GeneralMetaMixin, mixin.TimeMixin, Base):
    __tablename__ = "person_description"
    __table_args__ = {"schema": "base"}
    __schema_table__ = ".".join([__table_args__["schema"], __tablename__])

    person_id = Column("person_id", String, primary_key=True)
    person_name = Column("person_name", String)
    resume = Column("resume", String)


class FundOrgMapping(mixin.GeneralMetaMixin, mixin.TimeMixin, Base):
    __tablename__ = "fund_org_mapping"
    __table_args__ = {"schema": "base"}

    fund_id = Column("fund_id", String, primary_key=True)
    fund_name = Column("fund_name", String, primary_key=True)
    org_id = Column("org_id", String, primary_key=True)
    org_name = Column("org_name", String, primary_key=True)
    org_type_code = Column("org_type_code", Integer)
    org_type = Column("org_type", String)
    start_date = Column("start_date", Date)
    end_date = Column("end_date", Date)
    is_current = Column("is_current", Date)


class OrgPersonMapping(mixin.GeneralMetaMixin, mixin.TimeMixin, Base):
    __tablename__ = "org_person_mapping"
    __table_args__ = {"schema": "base"}
    __schema_table__ = ".".join([__table_args__["schema"], __tablename__])

    org_id = Column("org_id", String, primary_key=True)
    org_name = Column("org_name", String)
    person_id = Column("person_id", String, primary_key=True)
    person_name = Column("person_name", String)
    duty = Column("duty", Integer)
    duty_detail = Column("duty_detail", String)
    tenure_date = Column("tenure_date", Date)
    dimission_date = Column("dimission_date", Date)
    tenure_period = Column("tenure_period", Integer)
    is_current = Column("is_current", Integer)


class FundManagerMapping(mixin.GeneralMetaMixin, mixin.TimeMixin, Base):
    __tablename__ = "fund_manager_mapping"
    __table_args__ = {"schema": "base"}

    fund_id = Column("fund_id", String, primary_key=True)
    fund_name = Column("fund_name", String)
    person_id = Column("person_id", String, primary_key=True)
    person_name = Column("person_name", String)
    start_date = Column("start_date", Date)
    end_date = Column("end_date", Date)
    is_current = Column("is_current", Integer)
    is_leader = Column("is_leader", Integer)


class StockPrice(mixin.TimeMixin, Base):
    __tablename__ = "stock_price"
    __table_args__ = {"schema": "base_finance"}

    stock_id = Column("stock_id", String, primary_key=True)
    date = Column("date", Date, primary_key=True)
    close = Column("close", DECIMAL)
    trans_amount = Column("trans_amount", DECIMAL)
    status = Column("status", String)
    last_trading_day = Column("last_trading_day", String)


class StockValuation(mixin.TimeMixin, Base):
    __tablename__ = "stock_valuation"
    __table_args__ = {"schema": "base_finance"}

    stock_id = Column("stock_id", String, primary_key=True)
    date = Column("date", Date, primary_key=True)
    market_price = Column("market_price", DECIMAL)
    circulated_price = Column("circulated_price", DECIMAL)
    pe_ttm = Column("pe_ttm", DECIMAL)
    pe_deducted_ttm = Column("pe_deducted_ttm", DECIMAL)
    pe_lyr = Column("pe_lyr", DECIMAL)
    pb = Column("pb", DECIMAL)
    pb_lf = Column("pb_lf", DECIMAL)
    pb_mrq = Column("pb_mrq", DECIMAL)


class MarketIndex(mixin.TimeMixin, Base):
    __tablename__ = "market_index"
    __table_args__ = {"schema": "base"}

    statistic_date = Column("statistic_date", Date, primary_key=True)
    hs300 = Column("hs300", String)
    sse50 = Column("sse50", String)
    ssia = Column("ssia", DECIMAL)
    cbi = Column("cbi", DECIMAL)
    csi500 = Column("csi500", DECIMAL)
    nfi = Column("nfi", DECIMAL)
    y1_treasury_rate = Column("y1_treasury_rate", DECIMAL)
