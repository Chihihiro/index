from sqlalchemy import Column, String, Date, DECIMAL, Integer, Enum, Numeric
from sqlalchemy.ext.declarative import declarative_base
from utils.database.models import mixin

Base = declarative_base()


class IdMatch(mixin.GeneralMetaMixin, mixin.TimeMixin, Base):
    __tablename__ = "id_match"
    __table_args__ = {"schema": "base_public"}

    id_type = Column("id_type", Integer, primary_key=True)
    matched_id = Column("matched_id", String, primary_key=True)
    data_source = Column("data_source", String, primary_key=True)
    source_id = Column("source_id", String)
    accuracy = Column("accuracy", DECIMAL)


class FundInfo(mixin.TimeMixin, Base):
    __tablename__ = "fund_info"
    __table_args__ = {"schema": "base_public"}

    fund_id = Column("fund_id", String(6), primary_key=True)
    fund_name = Column("fund_name", String(255))
    data_source = Column("data_source", String)
    fund_full_name = Column("fund_full_name", String(255))
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


class FundInfoStructured(mixin.TimeMixin, Base):
    __tablename__ = "fund_info_structured"
    __table_args__ = {"schema": "base_public"}

    fund_id = Column("fund_id", String, primary_key=True)
    fund_name = Column("fund_name", String)
    sfund_id = Column("sfund_id", String, primary_key=True)
    sfund_name = Column("sfund_name", String)
    data_source = Column("data_source", String)
    is_LOF = Column("is_LOF", String)
    is_inverse = Column("is_inverse", String)
    termination_condition = Column("termination_condition", String)
    discount_date_regular = Column("discount_date_regular", String)
    discount_date_irregular = Column("discount_date_irregular", String)
    close_period = Column("close_period", String)
    fix_return_a = Column("fix_return_a", String)
    loss_limit = Column("loss_limit", String)
    guaranteed_limit = Column("guaranteed_limit", String)
    excess_limit = Column("excess_limit", String)


class FundManagerMapping(mixin.TimeMixin, Base):
    __tablename__ = "fund_manager_mapping"
    __table_args__ = {"schema": "base_public"}

    fund_id = Column("fund_id", String, primary_key=True)
    person_id = Column("person_id", String, primary_key=True)
    fund_name = Column("fund_name", String(255))
    person_name = Column("person_name", String(255))
    tenure_date = Column("tenure_date", Date)
    tenure_period = Column("tenure_period", Integer)
    dimission_date = Column("dimission_date", Date)
    is_current = Column("is_current", Integer)


class FundNvSource(mixin.TimeMixin, Base):
    __tablename__ = "fund_nv_source"
    __table_args__ = {"schema": "base_public"}

    fund_id = Column("fund_id", String, primary_key=True)
    fund_name = Column("fund_name", String)
    data_source = Column("data_source", String)
    statistic_date = Column("statistic_date", Date, primary_key=True)
    nav = Column("nav", DECIMAL)
    added_nav = Column("added_nav", DECIMAL)
    swanav = Column("swanav", DECIMAL)


class FundNv(mixin.TimeMixin, Base):
    __tablename__ = "fund_nv"
    __table_args__ = {"schema": "base_public"}

    fund_id = Column("fund_id", String, primary_key=True)
    fund_name = Column("fund_name", String)
    data_source = Column("data_source", String)
    statistic_date = Column("statistic_date", Date, primary_key=True)
    nav = Column("nav", DECIMAL)
    added_nav = Column("added_nav", DECIMAL)
    swanav = Column("swanav", DECIMAL)


class FundDividend(mixin.TimeMixin, Base):
    __tablename__ = "fund_dividend"
    __table_args__ = {"schema": "base_public"}

    fund_id = Column("fund_id", String, primary_key=True)
    fund_name = Column("fund_name", String)
    data_source = Column("data_source", String)
    statistic_date = Column("statistic_date", Date, primary_key=True)
    record_date = Column("record_date")
    ex_dividend_date = Column("ex_dividend_date", Date)
    dividend_date = Column("dividend_date", Date)
    dividend_at = Column("dividend_at", DECIMAL)


class FundSplit(mixin.TimeMixin, Base):
    __tablename__ = "fund_split"
    __table_args__ = {"schema": "base_public"}

    fund_id = Column("fund_id", String, primary_key=True)
    fund_name = Column("fund_name", String)
    statistic_date = Column("statistic_date", Date, primary_key=True)
    data_source = Column("data_source", String)
    split_date = Column("split_date", Date)
    split_ratio = Column("split_ratio", DECIMAL)


class FundDividendSplit(mixin.TimeMixin, Base):
    __tablename__ = "fund_dividend_split"
    __table_args__ = {"schema": "base_public"}

    fund_id = Column("fund_id", String, primary_key=True)
    fund_name = Column("fund_name", String)
    data_source = Column("data_source", String)
    statistic_date = Column("statistic_date", Date, primary_key=True)
    type = Column("type", String, primary_key=True)
    record_date = Column("record_date")
    ex_dividend_date = Column("ex_dividend_date", Date)
    dividend_date = Column("dividend_date", Date)
    split_date = Column("split_date", Date)
    split_ratio = Column("split_ratio", DECIMAL)
    value = Column("value", DECIMAL)


class FundPortfolioAsset(mixin.TimeMixin, Base):
    __tablename__ = "fund_portfolio_asset"
    __table_args__ = {"schema": "base_public"}

    fund_id = Column("fund_id", String, primary_key=True)
    fund_name = Column("fund_name", String)
    data_source = Column("data_source", String)
    statistic_date = Column("statistic_date", Date, primary_key=True)
    asset_type = Column("asset_type", Enum("买入返售金融资产", "固定收益投资", "其他", "基金投资", "权益投资", "金融衍生品投资", "货币市场工具", "现金"), primary_key=True)
    asset_stype = Column("asset_stype", Enum("买入返售金融资产", "债券", "其他", "基金", "存托凭证", "权证", "股票", "货币市场工具", "资产支持证券", "现金", ""), primary_key=True)
    scale = Column("scale", String)
    proportion = Column("proportion", DECIMAL)
    asset_scale = Column("asset_scale", DECIMAL)


class FundPortfolioIndustry(mixin.TimeMixin, Base):
    __tablename__ = "fund_portfolio_industry"
    __table_args__ = {"schema": "base_public"}

    fund_id = Column("fund_id", String, primary_key=True)
    fund_name = Column("fund_name", String)
    data_source = Column("data_source", String)
    statistic_date = Column("statistic_date", Date, primary_key=True)
    style = Column("style", Integer, primary_key=True)
    type = Column("type", Enum("农林牧渔", "采掘", "化工", "钢铁", "有色金属", "电子", "汽车", "家用电器", "食品饮料", "纺织服装", "轻工制造", "医药生物", "公用事业",
                                 "交通运输", "房地产", "商业贸易", "休闲服务", "银行", "非银金融", "综合", "建筑材料", "建筑装饰", "电气设备", "机械设备", "国防军工",
                                 "计算机", "传媒", "通信", "农、林、渔、牧业", "采矿业", "制造业", "电力、热力、燃气及水生产和供应业", "建筑业", "批发和零售业", "交通运输、仓储和邮政业",
                                 "住宿和餐饮业", "信息传输、软件和信息技术服务业", "金融业", "房地产业", "租赁和商务服务业", "科学研究和技术服务业", "水利、环境和公共设施管理业", "教育",
                                 "卫生和社会工作", "文化、体育和娱乐业", "综合"), primary_key=True)
    scale = Column("scale", DECIMAL)
    proportion = Column("proportion", DECIMAL)
    asset_scale = Column("asset_scale", DECIMAL)


class FundPositionStock(mixin.TimeMixin, Base):
    __tablename__ = "fund_position_stock"
    __table_args__ = {"schema": "base_public"}

    fund_id = Column("fund_id", String, primary_key=True)
    fund_name = Column("fund_name", String)
    statistic_date = Column("statistic_date", Date, primary_key=True)
    data_source = Column("data_source", String)
    subject_id = Column("subject_id", String, primary_key=True)
    subject_name = Column("subject_name", String)
    quantity = Column("quantity", DECIMAL)
    scale = Column("scale", DECIMAL)
    asset_scale = Column("asset_scale", DECIMAL)
    proportion_net = Column("proportion_net", DECIMAL)
    proportion_mv = Column("proportion_mv", DECIMAL)
    proportion_float = Column("proportion_float", DECIMAL)


class FundPositionBond(mixin.TimeMixin, Base):
    __tablename__ = "fund_position_bond"
    __table_args__ = {"schema": "base_public"}

    fund_id = Column("fund_id", String, primary_key=True)
    fund_name = Column("fund_name", String)
    statistic_date = Column("statistic_date", Date, primary_key=True)
    data_source = Column("data_source", String)
    subject_id = Column("subject_id", String, primary_key=True)
    subject_name = Column("subject_name", String)
    quantity = Column("quantity", DECIMAL)
    scale = Column("scale", DECIMAL)
    asset_scale = Column("asset_scale", DECIMAL)
    proportion_net = Column("proportion_net", DECIMAL)
    proportion_mv = Column("proportion_mv", DECIMAL)
    proportion_float = Column("proportion_float", DECIMAL)


class FundIncome(mixin.TimeMixin, Base):
    __tablename__ = "fund_income"
    __table_args__ = {"schema": "base_public"}

    fund_id = Column("fund_id", String, primary_key=True)
    fund_name = Column("fund_name", String)
    statistic_date = Column("statistic_date", Date, primary_key=True)
    data_source = Column("data_source", String)
    total_income = Column("total_income", Numeric)
    interest_revenue = Column("interest_revenue", Numeric)
    deposit_interest = Column("deposit_interest", Numeric)
    bonds_interest = Column("bonds_interest", Numeric)
    buying_back_income = Column("buying_back_income", Numeric)
    abs_interest = Column("abs_interest", Numeric)
    investment_income = Column("investment_income", Numeric)
    stock_income = Column("stock_income", Numeric)
    bonds_income = Column("bonds_income", Numeric)
    abs_income = Column("abs_income", Numeric)
    derivatives_income = Column("derivatives_income", Numeric)
    dividend_income = Column("dividend_income", Numeric)
    changes_in_fair_value = Column("changes_in_fair_value", Numeric)
    other_income = Column("other_income", Numeric)
    total_expense = Column("total_expense", Numeric)
    org_compensation = Column("org_compensation", Numeric)
    trustee_expense = Column("trustee_expense", Numeric)
    sales_service_expense = Column("sales_service_expense", Numeric)
    transaction_expense = Column("transaction_expense", Numeric)
    interest_payment = Column("interest_payment", Numeric)
    sold_repurchaseme_payment = Column("sold_repurchase_payment", Numeric)
    other_expense = Column("other_expense", Numeric)
    total_profit = Column("total_profit", Numeric)


class FundAnnouncement(mixin.TimeMixin, mixin.GeneralMetaMixin, Base):
    __tablename__ = "fund_announcement"
    __table_args__ = {"schema": "base_public"}

    fund_id = Column("fund_id", String, primary_key=True)
    fund_name = Column("fund_name", String)
    announcement_id = Column("announcement_id", String)
    announcement_name = Column("announcement_name", String)
    date = Column("date", Date)
    type = Column("type", String)
    content = Column("content", String)


class FundBalance(mixin.TimeMixin, Base):
    __tablename__ = "fund_balance"
    __table_args__ = {"schema": "base_public"}

    fund_id = Column("fund_id", String, primary_key=True)
    fund_name = Column("fund_name", String)
    statistic_date = Column("statistic_date", Date, primary_key=True)
    data_source = Column("data_source", String)
    bank_deposit = Column("bank_deposit", Numeric)
    provision_settlement_fund = Column("provision_settlement_fund", Numeric)
    refundable_deposits = Column("refundable_deposits", Numeric)
    transaction_monetary_assets = Column("transaction_monetary_assets", Numeric)
    stock_income = Column("stock_income", Numeric)
    bonds_income = Column("bonds_income", Numeric)
    abs_income = Column("abs_income", Numeric)
    derivatives_income = Column("derivatives_income", Numeric)
    buying_back_income = Column("buying_back_income", Numeric)
    securities_settlement_receivable = Column("securities_settlement_receivable", Numeric)
    interest_revenue = Column("interest_revenue", Numeric)
    dividend_income = Column("dividend_income", Numeric)
    subscription_receivable = Column("subscription_receivable", Numeric)
    other_assets = Column("other_assets", Numeric)
    total_assets = Column("total_assets", Numeric)
    short_term_loan = Column("short_term_loan", Numeric)
    transaction_financial_liabilities = Column("transaction_financial_liabilities", Numeric)
    derivative_financial_liabilities = Column("derivative_financial_liabilities", Numeric)
    sold_repurchase_payment = Column("sold_repurchase_payment", Numeric)
    securities_settlement_payable = Column("securities_settlement_payable", Numeric)
    redemption_payable = Column("redemption_payable", Numeric)
    org_compensation_payable = Column("org_compensation_payable", Numeric)
    trustee_payable = Column("trustee_payable", Numeric)
    sales_service_payable = Column("sales_service_payable", Numeric)
    transaction_payable = Column("transaction_payable", Numeric)
    tax_payable = Column("tax_payable", Numeric)
    interest_payable = Column("interest_payable", Numeric)
    profit_payable = Column("profit_payable", Numeric)
    other_liabilities = Column("other_liabilities", Numeric)
    total_liabilities = Column("total_liabilities", Numeric)
    paid_up_capital = Column("paid_up_capital", Numeric)
    undistributed_profit = Column("undistributed_profit", Numeric)
    owner_equity = Column("owner_equity", Numeric)
    total_liabilities_and_owners_equity = Column("total_liabilities_and_owners_equity", Numeric)


class FundAssetScale(mixin.MultiSourceMixin, mixin.TimeMixin, Base):
    __tablename__ = "fund_asset_scale"
    __table_args__ = {"schema": "base_public"}

    fund_id = Column("fund_id", String, primary_key=True)
    fund_name = Column("fund_name", String)
    statistic_date = Column("statistic_date", Date)
    data_source = Column("data_source", String)
    purchase_amount = Column("purchase_amount", DECIMAL)
    redemption_amount = Column("redemption_amount", DECIMAL)
    total_asset = Column("total_asset", DECIMAL)
    total_share = Column("total_share", DECIMAL)


class FundHolder(mixin.TimeMixin, Base):
    __tablename__ = "fund_holder"
    __table_args__ = {"schema": "base_public"}

    fund_id = Column("fund_id", String, primary_key=True)
    fund_name = Column("fund_name", String)
    statistic_date = Column("statistic_date", Date, primary_key=True)
    data_source = Column("data_source", String)
    holder_type = Column("holder_type", String, primary_key=True)
    proportion_held = Column("proportion_held", Integer)
    share_held = Column("share_held", String)
    holder_num = Column("holder_num", String)
    total_share = Column("total_share", String)


class FundDescription(mixin.TimeMixin, Base):
    __tablename__ = "fund_description"
    __table_args__ = {"schema": "base_public"}

    fund_id = Column("fund_id", String, primary_key=True)
    fund_name = Column("fund_name", String)
    data_source = Column("data_source", String)
    investment_target = Column("investment_target", String)
    investment_scope = Column("investment_scope", String)
    investment_strategy = Column("investment_strategy", String)
    investment_idea = Column("investment_idea", String)
    income_distribution = Column("income_distribution", String)
    risk_return_character = Column("risk_return_character", String)
    comparison_criterion = Column("comparison_criterion", String)
    guarantee_institution = Column("guarantee_institution", String)
    guarantee_period = Column("guarantee_period", String)
    guarantee_way = Column("guarantee_way", String)
    tracking_benchmark = Column("tracking_benchmark", String)


class FundFee(mixin.TimeMixin, Base):
    __tablename__ = "fund_fee"
    __table_args__ = {"schema": "base_public"}

    fund_id = Column("fund_id", String, primary_key=True)
    fund_name = Column("fund_name", String)
    data_source = Column("data_source", String)
    fee_subscription = Column("fee_subscription", String)
    fee_purchase = Column("fee_purchase", String)
    fee_redeem = Column("fee_redeem", String)
    fee_management = Column("fee_management", DECIMAL)
    fee_trust = Column("fee_trust", DECIMAL)
    fee_service = Column("fee_service", DECIMAL)


class FundYield(mixin.TimeMixin, Base):
    __tablename__ = "fund_yield"
    __table_args__ = {"schema": "base_public"}

    fund_id = Column("fund_id", String, primary_key=True)
    fund_name = Column("fund_name", String)
    data_source = Column("data_source", String)
    statistic_date = Column("statistic_date", Date, primary_key=True)
    return_10k = Column("return_10k", Numeric, primary_key=True)
    d7_return_a = Column("d7_return_a", Numeric, primary_key=True)


class OrgInfo(mixin.TimeMixin, Base):
    __tablename__ = "org_info"
    __table_args__ = {"schema": "base_public"}

    org_id = Column("org_id", String, primary_key=True)
    org_name = Column("org_name", String)
    data_source = Column("data_source", String)
    org_name_en = Column("org_name_en", String)
    org_full_name = Column("org_full_name", String)
    org_type_code = Column("org_type_code", Integer)
    org_type = Column("org_type", String)
    foundation_date = Column("foundation_date", Date)
    form = Column("form", String)
    scale = Column("scale", String)
    scale_mgt = Column("scale_mgt", String)
    legal_person = Column("legal_person", String)
    chairman = Column("chairman", String)
    general_manager = Column("general_manager", String)
    reg_capital = Column("reg_capital", String)
    reg_address = Column("reg_address", String)
    address = Column("address", String)
    tel = Column("tel", String)
    fax = Column("fax", String)
    email = Column("email", String)
    website = Column("website", String)
    tel_service = Column("tel_service", String)


class OrgAssetScale(mixin.TimeMixin, Base):
    __tablename__ = "org_asset_scale"
    __table_args__ = {"schema": "base_public"}

    org_id = Column("org_id", String, primary_key=True)
    org_name = Column("org_name", String)
    data_source = Column("data_source", String)
    statistic_date = Column("statistic_date", Date, primary_key=True)
    total_asset = Column("total_asset", String)
    funds_num = Column("funds_num", Integer)


class OrgPortfolioAsset(mixin.TimeMixin, Base):
    __tablename__ = "org_portfolio_asset"
    __table_args__ = {"schema": "base_public"}

    org_id = Column("org_id", String, primary_key=True)
    org_name = Column("org_name", String)
    data_source = Column("data_source", String)
    statistic_date = Column("statistic_date", Date, primary_key=True)
    asset_type = Column("asset_type", Enum("买入返售金融资产", "固定收益投资", "其他", "基金投资", "权益投资", "金融衍生品投资", "货币市场工具", "现金"), primary_key=True)
    asset_stype = Column("asset_stype", Enum("买入返售金融资产", "债券", "其他", "基金", "存托凭证", "权证", "股票", "货币市场工具", "资产支持证券", "现金", ""), primary_key=True)
    scale = Column("scale", String)
    proportion = Column("proportion", DECIMAL)
    asset_scale = Column("asset_scale", DECIMAL)


class OrgPortfolioIndustry(mixin.TimeMixin, Base):
    __tablename__ = "org_portfolio_industry"
    __table_args__ = {"schema": "base_public"}

    org_id = Column("org_id", String, primary_key=True)
    org_name = Column("org_name", String)
    data_source = Column("data_source", String)
    statistic_date = Column("statistic_date", Date, primary_key=True)
    style = Column("style", Integer, primary_key=True)
    type = Column("type", Enum("农林牧渔", "采掘", "化工", "钢铁", "有色金属", "电子", "汽车", "家用电器", "食品饮料", "纺织服装", "轻工制造", "医药生物", "公用事业",
                                 "交通运输", "房地产", "商业贸易", "休闲服务", "银行", "非银金融", "综合", "建筑材料", "建筑装饰", "电气设备", "机械设备", "国防军工",
                                 "计算机", "传媒", "通信", "农、林、渔、牧业", "采矿业", "制造业", "电力、热力、燃气及水生产和供应业", "建筑业", "批发和零售业", "交通运输、仓储和邮政业",
                                 "住宿和餐饮业", "信息传输、软件和信息技术服务业", "金融业", "房地产业", "租赁和商务服务业", "科学研究和技术服务业", "水利、环境和公共设施管理业", "教育",
                                 "卫生和社会工作", "文化、体育和娱乐业", "综合"), primary_key=True)
    scale = Column("scale", DECIMAL)
    proportion = Column("proportion", DECIMAL)
    asset_scale = Column("asset_scale", DECIMAL)


class OrgPositionStock(mixin.TimeMixin, Base):
    __tablename__ = "org_position_stock"
    __table_args__ = {"schema": "base_public"}

    org_id = Column("org_id", String, primary_key=True)
    org_name = Column("org_name", String)
    data_source = Column("data_source", String)
    statistic_date = Column("statistic_date", Date, primary_key=True)
    subject_id = Column("subject_id", String, primary_key=True)
    subject_name = Column("subject_name", String)
    scale = Column("scale", DECIMAL)
    proportion = Column("proportion", DECIMAL)
    quantity = Column("quantity", DECIMAL)
    asset_scale = Column("asset_scale", DECIMAL)


class OrgHolder(mixin.TimeMixin, Base):
    __tablename__ = "org_holder"
    __table_args__ = {"schema": "base_public"}

    org_id = Column("org_id", String, primary_key=True)
    org_name = Column("org_name", String)
    statistic_date = Column("statistic_date", Date, primary_key=True)
    data_source = Column("data_source", String)
    holder_type = Column("holder_type", Enum("机构", "个人", "内部"), primary_key=True)
    proportion_held = Column("proportion_held", Integer)
    share_held = Column("share_held", String)
    holder_num = Column("holder_num", String)
    total_scale = Column("total_scale", String)


class OrgShareholder(mixin.TimeMixin, Base):
    __tablename__ = "org_shareholder"
    __table_args__ = {"schema": "base_public"}

    org_id = Column("org_id", String, primary_key=True)
    org_name = Column("org_name", String)
    statistic_date = Column("statistic_date", Date, primary_key=True)
    data_source = Column("data_source", String)
    shareholder_name = Column("shareholder_name", String, primary_key=True)
    shareholder_num = Column("shareholder_num", Integer)
    capital_stock = Column("capital_stock", DECIMAL, primary_key=True)
    stock_held = Column("stock_held", DECIMAL)
    proportion_held = Column("proportion_held", DECIMAL)


class PersonInfo(mixin.MultiSourceMixin, mixin.TimeMixin, Base):
    __tablename__ = "person_info"
    __table_args__ = {"schema": "base_public"}

    person_id = Column("person_id", String, primary_key=True)
    person_name = Column("person_name", String)
    data_source = Column("data_source", String, primary_key=True)
    master_strategy = Column("master_strategy", String)
    gender = Column("gender", String)
    education = Column("education", String)
    investment_period = Column("investment_period", String)
    resume = Column("resume", String)


class FundOrgMapping(mixin.TimeMixin, Base):
    __tablename__ = "fund_org_mapping"
    __table_args__ = {"schema": "crawl_public"}

    org_id = Column("org_id", String, primary_key=True)
    org_name = Column("org_name", String)
    data_source = Column("data_source", String)
    fund_id = Column("fund_id", String, primary_key=True)
    fund_name = Column("fund_name", String)


class OrgPersonMapping(mixin.TimeMixin, Base):
    __tablename__ = "org_person_mapping"
    __table_args__ = {"schema": "base_public"}

    org_id = Column("org_id", String, primary_key=True)
    org_name = Column("org_name", String)
    person_id = Column("person_id", String, primary_key=True)
    person_name = Column("person_name", String)
    duty = Column("duty", String)
    duty_detail = Column("duty_detail", String)
    tenure_date = Column("tenure_date", Date)
    dimission_date = Column("dimission_date", Date)
    tenure_period = Column("tenure_period", Integer)
    is_current = Column("is_current", Integer)


class FundTypeMappingSource(mixin.TimeMixin, Base):
    __tablename__ = "fund_type_mapping_source"
    __table_args__ = {"schema": "base_public"}

    fund_id = Column("fund_id", String, primary_key=True)
    fund_name = Column("fund_name", String)
    data_source = Column("data_source", String, primary_key=True)
    typestandard_code = Column("typestandard_code", String, primary_key=True)
    typestandard_name = Column("typestandard_name", String)
    type_code = Column("type_code", String)
    type_name = Column("type_name", String)
    stype_code = Column("stype_code", String)
    stype_name = Column("stype_name", String)


class FundTypeMapping(mixin.TimeMixin, Base):
    __tablename__ = "fund_type_mapping"
    __table_args__ = {"schema": "base_public"}

    fund_id = Column("fund_id", String, primary_key=True)
    fund_name = Column("fund_name", String)
    data_source = Column("data_source", String)
    typestandard_code = Column("typestandard_code", String, primary_key=True)
    typestandard_name = Column("typestandard_name", String)
    type_code = Column("type_code", String)
    type_name = Column("type_name", String)
    stype_code = Column("stype_code", String)
    stype_name = Column("stype_name", String)
