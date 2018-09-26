from sqlalchemy import Column, String, Date, DECIMAL, Integer, Numeric
from sqlalchemy.ext.declarative import declarative_base
from utils.database.models import mixin


Base = declarative_base()


class DFundInfo(mixin.GeneralMetaMixin, mixin.MultiSourceMixin, mixin.TimeMixin, Base):
    __tablename__ = "d_fund_info"
    __table_args__ = {"schema": "crawl_public"}

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


class IFundInfo(mixin.GeneralMetaMixin, mixin.MultiSourceMixin, mixin.TimeMixin, Base):
    __tablename__ = "i_fund_info"
    __table_args__ = {"schema": "crawl_public"}

    fund_id = Column("fund_id", String)
    fund_name = Column("fund_name", String)
    fund_full_name = Column("fund_full_name", String)


class DFundInfoStructured(mixin.GeneralMetaMixin, mixin.MultiSourceMixin, mixin.TimeMixin, Base):
    __tablename__ = "d_fund_info_structured"
    __table_args__ = {"schema": "crawl_public"}

    fund_id = Column("fund_id", String, primary_key=True)
    fund_name = Column("fund_name", String)
    fund_full_name = Column("fund_full_name", String)
    portfolio = Column("portfolio", String)
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


class DFundNv(mixin.GeneralMetaMixin, mixin.MultiSourceMixin, mixin.TimeMixin, Base):
    __tablename__ = "d_fund_nv"
    __table_args__ = {"schema": "crawl_public"}

    fund_id = Column("fund_id", String, primary_key=True)
    fund_name = Column("fund_name", String)
    statistic_date = Column("statistic_date", Date, primary_key=True)
    nav = Column("nav", DECIMAL)
    added_nav = Column("added_nav", DECIMAL)
    swanav = Column("swanav", DECIMAL)


class DFundYield(mixin.GeneralMetaMixin, mixin.MultiSourceMixin, mixin.TimeMixin, Base):
    __tablename__ = "d_fund_yield"
    __table_args__ = {"schema": "crawl_public"}

    fund_id = Column("fund_id", String, primary_key=True)
    fund_name = Column("fund_name", String)
    statistic_date = Column("statistic_date", Date, primary_key=True)
    return_10k = Column("return_10k", Numeric, primary_key=True)
    d7_return_a = Column("d7_return_a", Numeric, primary_key=True)


class DFundDividend(mixin.MultiSourceMixin, mixin.TimeMixin, Base):
    __tablename__ = "d_fund_dividend"
    __table_args__ = {"schema": "crawl_public"}

    fund_id = Column("fund_id", String, primary_key=True)
    fund_name = Column("fund_name", String)
    statistic_date = Column("statistic_date", Date)
    record_date = Column("record_date", Date)
    ex_dividend_date = Column("ex_dividend_date", Date)
    dividend_date = Column("dividend_date", Date)
    dividend_at = Column("dividend_at", DECIMAL)
    is_used = Column("is_used", Numeric)


class DFundSplit(mixin.MultiSourceMixin, mixin.TimeMixin, Base):
    __tablename__ = "d_fund_split"
    __table_args__ = {"schema": "crawl_public"}

    fund_id = Column("fund_id", String, primary_key=True)
    fund_name = Column("fund_name", String)
    statistic_date = Column("statistic_date", Date)
    split_date = Column("split_date", Date)
    split_ratio = Column("split_ratio", DECIMAL)
    is_used = Column("is_used", Numeric)


class DFundAssetScale(mixin.MultiSourceMixin, mixin.TimeMixin, Base):
    __tablename__ = "d_fund_asset_scale"
    __table_args__ = {"schema": "crawl_public"}

    fund_id = Column("fund_id", String, primary_key=True)
    fund_name = Column("fund_name", String)
    data_source = Column("data_source", String)
    statistic_date = Column("statistic_date", Date)
    purchase_amount = Column("purchase_amount", String)
    redemption_amounnt = Column("redemption_amount", String)
    total_asset = Column("total_asset", String)
    total_share = Column("total_share", String)
    asset_change_ratio = Column("asset_change_ratio", String)
    remark = Column("remark", String)


class DFundDescription(mixin.MultiSourceMixin, mixin.TimeMixin, Base):
    __tablename__ = "d_fund_description"
    __table_args__ = {"schema": "crawl_public"}

    fund_id = Column("fund_id", String, primary_key=True)
    fund_name = Column("fund_name", String)
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


class DFundFee(mixin.MultiSourceMixin, mixin.TimeMixin, Base):
    __tablename__ = "d_fund_fee"
    __table_args__ = {"schema": "crawl_public"}

    fund_id = Column("fund_id", String, primary_key=True)
    fund_name = Column("fund_name", String)
    fee_subscription = Column("fee_subscription", String)
    fee_purchase = Column("fee_purchase", String)
    fee_redeem = Column("fee_redeem", String)
    fee_management = Column("fee_management", String)
    fee_trust = Column("fee_trust", String)
    fee_service = Column("fee_service", String)


class DFundPortfolioAsset(mixin.MultiSourceMixin, mixin.TimeMixin, Base):
    __tablename__ = "d_fund_portfolio_asset"
    __table_args__ = {"schema": "crawl_public"}

    fund_id = Column("fund_id", String, primary_key=True)
    fund_name = Column("fund_name", String)
    data_source = Column("data_source", String)
    statistic_date = Column("statistic_date", String)
    type = Column("type", String)
    scale = Column("scale", String)
    proportion = Column("proportion", String)
    change_ratio = Column("change_ratio", String)
    asset_scale = Column("asset_scale", String)


class DFundPortfolioIndustry(mixin.MultiSourceMixin, mixin.TimeMixin, Base):
    __tablename__ = "d_fund_portfolio_industry"
    __table_args__ = {"schema": "crawl_public"}

    fund_id = Column("fund_id", String, primary_key=True)
    fund_name = Column("fund_name", String)
    data_source = Column("data_source", String)
    statistic_date = Column("statistic_date", String)
    asset_type = Column("asset_type", String)
    type = Column("type", String)
    scale = Column("scale", String)
    proportion = Column("proportion", String)
    change_ratio = Column("change_ratio", String)
    asset_scale = Column("asset_scale", String)


class DFundHolder(mixin.MultiSourceMixin, mixin.TimeMixin, Base):
    __tablename__ = "d_fund_holder"
    __table_args__ = {"schema": "crawl_public"}

    fund_id = Column("fund_id", String, primary_key=True)
    fund_name = Column("fund_name", String)
    statistic_date = Column("statistic_date", Date, primary_key=True)
    holder_type = Column("holder_type", String, primary_key=True)
    proportion_held = Column("proportion_held", Integer)
    share_held = Column("share_held", String)
    holder_num = Column("holder_num", String)
    total_share = Column("total_share", String)


# class DFundDescription(mixin.MultiSourceMixin, mixin.TimeMixin, Base):
#     __tablename__ = "d_fund_description"
#     __table_args__ = {"schema": "crawl_public"}
#
#     fund_id = Column("fund_id", String, primary_key=True)
#     fund_name = Column("fund_name", String)
#     investment_target = Column("investment_target", String)
#     investment_scope = Column("investment_scope", String)
#     investment_strategy = Column("investment_strategy", String)
#     investment_idea = Column("investment_idea", String)
#     income_distribution = Column("income_distribution", String)
#     risk_return_character = Column("risk_return_character", String)
#     comparison_criterion = Column("comparison_criterion", String)


class DOrgInfo(mixin.MultiSourceMixin, mixin.TimeMixin, Base):
    __tablename__ = "d_org_info"
    __table_args__ = {"schema": "crawl_public"}

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


class DOrgAssetScale(mixin.MultiSourceMixin, mixin.TimeMixin, Base):
    __tablename__ = "d_org_asset_scale"
    __table_args__ = {"schema": "crawl_public"}

    org_id = Column("org_id", String, primary_key=True)
    org_name = Column("org_name", String)
    data_source = Column("data_source", String)
    statistic_date = Column("statistic_date", Date)
    total_asset = Column("total_asset", DECIMAL)
    funds_num = Column("funds_num", Integer)


class DOrgPortfolioAsset(mixin.MultiSourceMixin, mixin.TimeMixin, Base):
    __tablename__ = "d_org_portfolio_asset"
    __table_args__ = {"schema": "crawl_public"}

    org_id = Column("org_id", String, primary_key=True)
    org_name = Column("org_name", String)
    data_source = Column("data_source", String, primary_key=True)
    statistic_date = Column("statistic_date", Date, primary_key=True)
    asset_type = Column("asset_type", String, primary_key=True)
    scale = Column("scale", String)
    proportion = Column("proportion", DECIMAL)
    asset_scale = Column("asset_scale", DECIMAL)


class DOrgPortfolioIndustry(mixin.MultiSourceMixin, mixin.TimeMixin, Base):
    __tablename__ = "d_org_portfolio_industry"
    __table_args__ = {"schema": "crawl_public"}

    org_id = Column("org_id", String, primary_key=True)
    org_name = Column("org_name", String)
    data_source = Column("data_source", String, primary_key=True)
    statistic_date = Column("statistic_date", Date, primary_key=True)
    asset_type = Column("asset_type", String, primary_key=True)
    type = Column("type", String, primary_key=True)
    scale = Column("scale", String)
    proportion = Column("proportion", String)
    change_ratio = Column("change_ratio", String)
    asset_scale = Column("asset_scale", String)


class DOrgPosition(mixin.MultiSourceMixin, mixin.TimeMixin, Base):
    __tablename__ = "d_org_position"
    __table_args__ = {"schema": "crawl_public"}

    org_id = Column("org_id", String, primary_key=True)
    org_name = Column("org_name", String)
    data_source = Column("data_source", String, primary_key=True)
    statistic_date = Column("statistic_date", Date, primary_key=True)
    type = Column("type", String, primary_key=True)
    subject_id = Column("subject_id", String, primary_key=True)
    subject_name = Column("subject_name", String)
    scale = Column("scale", String)
    proportion = Column("proportion", String)
    quantity = Column("quantity", String)
    change_ratio = Column("change_ratio", String)
    asset_scale = Column("asset_scale", String)


class DFundPosition(mixin.MultiSourceMixin, mixin.TimeMixin, Base):
    __tablename__ = "d_fund_position"
    __table_args__ = {"schema": "crawl_public"}

    fund_id = Column("fund_id", String, primary_key=True)
    fund_name = Column("fund_name", String)
    statistic_date = Column("statistic_date", Date, primary_key=True)
    type = Column("type", String, primary_key=True)
    subject_id = Column("subject_id", String, primary_key=True)
    subject_name = Column("subject_name", String)
    scale = Column("scale", String)
    proportion = Column("proportion", String)
    quantity = Column("quantity", String)
    change_ratio = Column("change_ratio", String)
    asset_scale = Column("asset_scale", String)


class DFundIncome(mixin.MultiSourceMixin, mixin.TimeMixin, Base):
    __tablename__ = "d_fund_income"
    __table_args__ = {"schema": "crawl_public"}

    fund_id = Column("fund_id", String, primary_key=True)
    fund_name = Column("fund_name", String)
    statistic_date = Column("statistic_date", Date, primary_key=True)
    total_income = Column("total_income", String)
    interest_revenue = Column("interest_revenue", String)
    deposit_interest = Column("deposit_interest", String)
    bonds_interest = Column("bonds_interest", String)
    buying_back_income = Column("buying_back_income", String)
    abs_interest = Column("abs_interest", String)
    investment_income = Column("investment_income", String)
    stock_income = Column("stock_income", String)
    bonds_income = Column("bonds_income", String)
    abs_income = Column("abs_income", String)
    derivatives_income = Column("derivatives_income", String)
    dividend_income = Column("dividend_income", String)
    changes_in_fair_value = Column("changes_in_fair_value", String)
    other_income = Column("other_income", String)
    total_expense = Column("total_expense", String)
    org_compensation = Column("org_compensation", String)
    trustee_expense = Column("trustee_expense", String)
    sales_service_expense = Column("sales_service_expense", String)
    transaction_expense = Column("transaction_expense", String)
    interest_payment = Column("interest_payment", String)
    sold_repurchaseme_payment = Column("sold_repurchase_payment", String)
    other_expense = Column("other_expense", String)
    total_profit = Column("total_profit", String)


class DFundBalance(mixin.MultiSourceMixin, mixin.TimeMixin, Base):
    __tablename__ = "d_fund_balance"
    __table_args__ = {"schema": "crawl_public"}

    fund_id = Column("fund_id", String, primary_key=True)
    fund_name = Column("fund_name", String)
    statistic_date = Column("statistic_date", Date, primary_key=True)
    bank_deposit = Column("bank_deposit", String)
    provision_settlement_fund = Column("provision_settlement_fund", String)
    refundable_deposits = Column("refundable_deposits", String)
    transaction_monetary_assets = Column("transaction_monetary_assets", String)
    stock_income = Column("stock_income", String)
    bonds_income = Column("bonds_income", String)
    abs_income = Column("abs_income", String)
    derivatives_income = Column("derivatives_income", String)
    buying_back_income = Column("buying_back_income", String)
    securities_settlement_receivable = Column("securities_settlement_receivable", String)
    interest_revenue = Column("interest_revenue", String)
    dividend_income = Column("dividend_income", String)
    subscription_receivable = Column("subscription_receivable", String)
    other_assets = Column("other_assets", String)
    total_assets = Column("total_assets", String)
    short_term_loan = Column("short_term_loan", String)
    transaction_financial_liabilities = Column("transaction_financial_liabilities", String)
    derivative_financial_liabilities = Column("derivative_financial_liabilities", String)
    sold_repurchase_payment = Column("sold_repurchase_payment", String)
    securities_settlement_payable = Column("securities_settlement_payable", String)
    redemption_payable = Column("redemption_payable", String)
    org_compensation_payable = Column("org_compensation_payable", String)
    trustee_payable = Column("trustee_payable", String)
    sales_service_payable = Column("sales_service_payable", String)
    transaction_payable = Column("transaction_payable", String)
    tax_payable = Column("tax_payable", String)
    interest_payable = Column("interest_payable", String)
    profit_payable = Column("profit_payable", String)
    other_liabilities = Column("other_liabilities", String)
    total_liabilities = Column("total_liabilities", String)
    paid_up_capital = Column("paid_up_capital", String)
    undistributed_profit = Column("undistributed_profit", String)
    owner_equity = Column("owner_equity", String)
    total_liabilities_and_owners_equity = Column("total_liabilities_and_owners_equity", String)


class DFundAnnouncement(mixin.MultiSourceMixin, mixin.TimeMixin, mixin.GeneralMetaMixin, Base):
    __tablename__ = "d_fund_announcement"
    __table_args__ = {"schema": "crawl_public"}

    fund_id = Column("fund_id", String, primary_key=True)
    fund_name = Column("fund_name", String)
    announcement_id = Column("announcement_id", String)
    announcement_name = Column("announcement_name", String)
    date = Column("date", Date)
    type = Column("type", String)
    content = Column("content", String)


class DOrgShareholder(mixin.MultiSourceMixin, mixin.TimeMixin, Base):
    __tablename__ = "d_org_shareholder"
    __table_args__ = {"schema": "crawl_public"}

    org_id = Column("org_id", String, primary_key=True)
    org_name = Column("org_name", String)
    statistic_date = Column("statistic_date", Date, primary_key=True)
    shareholder_name = Column("shareholder_name", String, primary_key=True)
    shareholder_num = Column("shareholder_num", Integer)
    capital_stock = Column("capital_stock", String, primary_key=True)
    stock_held = Column("stock_held", String)
    proportion_held = Column("proportion_held", String)


class DOrgHolder(mixin.MultiSourceMixin, mixin.TimeMixin, Base):
    __tablename__ = "d_org_holder"
    __table_args__ = {"schema": "crawl_public"}

    org_id = Column("org_id", String, primary_key=True)
    org_name = Column("org_name", String)
    statistic_date = Column("statistic_date", Date, primary_key=True)
    holder_type = Column("holder_type", String, primary_key=True)
    proportion_held = Column("proportion_held", Integer)
    share_held = Column("share_held", String)
    holder_num = Column("holder_num", String)
    total_scale = Column("total_scale", String)


class DOrgFund(mixin.MultiSourceMixin, mixin.TimeMixin, Base):
    __tablename__ = "d_org_fund"
    __table_args__ = {"schema": "crawl_public"}

    org_id = Column("org_id", String, primary_key=True)
    org_name = Column("org_name", String)
    fund_id = Column("fund_id", String, primary_key=True)
    fund_name = Column("fund_name", String)


class DOrgPerson(mixin.MultiSourceMixin, mixin.TimeMixin, Base):
    __tablename__ = "d_org_person"
    __table_args__ = {"schema": "crawl_public"}

    org_id =  Column("org_id", String, primary_key=True)
    org_name = Column("org_name", String)
    person_id = Column("person_id", String, primary_key=True)
    person_name = Column("person_name", String)
    duty = Column("duty", String)
    tenure_date = Column("tenure_date", Date)
    dimission_date = Column("dimission_date", Date)
    is_current = Column("is_current", Integer)


class DPersonInfo(mixin.MultiSourceMixin, mixin.TimeMixin, Base):
    __tablename__ = "d_person_info"
    __table_args__ = {"schema": "crawl_public"}

    person_id = Column("person_id", String, primary_key=True)
    person_name = Column("person_name", String)
    master_strategy = Column("master_strategy", String)
    gender = Column("gender", String)
    education = Column("education", Date)
    investment_period = Column("investment_period", Date)


class DPersonFund(mixin.MultiSourceMixin, mixin.TimeMixin, Base):
    __tablename__ = "d_person_fund"
    __table_args__ = {"schema": "crawl_public"}

    person_id = Column("person_id", String, primary_key=True)
    person_name = Column("person_name", String)
    fund_id = Column("fund_id", String, primary_key=True)
    fund_name = Column("fund_name", String)
    tenure_date = Column("tenure_date", String)
    dimission_date = Column("dimission_date", Date)
    tenure_period = Column("tenure_period", Date)
    is_current = Column("is_current", Date)
    is_used = Column("is_used", Integer)


class DPersonDescription(mixin.MultiSourceMixin, mixin.TimeMixin, Base):
    __tablename__ = "d_person_description"
    __table_args__ = {"schema": "crawl_public"}

    person_id = Column("person_id", String, primary_key=True)
    person_name = Column("person_name", String)
    introduction = Column("introduction", String)
    awarding = Column("awarding", String)
