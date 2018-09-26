from importlib import reload
import datetime as dt
from utils.synckit.core import common
from utils.synckit.mysqlreader import splitter
from sqlalchemy import create_engine
reload(splitter)
reload(common)

from utils.database import config as cfg
ENGINE_RD = cfg.load_engine()["2Gbp"]
ENGINE_4119PRODUCT = cfg.load_engine()["4Gpp"]

# ENGINE_RD = create_engine("mysql+pymysql://jr_sync_yu:jr_sync_yu@182.254.128.241:4171/base_public",
#                                     connect_args={"charset": "utf8"})
# ENGINE_4119PRODUCT = create_engine("mysql+pymysql://jr_sync_yu:jr_sync_yu@182.254.128.241:4119/product_mutual",
#                                     connect_args={"charset": "utf8"})

tasks = [
    (splitter.MysqlReader("fund_asset_scale", ENGINE_RD), splitter.MysqlReader("fund_asset_scale", ENGINE_4119PRODUCT)),
    (splitter.MysqlReader("fund_balance", ENGINE_RD), splitter.MysqlReader("fund_balance", ENGINE_4119PRODUCT)),
    (splitter.MysqlReader("fund_description", ENGINE_RD), splitter.MysqlReader("fund_description", ENGINE_4119PRODUCT)),
    (splitter.MysqlReader("fund_dividend", ENGINE_RD), splitter.MysqlReader("fund_dividend", ENGINE_4119PRODUCT)),
    (splitter.MysqlReader("fund_dividend_split", ENGINE_RD), splitter.MysqlReader("fund_dividend_split", ENGINE_4119PRODUCT)),
    (splitter.MysqlReader("fund_fee", ENGINE_RD), splitter.MysqlReader("fund_fee", ENGINE_4119PRODUCT)),
    (splitter.MysqlReader("fund_holder", ENGINE_RD), splitter.MysqlReader("fund_holder", ENGINE_4119PRODUCT)),
    (splitter.MysqlReader("fund_income", ENGINE_RD), splitter.MysqlReader("fund_income", ENGINE_4119PRODUCT)),
    (splitter.MysqlReader("fund_info", ENGINE_RD), splitter.MysqlReader("fund_info", ENGINE_4119PRODUCT)),
    (splitter.MysqlReader("fund_info_structured", ENGINE_RD), splitter.MysqlReader("fund_info_structured", ENGINE_4119PRODUCT)),
    (splitter.MysqlReader("fund_manager_mapping", ENGINE_RD), splitter.MysqlReader("fund_manager_mapping", ENGINE_4119PRODUCT)),
    (splitter.MysqlReader("fund_nv", ENGINE_RD), splitter.MysqlReader("fund_nv", ENGINE_4119PRODUCT)),
    (splitter.MysqlReader("fund_nv_source", ENGINE_RD), splitter.MysqlReader("fund_nv_source", ENGINE_4119PRODUCT)),
    (splitter.MysqlReader("fund_org_mapping", ENGINE_RD), splitter.MysqlReader("fund_org_mapping", ENGINE_4119PRODUCT)),
    (splitter.MysqlReader("fund_portfolio_asset", ENGINE_RD), splitter.MysqlReader("fund_portfolio_asset", ENGINE_4119PRODUCT)),
    (splitter.MysqlReader("fund_portfolio_industry", ENGINE_RD), splitter.MysqlReader("fund_portfolio_industry", ENGINE_4119PRODUCT)),
    (splitter.MysqlReader("fund_position_bond", ENGINE_RD), splitter.MysqlReader("fund_position_bond", ENGINE_4119PRODUCT)),
    (splitter.MysqlReader("fund_position_stock", ENGINE_RD), splitter.MysqlReader("fund_position_stock", ENGINE_4119PRODUCT)),
    (splitter.MysqlReader("fund_split", ENGINE_RD), splitter.MysqlReader("fund_split", ENGINE_4119PRODUCT)),
    (splitter.MysqlReader("fund_type_mapping", ENGINE_RD), splitter.MysqlReader("fund_type_mapping", ENGINE_4119PRODUCT)),
    (splitter.MysqlReader("fund_yield", ENGINE_RD), splitter.MysqlReader("fund_yield", ENGINE_4119PRODUCT)),
    (splitter.MysqlReader("org_asset_scale", ENGINE_RD), splitter.MysqlReader("org_asset_scale", ENGINE_4119PRODUCT)),
    (splitter.MysqlReader("org_holder", ENGINE_RD), splitter.MysqlReader("org_holder", ENGINE_4119PRODUCT)),
    (splitter.MysqlReader("org_info", ENGINE_RD), splitter.MysqlReader("org_info", ENGINE_4119PRODUCT)),
    (splitter.MysqlReader("org_person_mapping", ENGINE_RD), splitter.MysqlReader("org_person_mapping", ENGINE_4119PRODUCT)),
    (splitter.MysqlReader("org_portfolio_asset", ENGINE_RD), splitter.MysqlReader("org_portfolio_asset", ENGINE_4119PRODUCT)),
    (splitter.MysqlReader("org_portfolio_industry", ENGINE_RD), splitter.MysqlReader("org_portfolio_industry", ENGINE_4119PRODUCT)),
    (splitter.MysqlReader("org_position_stock", ENGINE_RD), splitter.MysqlReader("org_position_stock", ENGINE_4119PRODUCT)),
    (splitter.MysqlReader("org_shareholder", ENGINE_RD), splitter.MysqlReader("org_shareholder", ENGINE_4119PRODUCT)),
    (splitter.MysqlReader("person_info", ENGINE_RD), splitter.MysqlReader("person_info", ENGINE_4119PRODUCT)),
]


def main():
    WHERE = "WHERE update_time BETWEEN '{t0}' AND '{t1}'".format(
        t0=(dt.datetime.today() - dt.timedelta(1)).strftime("%Y%m%d"),
        t1=dt.datetime.today().strftime("%Y%m%d")
   )
    for TASK_NO in range(0, len(tasks)):
        print("task_no: {no}; task: {src} --> {tgt}".format(no=TASK_NO, src=tasks[TASK_NO][0]._name, tgt=tasks[TASK_NO][1]._name))
        try:
            j = splitter.Job(tasks[TASK_NO][0], tasks[TASK_NO][1], pool_size={"update": 5}, where=WHERE)
            j.sync()

        except Exception as e:
            print(TASK_NO, e)

if __name__ == "__main__":
    main()
