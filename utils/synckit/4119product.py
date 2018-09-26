from importlib import reload
import datetime as dt
from sqlalchemy import create_engine
from utils.database import config as cfg
from utils.synckit.core import common
from utils.synckit.mysqlreader import splitter
from dateutil.relativedelta import relativedelta

reload(splitter)
reload(common)

ENGINE_RD = cfg.load_engine()["2Gb"]
ENGINE_4119PRODUCT = cfg.load_engine()["4Gp"]
# ENGINE_4119PRODUCT = create_engine("mysql+pymysql://jr_sync_yu:jr_sync_yu@182.254.128.241:4119/product",
#                                     connect_args={"charset": "utf8"})

tasks = [
    (splitter.MysqlReader("fund_info", ENGINE_RD), splitter.MysqlReader("fund_info", ENGINE_4119PRODUCT)),
    (splitter.MysqlReader("fund_asset_scale", ENGINE_RD), splitter.MysqlReader("fund_asset_scale", ENGINE_4119PRODUCT)),
    (splitter.MysqlReader("fund_allocation_data", ENGINE_RD), splitter.MysqlReader("fund_allocation_data", ENGINE_4119PRODUCT)),
    (splitter.MysqlReader("fund_fee_data", ENGINE_RD), splitter.MysqlReader("fund_fee_data", ENGINE_4119PRODUCT)),
    (splitter.MysqlReader("fund_manager_mapping", ENGINE_RD), splitter.MysqlReader("fund_manager_mapping", ENGINE_4119PRODUCT)),
    (splitter.MysqlReader("fund_org_mapping", ENGINE_RD), splitter.MysqlReader("fund_org_mapping", ENGINE_4119PRODUCT)),
    (splitter.MysqlReader("fund_portfolio", ENGINE_RD), splitter.MysqlReader("fund_portfolio", ENGINE_4119PRODUCT)),
    (splitter.MysqlReader("fund_security_data", ENGINE_RD), splitter.MysqlReader("fund_security_data", ENGINE_4119PRODUCT)),
    (splitter.MysqlReader("fund_type_mapping", ENGINE_RD), splitter.MysqlReader("fund_type_mapping", ENGINE_4119PRODUCT)),
    (splitter.MysqlReader("fund_weekly_index_static", ENGINE_RD), splitter.MysqlReader("fund_weekly_index", ENGINE_4119PRODUCT)),
    (splitter.MysqlReader("fund_month_index_static", ENGINE_RD), splitter.MysqlReader("fund_month_index", ENGINE_4119PRODUCT)),
    (splitter.MysqlReader("index_monthly_return", ENGINE_RD), splitter.MysqlReader("index_monthly_return", ENGINE_4119PRODUCT)),
    (splitter.MysqlReader("index_monthly_risk", ENGINE_RD), splitter.MysqlReader("index_monthly_risk", ENGINE_4119PRODUCT)),
    (splitter.MysqlReader("index_monthly_subsidiary", ENGINE_RD), splitter.MysqlReader("index_monthly_subsidiary", ENGINE_4119PRODUCT)),
    (splitter.MysqlReader("index_weekly_return", ENGINE_RD), splitter.MysqlReader("index_weekly_return", ENGINE_4119PRODUCT)),
    (splitter.MysqlReader("index_weekly_risk", ENGINE_RD), splitter.MysqlReader("index_weekly_risk", ENGINE_4119PRODUCT)),
    (splitter.MysqlReader("index_weekly_subsidiary", ENGINE_RD), splitter.MysqlReader("index_weekly_subsidiary", ENGINE_4119PRODUCT)),
    (splitter.MysqlReader("manager_info", ENGINE_RD), splitter.MysqlReader("manager_info", ENGINE_4119PRODUCT)),
    (splitter.MysqlReader("manager_resume", ENGINE_RD), splitter.MysqlReader("manager_resume", ENGINE_4119PRODUCT)),
    (splitter.MysqlReader("market_index", ENGINE_RD), splitter.MysqlReader("market_index", ENGINE_4119PRODUCT)),
    (splitter.MysqlReader("org_executive_info", ENGINE_RD), splitter.MysqlReader("org_executive_info", ENGINE_4119PRODUCT)),
    (splitter.MysqlReader("org_info", ENGINE_RD), splitter.MysqlReader("org_info", ENGINE_4119PRODUCT)),
    (splitter.MysqlReader("fund_nv_standard_w", ENGINE_RD), splitter.MysqlReader("fund_nv_standard_w", ENGINE_4119PRODUCT)),
    (splitter.MysqlReader("fund_nv_standard_m", ENGINE_RD), splitter.MysqlReader("fund_nv_standard_m", ENGINE_4119PRODUCT)),
    (splitter.MysqlReader("fund_nv_data_standard", ENGINE_RD), splitter.MysqlReader("fund_nv_data_standard", ENGINE_4119PRODUCT)),
    (splitter.MysqlReader("org_month_research", ENGINE_RD), splitter.MysqlReader("org_month_research", ENGINE_4119PRODUCT)),
    (splitter.MysqlReader("org_month_return", ENGINE_RD), splitter.MysqlReader("org_month_return", ENGINE_4119PRODUCT)),
    (splitter.MysqlReader("org_month_risk", ENGINE_RD), splitter.MysqlReader("org_month_risk", ENGINE_4119PRODUCT)),
    (splitter.MysqlReader("org_month_routine", ENGINE_RD), splitter.MysqlReader("org_month_routine", ENGINE_4119PRODUCT)),
    (splitter.MysqlReader("fund_weekly_return", ENGINE_RD), splitter.MysqlReader("fund_weekly_return", ENGINE_4119PRODUCT)),
    (splitter.MysqlReader("fund_weekly_risk", ENGINE_RD), splitter.MysqlReader("fund_weekly_risk", ENGINE_4119PRODUCT)),
    (splitter.MysqlReader("fund_weekly_risk2", ENGINE_RD), splitter.MysqlReader("fund_weekly_risk2", ENGINE_4119PRODUCT)),
    (splitter.MysqlReader("fund_subsidiary_weekly_index", ENGINE_RD), splitter.MysqlReader("fund_weekly_risk", ENGINE_4119PRODUCT)),
    (splitter.MysqlReader("fund_subsidiary_weekly_index2", ENGINE_RD), splitter.MysqlReader("fund_subsidiary_weekly_index2", ENGINE_4119PRODUCT)),
    (splitter.MysqlReader("fund_subsidiary_weekly_index3", ENGINE_RD), splitter.MysqlReader("fund_subsidiary_weekly_index3", ENGINE_4119PRODUCT)),
    (splitter.MysqlReader("fund_month_return", ENGINE_RD), splitter.MysqlReader("fund_month_return", ENGINE_4119PRODUCT)),
    (splitter.MysqlReader("fund_month_risk", ENGINE_RD), splitter.MysqlReader("fund_month_risk", ENGINE_4119PRODUCT)),
    (splitter.MysqlReader("fund_month_risk2", ENGINE_RD), splitter.MysqlReader("fund_month_risk2", ENGINE_4119PRODUCT)),
    (splitter.MysqlReader("fund_subsidiary_month_index", ENGINE_RD), splitter.MysqlReader("fund_month_risk", ENGINE_4119PRODUCT)),
    (splitter.MysqlReader("fund_subsidiary_month_index2", ENGINE_RD), splitter.MysqlReader("fund_subsidiary_month_index2", ENGINE_4119PRODUCT)),
    (splitter.MysqlReader("fund_subsidiary_month_index3", ENGINE_RD), splitter.MysqlReader("fund_subsidiary_month_index3", ENGINE_4119PRODUCT)),
]


def main():
    WHERE = "WHERE update_time BETWEEN '{t0}' AND '{t1}'".format(
        t0=(dt.datetime.now() - relativedelta(hours=1, minutes=5)).strftime("%Y%m%d%H%M%S"),
        t1=dt.datetime.now().strftime("%Y%m%d%H%M%S")
    )
    for TASK_NO in range(0, len(tasks)):
        print("task_no: {no}; task: {src} --> {tgt}".format(no=TASK_NO, src=tasks[TASK_NO][0]._name, tgt=tasks[TASK_NO][1]._name))
        try:
            if tasks[TASK_NO][0]._name in (
                    "fund_weekly_return", "fund_weekly_risk", "fund_subsidiary_weekly_index",
                    "fund_month_return", "fund_month_risk", "fund_subsidiary_month_index",
                    "fund_month_risk2", "fund_weekly_risk2", "fund_subsidiary_weekly_index2",
                    "fund_subsidiary_weekly_index3", "fund_subsidiary_month_index2", "fund_subsidiary_month_index3",
            ):
                j = splitter.Job(
                    tasks[TASK_NO][0], tasks[TASK_NO][1], pool_size={"update": 5}, where=WHERE,
                    where_del="WHERE fund_id LIKE 'JR%%'"
                )
                j.update()

            else:
                j = splitter.Job(tasks[TASK_NO][0], tasks[TASK_NO][1], pool_size={"update": 5}, where=WHERE)
                j.sync()

        except Exception as e:
            print(TASK_NO, e)

if __name__ == "__main__":
    main()
