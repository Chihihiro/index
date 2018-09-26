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
ENGINE_8612GT = cfg.load_engine()["8612test_sync"]

tasks = [
    (splitter.MysqlReader("fund_info", ENGINE_RD), splitter.MysqlReader("fund_info", ENGINE_8612GT)),
    (splitter.MysqlReader("fund_info_aggregation", ENGINE_RD), splitter.MysqlReader("fund_info_aggregation", ENGINE_8612GT)),
    (splitter.MysqlReader("fund_asset_scale", ENGINE_RD), splitter.MysqlReader("fund_asset_scale", ENGINE_8612GT)),
    (splitter.MysqlReader("fund_allocation_data", ENGINE_RD), splitter.MysqlReader("fund_allocation_data", ENGINE_8612GT)),
    (splitter.MysqlReader("fund_fee_data", ENGINE_RD), splitter.MysqlReader("fund_fee_data", ENGINE_8612GT)),
    (splitter.MysqlReader("fund_manager_mapping", ENGINE_RD), splitter.MysqlReader("fund_manager_mapping", ENGINE_8612GT)),
    (splitter.MysqlReader("fund_month_index", ENGINE_RD), splitter.MysqlReader("fund_month_index", ENGINE_8612GT)),
    (splitter.MysqlReader("fund_org_mapping", ENGINE_RD), splitter.MysqlReader("fund_org_mapping", ENGINE_8612GT)),
    (splitter.MysqlReader("fund_portfolio", ENGINE_RD), splitter.MysqlReader("fund_portfolio", ENGINE_8612GT)),
    (splitter.MysqlReader("fund_security_data", ENGINE_RD), splitter.MysqlReader("fund_security_data", ENGINE_8612GT)),
    (splitter.MysqlReader("fund_type_mapping", ENGINE_RD), splitter.MysqlReader("fund_type_mapping", ENGINE_8612GT)),
    (splitter.MysqlReader("fund_weekly_index_static", ENGINE_RD), splitter.MysqlReader("fund_weekly_index", ENGINE_8612GT)),
    (splitter.MysqlReader("index_monthly_return", ENGINE_RD), splitter.MysqlReader("index_monthly_return", ENGINE_8612GT)),
    (splitter.MysqlReader("index_monthly_risk", ENGINE_RD), splitter.MysqlReader("index_monthly_risk", ENGINE_8612GT)),
    (splitter.MysqlReader("index_monthly_subsidiary", ENGINE_RD), splitter.MysqlReader("index_monthly_subsidiary", ENGINE_8612GT)),
    (splitter.MysqlReader("index_weekly_return", ENGINE_RD), splitter.MysqlReader("index_weekly_return", ENGINE_8612GT)),
    (splitter.MysqlReader("index_weekly_risk", ENGINE_RD), splitter.MysqlReader("index_weekly_risk", ENGINE_8612GT)),
    (splitter.MysqlReader("index_weekly_subsidiary", ENGINE_RD), splitter.MysqlReader("index_weekly_subsidiary", ENGINE_8612GT)),
    (splitter.MysqlReader("manager_info", ENGINE_RD), splitter.MysqlReader("manager_info", ENGINE_8612GT)),
    (splitter.MysqlReader("manager_resume", ENGINE_RD), splitter.MysqlReader("manager_resume", ENGINE_8612GT)),
    (splitter.MysqlReader("market_index", ENGINE_RD), splitter.MysqlReader("market_index", ENGINE_8612GT)),
    (splitter.MysqlReader("market_info", ENGINE_RD), splitter.MysqlReader("market_info", ENGINE_8612GT)),
    (splitter.MysqlReader("org_executive_info", ENGINE_RD), splitter.MysqlReader("org_executive_info", ENGINE_8612GT)),
    (splitter.MysqlReader("org_info", ENGINE_RD), splitter.MysqlReader("org_info", ENGINE_8612GT)),
    (splitter.MysqlReader("security_price", ENGINE_RD), splitter.MysqlReader("security_price", ENGINE_8612GT)),
    (splitter.MysqlReader("sws_index", ENGINE_RD), splitter.MysqlReader("sws_index", ENGINE_8612GT)),
    (splitter.MysqlReader("fund_nv_data_standard", ENGINE_RD), splitter.MysqlReader("fund_nv_data", ENGINE_8612GT)),
    (splitter.MysqlReader("fund_weekly_return", ENGINE_RD), splitter.MysqlReader("fund_weekly_return", ENGINE_8612GT)),
    (splitter.MysqlReader("fund_weekly_risk", ENGINE_RD), splitter.MysqlReader("fund_weekly_risk", ENGINE_8612GT)),
    (splitter.MysqlReader("fund_weekly_risk2", ENGINE_RD), splitter.MysqlReader("fund_weekly_risk2", ENGINE_8612GT)),
    (splitter.MysqlReader("fund_subsidiary_weekly_index", ENGINE_RD), splitter.MysqlReader("fund_subsidiary_weekly_index", ENGINE_8612GT)),
    (splitter.MysqlReader("fund_subsidiary_weekly_index2", ENGINE_RD), splitter.MysqlReader("fund_subsidiary_weekly_index2", ENGINE_8612GT)),
    (splitter.MysqlReader("fund_subsidiary_weekly_index3", ENGINE_RD), splitter.MysqlReader("fund_subsidiary_weekly_index3", ENGINE_8612GT)),
    (splitter.MysqlReader("fund_month_return", ENGINE_RD), splitter.MysqlReader("fund_month_return", ENGINE_8612GT)),
    (splitter.MysqlReader("fund_month_risk", ENGINE_RD), splitter.MysqlReader("fund_month_risk", ENGINE_8612GT)),
    (splitter.MysqlReader("fund_month_risk2", ENGINE_RD), splitter.MysqlReader("fund_month_risk2", ENGINE_8612GT)),
    (splitter.MysqlReader("fund_subsidiary_month_index", ENGINE_RD), splitter.MysqlReader("fund_subsidiary_month_index", ENGINE_8612GT)),
    (splitter.MysqlReader("fund_subsidiary_month_index2", ENGINE_RD), splitter.MysqlReader("fund_subsidiary_month_index2", ENGINE_8612GT)),
    (splitter.MysqlReader("fund_subsidiary_month_index3", ENGINE_RD), splitter.MysqlReader("fund_subsidiary_month_index3", ENGINE_8612GT)),
    (splitter.MysqlReader("fund_weekly_indicator", ENGINE_RD), splitter.MysqlReader("fund_weekly_indicator", ENGINE_8612GT)),
    (splitter.MysqlReader("fund_month_indicator", ENGINE_RD), splitter.MysqlReader("fund_month_indicator", ENGINE_8612GT)),
    (splitter.MysqlReader("org_month_research", ENGINE_RD), splitter.MysqlReader("org_month_research", ENGINE_8612GT)),
    (splitter.MysqlReader("org_month_return", ENGINE_RD), splitter.MysqlReader("org_month_return", ENGINE_8612GT)),
    (splitter.MysqlReader("org_month_risk", ENGINE_RD), splitter.MysqlReader("org_month_risk", ENGINE_8612GT)),
    (splitter.MysqlReader("org_month_routine", ENGINE_RD), splitter.MysqlReader("org_month_routine", ENGINE_8612GT)),
]


def main():
    NOT_SYNC_PRODUCT = (
        # 雷根
        'JR006559', 'JR006560', 'JR006602', 'JR010118', 'JR013757', 'JR015022', 'JR018212', 'JR025431',
        'JR027002', 'JR028121', 'JR031510', 'JR033010', 'JR034849', 'JR040097', 'JR058358', 'JR058516',
        'JR065039', 'JR070143', 'JR078235', 'JR087376', 'JR100709', 'JR100710', 'JR100711', 'JR102591',
        'JR103119', 'JR103556', 'JR103755', 'JR103939', 'JR105422', 'JR106452', 'JR109261', 'JR113887',
        'JR114298', 'JR115117', 'JR122835', 'JR122945', 'JR123264', 'JR126054', 'JR126094', 'JR126880',
        'JR131987', 'JR134330', 'JR134488', 'JR134505', 'JR134507', 'JR134515', 'JR135170', 'JR136217',
        'JR137446', 'JR137524', 'JR137557', 'JR137577', 'JR138618', 'JR138619', 'JR139002', 'JR139099',
        'JR139389', 'JR139390', 'JR139532', 'JR139769', 'JR141827', 'JR141896', 'JR142194', 'JR142961',
        'JR143253', 'JR147568',
        # 洛书
        'JR155148', 'JR155579', 'JR156934', 'JR147792', 'JR142156', 'JR142309', 'JR138694', 'JR087140',
        'JR086911', 'JR086986', 'JR036144', 'JR063527', 'JR073645', 'JR085036', 'JR074126', 'JR046611',
        'JR100769', 'JR109354', 'JR024256', 'JR071872', 'JR023887', 'JR027867', 'JR027868', 'JR027869',
        'JR027870', 'JR049010', 'JR147330',
    )
    WHERE_EXCEPT = " AND fund_id NOT IN {fid}".format(fid=str(NOT_SYNC_PRODUCT))

    WHERE = "WHERE update_time BETWEEN '{t0}' AND '{t1}'".format(
        t0=(dt.datetime.now() - relativedelta(hours=1, minutes=5)).strftime("%Y%m%d%H%M%S"),
        t1=dt.datetime.now().strftime("%Y%m%d%H%M%S")
    )

    for TASK_NO in range(0, len(tasks)):
        print("task_no: {no}; task: {src} --> {tgt}".format(no=TASK_NO, src=tasks[TASK_NO][0]._name, tgt=tasks[TASK_NO][1]._name))
        try:
            if tasks[TASK_NO][0]._name == "fund_type_mapping":
                j = splitter.Job(
                    tasks[TASK_NO][0], tasks[TASK_NO][1], pool_size={"update": 5}, where="WHERE flag = 1",
                    apply={"typestandard_code": lambda x: {601: 1, 603: 2, 604: 3, 602: 4, 600: 5, 605: 6}.get(x)}
                )
                j.sync()

            elif tasks[TASK_NO][0]._name == "security_price":
                j = splitter.Job(
                    tasks[TASK_NO][0], tasks[TASK_NO][1], pool_size={"update": 5}, where=WHERE,
                    apply={"circulated_price": lambda x: x / 1e8, "market_price": lambda x: x / 1e8}
                )
                j.update()

            elif tasks[TASK_NO][0]._name in (
                    "fund_info", "fund_info_aggregation", "fund_nv_data_standard"):
                j = splitter.Job(
                    tasks[TASK_NO][0], tasks[TASK_NO][1], pool_size={"update": 5}, where=WHERE + WHERE_EXCEPT,
                    where_del="WHERE fund_id LIKE 'JR%%'"
                )
                j.sync()

            elif tasks[TASK_NO][0]._name in (
                    "fund_weekly_return", "fund_weekly_risk", "fund_subsidiary_weekly_index",
                    "fund_month_return", "fund_month_risk", "fund_subsidiary_month_index",
                    "fund_month_risk2", "fund_weekly_risk2", "fund_subsidiary_weekly_index2",
                    "fund_subsidiary_weekly_index3", "fund_subsidiary_month_index2", "fund_subsidiary_month_index3",
            ):
                j = splitter.Job(
                    tasks[TASK_NO][0], tasks[TASK_NO][1], pool_size={"update": 5}, where=WHERE + WHERE_EXCEPT,
                    where_del="WHERE fund_id LIKE 'JR%%'"
                )
                j.update()

            else:
                j = splitter.Job(
                    tasks[TASK_NO][0], tasks[TASK_NO][1], pool_size={"update": 5}, where=WHERE,
                )
                j.sync()

        except Exception as e:
            print(TASK_NO, e)

if __name__ == "__main__":
    main()
