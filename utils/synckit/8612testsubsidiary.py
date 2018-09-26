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
ENGINE_8612GT = cfg.load_engine()["8612ts"]
# ENGINE_8612GT = create_engine("mysql+pymysql://jr_sync_yu:15901622959q@182.254.128.241:8612/test_subsidiary", pool_size=50, connect_args={"charset": "utf8"})

tasks = [
    (splitter.MysqlReader("index_monthly_return", ENGINE_RD), splitter.MysqlReader("index_monthly_return", ENGINE_8612GT)),
    (splitter.MysqlReader("index_monthly_risk", ENGINE_RD), splitter.MysqlReader("index_monthly_risk", ENGINE_8612GT)),
    (splitter.MysqlReader("index_monthly_subsidiary", ENGINE_RD), splitter.MysqlReader("index_monthly_subsidiary", ENGINE_8612GT)),
    (splitter.MysqlReader("index_weekly_return", ENGINE_RD), splitter.MysqlReader("index_weekly_return", ENGINE_8612GT)),
    (splitter.MysqlReader("index_weekly_risk", ENGINE_RD), splitter.MysqlReader("index_weekly_risk", ENGINE_8612GT)),
    (splitter.MysqlReader("index_weekly_subsidiary", ENGINE_RD), splitter.MysqlReader("index_weekly_subsidiary", ENGINE_8612GT)),
    (splitter.MysqlReader("market_index", ENGINE_RD), splitter.MysqlReader("market_index", ENGINE_8612GT)),
    (splitter.MysqlReader("security_price", ENGINE_RD), splitter.MysqlReader("security_price", ENGINE_8612GT)),
    (splitter.MysqlReader("sws_index", ENGINE_RD), splitter.MysqlReader("sws_index", ENGINE_8612GT)),
    (splitter.MysqlReader("market_info", ENGINE_RD), splitter.MysqlReader("market_info", ENGINE_8612GT)),
    (splitter.MysqlReader("index_daily_return", ENGINE_RD), splitter.MysqlReader("index_daily_return", ENGINE_8612GT)),
    (splitter.MysqlReader("index_daily_risk", ENGINE_RD), splitter.MysqlReader("index_daily_risk", ENGINE_8612GT)),
    (splitter.MysqlReader("index_daily_subsidiary", ENGINE_RD), splitter.MysqlReader("index_daily_subsidiary", ENGINE_8612GT)),
]


def main():
    WHERE = "WHERE update_time BETWEEN '{t0}' AND '{t1}'".format(
        t0=(dt.datetime.today() - relativedelta(days=1, minutes=10)).strftime("%Y%m%d"),
        t1=dt.datetime.today().strftime("%Y%m%d")
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
                    tasks[TASK_NO][0], tasks[TASK_NO][1], pool_size={"update": 5}, where=WHERE,
                    where_del="WHERE fund_id LIKE 'JR%%'"
                )
                j.sync()

            elif tasks[TASK_NO][0]._name in (
                    "fund_weekly_return", "fund_weekly_risk", "fund_subsidiary_weekly_index",
                    "fund_month_return", "fund_month_risk", "fund_subsidiary_month_index"):
                j = splitter.Job(
                    tasks[TASK_NO][0], tasks[TASK_NO][1], pool_size={"update": 5}, where=WHERE,
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
