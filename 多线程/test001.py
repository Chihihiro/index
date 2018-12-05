import datetime as dt
from dateutil.relativedelta import relativedelta
import pandas as pd
from utils.algorithm import preprocess as pre, calculation as cal
from utils.database import config as cfg, io, sqlfactory as sf
from utils.dev import calargs_mutual
import time
from sqlalchemy import create_engine
import arrow

# 设置preprocess, calculation类的计算参数为公募的计算参数;
pre.SQL_USED = sf.SQL.Mutual
cal.calargs = calargs_mutual


UPDATE_TIME = dt.datetime.now()

engines = cfg.load_engine()
# engine_rd = create_engine("mysql+pymysql://jr_sync_yu:jr_sync_yu@10.66.175.186:3306/base_public", connect_args={"charset": "utf8"})
engine_mkt = engines["2Gb"]
# engine_wt = create_engine("mysql+pymysql://jr_sync_yu:jr_sync_yu@10.66.175.186:3306/base_public", connect_args={"charset": "utf8"})
engine_rd = engines["2Gbp"]
engine_wt = engines["2Gbp"]

_freq = "d"
_intervals = calargs_mutual.intervals
_funcs_1 = [
    "accumulative_return", "return_a", "excess_return_a", "persistence", "odds", "sharpe_a", "calmar_a", "sortino_a", "info_a",
    "jensen_a", "treynor_a"
]
_funcs_2 = [
    "stdev", "stdev_a", "downside_deviation_a", "unsystematic_risk", "max_drawdown", "mdd_time", "mdd_repair_time", "beta", "corr", "VaR", "ERVaR"
]
_funcs_3 = [
    "con_rise_periods", "con_fall_periods", "ability_timing", "ability_security", "tracking_error_a", "p_earning_periods", "n_earning_periods", "min_return", "max_return",
    "skewness", "kurtosis",
]

_bms_used = ["hs300", "csi500", "sse50", "cbi", "nfi"]

def main(date_start, date_end, date_column='statistic_date'):
    """

    :param date_start:开始日期，str
    :param date_end: 结束日期，str
    :param date_column: 作用列，statistic_date | updatetime
    :return:
    """
    indexs = pd.date_range(date_start, date_end, freq='D')
    for i in range(len(indexs)-1):
        m_start = indexs[i].date()
        m_end = indexs[i+1].date()
        sql = "select distinct fund_id from fund_nv where {col} > '{start}' and {col} <= '{end}' " \
            .format(col=date_column, start=str(m_start), end=(m_end))
        df_ids = pd.read_sql(sql, engine_rd)
        print("{}: count:{}".format(m_end, len(df_ids)))
        n = 500
        dfs = tuple(df_ids[i:i + n] for i in range(0, len(df_ids), n))
        for df_slice in dfs:
            cal_by_date(m_end, df_slice['fund_id'].tolist())

# def main():
#     for update_time in [UPDATE_TIME]:
#         try:
#             tasks = pre.generate_tasks(update_time - relativedelta(hours=24, minutes=5), update_time, freq=_freq, processes=7, conn=engine_rd)
#             tasks = {k: v for k, v in tasks.items() if k >= dt.date(2015, 1, 1)}
#             print(update_time, len(tasks))
#
#         except ValueError as e:
#             print(update_time, e)
#             continue
#         for statistic_date, ids_used in sorted(tasks.items(), key=lambda x: x[0]):
#             cal_by_date(statistic_date, list(ids_used))
#
#         print("TASK DONE: {ut}".format(ut=update_time))

def cal_by_date(statistic_date, fund_ids):
    """

    :param statistic_date: datetime.date
    :param fund_ids: list
    :return:
    """
    print("STATISTIC_DATE:{sd}, LENGTH:{l}".format(sd=statistic_date, l=len(fund_ids)))
    result_1 = []
    result_2 = []
    result_3 = []
    data = pre.ProcessedData(statistic_date, fund_ids, _freq, pe=[], conn=engine_rd, conn_mkt=engine_mkt,
                             weekday=True)
    bms = {index_name: cal.Benchmark(attr_dict, index_name) for index_name, attr_dict in data.index.items()}
    tbond = cal.Tbond(data.index["y1_treasury_rate"], "y1_treasury_rate")
    for fid, attrs in data.funds.items():
        fund = cal.Fund(attrs)
        res_1, _funcs_1_sourted = cal.calculate(_funcs_1, _intervals, _bms_used, _freq, statistic_date, fund, bms,
                                                tbond, with_func_names=True)
        res_2, _funcs_2_sorted = cal.calculate(_funcs_2, _intervals, _bms_used, _freq, statistic_date, fund, bms, tbond,
                                               with_func_names=True)
        res_3, _funcs_3_sorted = cal.calculate(_funcs_3, _intervals, _bms_used, _freq, statistic_date, fund, bms, tbond,
                                               with_func_names=True)
        result_1.extend(res_1)
        result_2.extend(res_2)
        result_3.extend(res_3)

    df_1 = pd.DataFrame(result_1)
    df_2 = pd.DataFrame(result_2)
    df_3 = pd.DataFrame(result_3)

    # 删除空行
    df_1.dropna(how='all', inplace=True)
    df_2.dropna(how='all', inplace=True)
    df_3.dropna(how='all', inplace=True)

    cols_1 = cal.format_cols_mutual(_funcs_1_sourted, _freq,
                                    prefix=["fund_id", "fund_name", "statistic_date", "benchmark"])
    cols_2 = cal.format_cols_mutual(_funcs_2_sorted, _freq,
                                    prefix=["fund_id", "fund_name", "statistic_date", "benchmark"])
    cols_3 = cal.format_cols_mutual(_funcs_3_sorted, _freq,
                                    prefix=["fund_id", "fund_name", "statistic_date", "benchmark"])
    if len(df_1):
        df_1.columns = cols_1
        io.to_sql("fund_daily_return", conn=engine_wt, dataframe=df_1, chunksize=500)
    if len(df_2):
        df_2.columns = cols_2
        io.to_sql("fund_daily_risk", conn=engine_wt, dataframe=df_2, chunksize=500)
    if len(df_3):
        df_3.columns = cols_3
        io.to_sql("fund_daily_subsidiary", conn=engine_wt, dataframe=df_3, chunksize=500)

# def test():
#     statistic_date = dt.date(2018, 8, 6)
#     fund_id = "000001"
#     cal_by_date(statistic_date, [fund_id])

if __name__ == "__main__":
    # date_end = arrow.now().date()
    # date_start = arrow.now().shift(years=0, months=0, days=-3).date()
    # main(date_start, date_end, 'update_time')
    date_end = arrow.now().shift(years=0, months=0, days=-1).date()

    date_start = arrow.now().shift(years=0, months=0, days=-3).date()

    # date_start = arrow.get("2018-08-08").date()
    main(date_start, date_end)
