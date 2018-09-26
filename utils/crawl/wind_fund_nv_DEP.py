import datetime as dt
import pandas as pd
from WindPy import w as wind
from functools import partial
from utils.database import config as cfg, io
from multiprocessing.dummy import Pool as ThreadPool

engine = cfg.load_engine()["2Gb"]
wind.start()


def _fetch_latest_date(wind_ids, anchor=dt.date.today()):
    result = {}
    anchor = anchor.strftime("%Y-%m-%d")
    for wind_id in wind_ids:
        print(wind_id)
        tmp = wind.wsd(wind_id, "NAV_date", anchor, anchor)
        result[wind_id] = tmp.Data[0][0]
    result = {k: v.date() for k, v in result.items() if v is not None}
    return result


def fetch_latest_date(wind_ids, anchor=dt.date.today(), pool_size=4):
    result = {}
    pool = ThreadPool(processes=pool_size)
    func = partial(_fetch_latest_date, anchor=anchor)
    results = pool.map(func, wind_ids)
    for r in results:
        result.update(r)
    return result


def fetch_start_date(fund_ids):
    fund_ids = str(tuple(fund_ids)).replace(",)", ")")
    sql = "SELECT fi.fund_id as fund_id, IFNULL(IFNULL(msd1.msd, fi.foundation_date), msd2.msd) as date FROM fund_info fi \
           LEFT JOIN (SELECT fund_id, MAX(statistic_date) as msd FROM fund_nv_data_source \
           WHERE fund_id IN {fid} AND data_source = 13 GROUP BY fund_id) msd1 ON fi.fund_id = msd1.fund_id \
           LEFT JOIN (SELECT fund_id, MIN(statistic_date) as msd FROM fund_nv_data_source \
           WHERE fund_id IN {fid} AND data_source != 13 GROUP BY fund_id) msd2 ON fi.fund_id = msd2.fund_id \
           WHERE fi.fund_id IN {fid}".format(fid=fund_ids)
    data = pd.read_sql(sql, engine).dropna()
    return dict(data.as_matrix())


def fetch_fund_name(fund_ids):
    fund_ids = str(tuple(fund_ids)).replace(",)", ")")
    sql = "SELECT fund_id as fund_id, fund_name as fund_name FROM fund_info \
           WHERE fund_id IN {fid}".format(fid=fund_ids)
    data = pd.read_sql(sql, engine)
    return dict(data.as_matrix())


def fetch_nv(latest_date, earliest_date, anchor=dt.date(2008, 1, 1)):
    """
        Fetch nv, nv_add, nv_adj according to ld; The interval of each fund depends on `latest_date` and `earliest_date`
    Args:
        latest_date: dict
        earliest_date: dict
        anchor: datetime.date
    Returns:
        dataframe
    """
    result = {}
    for wind_id, ld in latest_date.items():
        print(wind_id)
        ed = earliest_date.get(wind_id, anchor)
        if ed <= ld:
            WindData = wind.wsd(wind_id, "NAV_date,nav,NAV_acc,NAV_adj", ed.strftime("%Y-%m-%d"), ld.strftime("%Y-%m-%d"), "")
            tmp_df = pd.DataFrame(WindData.Data).T
            tmp_df.columns = ["statistic_date", "nav", "added_nav", "swanav"]
            tmp_df = tmp_df.drop_duplicates(subset=["statistic_date"]).dropna(subset=["statistic_date"], how="any").dropna(subset=["nav", "added_nav", "swanav"], how="all")
            tmp_df["fund_id"] = wind_id
            result[wind_id] = tmp_df
    return result


def add_to_update_source(fund_ids, source):
    fund_ids = str(tuple(fund_ids)).replace(",)", ")")
    df = pd.DataFrame(fund_ids, columns=["fund_id"])
    df["data_source"] = source
    df["is_updata"] = 1
    io.to_sql("fund_nv_updata_source", df)


def main():
    target = pd.read_excel("c:/Users/Yu/Desktop/wind_fund.xlsx", header=None)
    target.columns = ["wind_id", "fund_id"]
    wind_ids = target["wind_id"].tolist()
    fund_ids = target["fund_id"].tolist()
    id_map = dict(target.as_matrix())
    id_map_reverse = dict(zip(fund_ids, wind_ids))
    fund_names = fetch_fund_name(fund_ids)

    ld = _fetch_latest_date(wind_ids)
    ed = fetch_start_date(fund_ids)
    ed = {id_map_reverse[k]: v for k, v in ed.items()}

    result = pd.DataFrame()
    nv_dict = fetch_nv(ld, ed)
    for fid, df_nv in nv_dict.items():
        result = result.append(df_nv)
    result["statistic_date"] = result["statistic_date"].apply(lambda x: x.date())
    result["fund_id"] = result["fund_id"].apply(lambda x: id_map[x])
    result.index = range(len(result))
    result["fund_name"] = result["fund_id"].apply(lambda x: fund_names.get(x, None))
    result = result[["fund_id", "fund_name", "statistic_date", "nav", "added_nav", "swanav"]]

    result["source_code"] = 3
    result["source"] = "第三方"
    result["data_source"] = 13
    result["data_source_name"] = "Wind"

    add_to_update_source(result["fund_id"].drop_duplicates().tolist(), 13)

    return result
