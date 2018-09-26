from urllib.parse import quote
import requests
import pandas as pd
from utils.database import io, config as cfg
import datetime as dt
engine = cfg.load_engine()["2Gb"]


def fetch_data(period=1):
    # http://www.csindex.com.cn/zh-CN/indices/index-detail/H11001

    period = str(period) + quote("个月")
    index_id = "H11001"
    api_url = "http://www.csindex.com.cn/zh-CN/indices/index-detail/{idx_id}?" \
              "earnings_performance={period}&data_type=json".format(idx_id=index_id, period=period)

    res = pd.DataFrame(requests.get(api_url).json()).rename(columns={"tradedate": "statistic_date", "tclose": "cbi"})
    res["statistic_date"] = res["statistic_date"].apply(lambda x: dt.datetime.strptime(x, "%Y-%m-%d %H:%M:%S").date())

    return res[["statistic_date", "cbi"]].dropna()


def main():
    result = fetch_data(1)
    io.to_sql("market_index", engine, result, "update")


if __name__ == "__main__":
    main()
