import calendar as cld
import datetime as dt
import tushare as ts
import pandas as pd
from dateutil.relativedelta import relativedelta
from functools import partial
from multiprocessing.dummy import Pool as ThreadPool
from utils.database import io, config as cfg
from utils.decofactory import common

DEFAULT_ENGINE = cfg.load_engine()["2Gb"]


def date2str(date):
    return date.strftime("%Y-%m-%d")


# 采集工具类基类
class BaseCrawler:
    def __init__(self, **kwargs):
        self.engine = kwargs.get("engine", DEFAULT_ENGINE)

        self.pool_size = kwargs.get("pool_size", 1)
        self.thread_pool = ThreadPool(self.pool_size)


# 基本面数据
class BasicDataCrawler(BaseCrawler):
    code_name = "stock_id"

    def __init__(self, year, season, **kwargs):
        super().__init__(**kwargs)

        self.year = year
        self.season = season
        self.year_season = year * 100 + season

    @classmethod
    def guess_report_date(cls, year, season, report_date):
        m, d = [int(x) for x in report_date.split("-")]
        earliest_report_date = dt.date(year, season * 3, cld.monthrange(year, season * 3)[1])
        cur_report_date = dt.date(year, m, d)

        while cur_report_date < earliest_report_date:
            cur_report_date = dt.date(cur_report_date.year + 1, m, d)
        return cur_report_date

    @classmethod
    def base(cls, ):
        pass

    def performance(self, year, season):
        pass

    def revenue(self):
        df = ts.get_profit_data(self.year, self.season).rename(
            columns={"code": "stock_id", "gross_profit_rate": "gross_profit_ratio"}).drop(labels=["name"], axis=1)
        df["season"] = self.year_season
        return df

    def cashflow(self):
        df = ts.get_cashflow_data(self.year, self.season).rename(
            columns={"code": "stock_id", "rateofreturn": "ofreturn_ratio"}).drop(labels=["name"], axis=1)
        df["season"] = self.year_season
        return df

    def crawl(self):
        # io.to_sql("babylon.stock_revenue", self.engine, self.revenue())
        io.to_sql("babylon.stock_cashflow", self.engine, self.cashflow())


# K线数据基类
class KdataCrawler(BaseCrawler):
    tables = {}
    code_name = None
    index = False

    def __init__(self, code=None, **kwargs):
        """
        实例化crawler
        Args:
            code: str, list<str>, or None, default None
                code为None时, 时候需要在子类实现load_codes方法
            **kwargs:
                date_start: datetime.date
                date_end: datetime.date
                ktype: str, optional {"D", "5", "15", "30", "60"}
                engine: sqlalchemy.engine.base.Engine
                pool_size: int
        """
        super().__init__(**kwargs)

        self._code = [code] if type(code) is str else code
        self.ktype = kwargs.get("ktype", "D")
        self.date_end = kwargs.get("date_end", dt.date.today())
        self.date_start = kwargs.get("date_start", self.date_end - relativedelta(weeks=1))

    def load_codes(self):
        """
        若初始实例时传入的code为None, 加载需要采集的代码至_code属性.
        Returns:
        """

        raise NotImplementedError(
            "Must implement `load_codes` method if instance initialized without `code` parameter.")

    @property
    def code(self):
        if not self._code:
            self.load_codes()
        return self._code

    @classmethod
    def kdata(cls, code, ktype, start=None, end=None, index=False):
        """
        封装tushare.get_k_data接口, 采集股票和基准的k线数据.
        Args:
            code: str
            ktype: str, optional {"D", "5", "15", "30", "60"}
            start: datetime.date, or None
            end: datetime.date, or None
        Returns:
            pandas.DataFrame{
                index: <datetime.date>
                columns: ["code"<str>, "date"<datetime.date>,
                         "open", "close", "open_fadj", "close_fadj", "open_badj", "close_badj"<float>,
                         "high", "low", "high_fadj", "low_fadj", "high_badj", "low_badj", "volume"<float>],
            }
        """

        code, suffix = code.split(".")

        start = date2str(start) if start is not None else "1985-01-01"
        end = date2str(end) if end is not None else None

        try:
            print("code:{}, start:{}, end:{}".format(code, start, end))
            result = ts.get_k_data(code, ktype=ktype, autype=None, start=start, end=end, index=False, retry_count=10)
            result["stock_id"] = result["code"].apply(lambda x: x + "." + suffix)
            del result["code"]
            result["last_trading_day"] = result["date"]

            return result[["stock_id", "date", "last_trading_day", "close", "open", "high", "low", "volume"]]

        except KeyError:
            print("{code} has no data during {start}-{end}".format(code=code, start=start, end=end))

    def store(self, data: pd.DataFrame):
        if data is not None:
            io.to_sql(self.tables[self.ktype], self.engine, data)

    def crawl(self):
        """
        采集k线数据并入库
        Returns:
        """

        f = partial(self.kdata, ktype=self.ktype)
        if self.ktype == "D":
            f = partial(f, start=self.date_start, end=self.date_end)

        # 多线程异步采集, 存储
        [self.thread_pool.apply_async(f, args=(sid,), callback=self.store) for sid in self.code]

        self.thread_pool.close()
        self.thread_pool.join()


class StockKdataCrawler(KdataCrawler):
    tables = {
        "D": "base_finance.stock_price",
    }
    code_name = "stock_id"

    def load_codes(self):
        self._code = sorted([x[0] for x in self.engine.execute("SELECT DISTINCT stock_id FROM base_finance.stock_info").fetchall()])


def main():
    s = StockKdataCrawler(date_start=dt.date(2007, 1, 1), date_end=dt.date.today() - dt.timedelta(1))
    s.crawl()


if __name__ == "__main__":
    main()
