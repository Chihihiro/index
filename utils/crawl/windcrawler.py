import datetime as dt
import pandas as pd
from abc import ABCMeta, abstractmethod
from multiprocessing.dummy import Pool as ThreadPool
from utils.database import config as cfg, io
from utils.decofactory import common
from utils.crawl.const import wsdconst
from WindPy import w as wind

default_engine = cfg.load_engine()["2Gbf"]


class ICrawler(metaclass=ABCMeta):
    engine = default_engine

    cols_store = {}

    @abstractmethod
    def crawl(self):
        """should generate dataframe contains all columns in `cols_store`, and pass it to `store` method"""

    @classmethod
    def store(cls, dataframe):
        for tb, col in cls.cols_store.items():
            tmp = dataframe.reindex(columns=col).dropna(axis=1, how="all")  # tolerant index
            io.to_sql(tb, cls.engine, tmp)


class BaseWindStockCrawler:
    engine = default_engine

    @property
    @common.unhash_inscache()
    def ids_to_crawl(self):
        sql = "SELECT DISTINCT stock_id FROM stock_info"
        # sql = "select DISTINCT stock_id from `stock_valuation` where date >= '2018-06-01' and pe_ttm  is null"

        return pd.read_sql(sql, self.engine)["stock_id"].tolist()

    @classmethod
    def fetch_stocks_ol(cls, date=dt.date.today()):
        """
            获取陆(sh, sz)股通, 全部上市公司, 创业板股票的Wind Ids;

        Args:
            date:

        Returns:

        """

        date = date.strftime("%Y-%m-%d")
        sector_AB = wind.wset("sectorconstituent",
                              "date={time};sectorid=a001010900000000;field=wind_code".format(time=date))
        sector_china = wind.wset("sectorconstituent",
                                 "date={time};sectorid=1000025141000000;field=wind_code".format(time=date))
        sector_gem = wind.wset("sectorconstituent",
                               "date={time};sectorid=a001010r00000000;field=wind_code".format(time=date))

        security_lists = []
        for sector in [sector_AB, sector_china, sector_gem]:
            if len(sector.Data) == 1:
                security_lists.extend(sector.Data[0])

        security_lists = set(security_lists)

        return security_lists


class WindStockTimeSeriesCrawler(BaseWindStockCrawler):
    engine = default_engine

    def __init__(self, start=None, end=None):
        self._start = start
        self._end = end

    @classmethod
    def timefmt(cls, date):
        return date.strftime("%Y-%m-%d")

    @property
    def start(self):
        return self._start

    @property
    def end(self):
        return self._end or (dt.date.today() - dt.timedelta(1) * (dt.datetime.now().hour <= 18))


class WindSWSTypeCrawler(ICrawler):
    engine = cfg.load_engine()["2Gb"]
    TM_SWS = {
        "6102000000000000": ("210000", "采掘"),
        "6103000000000000": ("220000", "化工"),
        "6104000000000000": ("230000", "钢铁"),
        "6105000000000000": ("240000", "有色金属"),
        "6106010000000000": ("610000", "建筑材料"),
        "6106020000000000": ("620000", "建筑装饰"),
        "6107010000000000": ("630000", "电气设备"),
        "6107000000000000": ("640000", "机械设备"),
        "1000012579000000": ("650000", "国防军工"),
        "1000012588000000": ("280000", "汽车"),
        "6111000000000000": ("330000", "家用电器"),
        "6113000000000000": ("350000", "纺织服装"),
        "6114000000000000": ("360000", "轻工制造"),
        "6120000000000000": ("450000", "商业贸易"),
        "6101000000000000": ("110000", "农林牧渔"),
        "6112000000000000": ("340000", "食品饮料"),
        "6121000000000000": ("460000", "休闲服务"),
        "6115000000000000": ("370000", "医药生物"),
        "6116000000000000": ("410000", "公用事业"),
        "6117000000000000": ("420000", "交通运输"),
        "6118000000000000": ("430000", "房地产"),
        "6108000000000000": ("270000", "电子"),
        "1000012601000000": ("710000", "计算机"),
        "6122010000000000": ("720000", "传媒"),
        "1000012611000000": ("730000", "通信"),
        "1000012612000000": ("480000", "银行"),
        "1000012613000000": ("490000", "非银金融"),
        "6123000000000000": ("510000", "综合"),

    }

    cols_store = {
        "base_finance.stock_type": ["stock_id", "name", "dimension", "type_code", "type_name"]
    }

    @classmethod
    def _fetch_swstype(cls):
        import time
        """
            获取申万行业分类

        Returns:
            pd.DataFrame

        """

        date_str = dt.date.today().strftime("%Y-%m-%d")
        result = pd.DataFrame()
        for wind_sid, mp_sws in cls.TM_SWS.items():
            time.sleep(1)
            print(wind_sid)
            params_options = "date={date};sectorid={sectorid}".format(date=date_str, sectorid=wind_sid)
            data = wind.wset("sectorconstituent", params_options).Data
            tmp = pd.DataFrame(data).T[[1, 2]]
            tmp[3] = mp_sws[0]
            tmp[4] = mp_sws[1]
            result = result.append(tmp)
        result.columns = ["stock_id", "name", "type_code", "type_name"]
        result["dimension"] = "SWS"
        del result["name"]
        return result

    @classmethod
    def crawl(cls):
        wind.start()
        result = cls._fetch_swstype()
        cls.store(result)


class WindStockValCrawler(ICrawler, WindStockTimeSeriesCrawler):
    cols_query = {
        **wsdconst.CommonQuery.D_STOCKVAL,
    }

    cols_store = {
        "base_finance.stock_valuation": [
            "stock_id", "date", "market_price", "circulated_price", "pe_ttm",
            "pe_deducted_ttm", "pe_lyr", "pb", "pb_lf", "beta_24m"]
    }

    @property
    def start(self):
        if self._start is None:
            self._start = default_engine.execute("SELECT MAX(date) FROM base_finance.stock_price").fetchone()[0]
        return self._start

    @classmethod
    def _fetch_price(cls, stock_ids, date_s, date_e):
        try:
            date_s_str = date_s.strftime("%Y-%m-%d")
            date_e_str = date_e.strftime("%Y-%m-%d")
            result = pd.DataFrame()

            cols_query_str = ",".join(cls.cols_query.keys())
            for stock_id in stock_ids:
                resprox = wind.wsd(stock_id, cols_query_str, date_s_str, date_e_str, "ruleType=3;unit=1;currencyType=")
                if resprox.ErrorCode == 0:
                    print("Success {} ###{} - {}".format(stock_id, date_e_str, date_e_str))
                    dates = resprox.Times
                    resprox = resprox.Data
                else:
                    print("Error: ", date_s, date_e, stock_id)
                    continue
                d = pd.DataFrame(resprox).T
                d.columns = list(cls.cols_query.values())
                d["stock_id"] = stock_id
                d["date"] = dates
                result = result.append(d)

            if len(result) > 0:
                subsets = list(set(["date", "last_trading_day"]).intersection(set(result.columns)))
                result = result.dropna(subset=subsets, how="any")
                #result = result.dropna(subset=["date", "last_trading_day"], how="any")
                # result["date"] = result["date"].apply(lambda x: x.date()) # 2017.11.9股价数据改版,格式直接为datetime.date
                if 'last_trading_day' in result.columns:
                    result["last_trading_day"] = result["last_trading_day"].apply(lambda x: x.date())
                result.index = range(len(result))
            else:
                result = pd.DataFrame()

            return result

        except Exception as e:
            print(e)

    def crawl(self):
        wind.start()

        if self.start == self.end: return
        security_ids_sorted = self.ids_to_crawl

        STEP = 5
        sliced = [security_ids_sorted[i: i + STEP] for i in range(0, len(security_ids_sorted), STEP)]
        # sliced = sliced[:1]

        # 异步采集, 入库
        pool = ThreadPool(8)
        for sids in sliced:
            # sids = sids[:1]
            pool.apply_async(self._fetch_price, args=(sids, self.start, self.end), callback=self.store)
        pool.close()
        pool.join()
        print("done")


class WindStockCapitalCrawler(ICrawler, WindStockTimeSeriesCrawler):
    cols_query = {
        **wsdconst.CommonQuery.D_STOCK_CAPITAL
    }

    cols_store = {
        "base_finance.stock_capital": ["stock_id", "date", "free_float_shares"]
    }

    def _fetch_capital_data(self, stock_ids):
        result = pd.DataFrame()

        s_start, s_end = [self.timefmt(x) for x in [self.start, self.end]]
        query_str = ",".join(self.cols_query)
        for stock_id in stock_ids:
            resprox = wind.wsd(stock_id, query_str, s_start, s_end, "unit=1")

            if resprox.ErrorCode == 0:
                print("Success", stock_id)
                dates = resprox.Times
                resprox = resprox.Data
            else:
                print("Error: ", self.start, self.end, stock_id)
                continue

            d = pd.DataFrame(resprox).T
            d.columns = list(self.cols_query.values())
            d["stock_id"] = stock_id
            d["date"] = dates
            result = result.append(d)

        return result.dropna(how="all", subset=list(self.cols_query.values()))

    def crawl(self):
        wind.start()
        STEP = 5
        sliced = [self.ids_to_crawl[i: i + STEP] for i in range(0, len(self.ids_to_crawl), STEP)]

        # 异步采集, 入库
        pool = ThreadPool(8)
        for sids in sliced:
            pool.apply_async(self._fetch_capital_data, args=(sids,), callback=self.store)
        pool.close()
        pool.join()
        print("done")


class WindStockSeasonDataCrawler(ICrawler, WindStockTimeSeriesCrawler):
    ENGINE = cfg.load_engine()["2Gb"]
    cols_query = {
        **wsdconst.CommonQuery.S_STOCK_OTHERS
    }

    cols_store = {
        "base_finance.stock_ability_revenue": ["stock_id", "date", "roe_ttm2"],
        "base_finance.stock_ability_debt": ["stock_id", "date", "longdebttodebt"],
        "base_finance.stock_ability_growth": ["stock_id", "date", "yoyeps_basic"],
        "base_finance.stock_asset": ["stock_id", "date", "tot_assets"],
        "base_finance.stock_liability": ["stock_id", "date", "tot_liab"],
        "base_finance.stock_equity": ["stock_id", "date", "cap_stk"],
    }

    def _fetch_season_data(self, stock_ids):
        result = pd.DataFrame()

        s_start, s_end = [self.timefmt(x) for x in [self.start, self.end]]
        query_str = ",".join(self.cols_query)
        for stock_id in stock_ids:
            resprox = wind.wsd(stock_id, query_str, s_start, s_end, "unit=1;rptType=1;Period=Q;Days=Alldays")

            if resprox.ErrorCode == 0:
                print("Success", stock_id)
                dates = resprox.Times
                resprox = resprox.Data
            else:
                print("Error: ", self.start, self.end, stock_id)
                continue

            d = pd.DataFrame(resprox).T
            d.columns = list(self.cols_query.values())
            d["stock_id"] = stock_id
            d["date"] = dates
            result = result.append(d)

        return result.dropna(how="all", subset=list(self.cols_query.values()))

    def crawl(self):
        wind.start()
        STEP = 5
        sliced = [self.ids_to_crawl[i: i + STEP] for i in range(0, len(self.ids_to_crawl), STEP)]

        # 异步采集, 入库
        pool = ThreadPool(8)
        for sids in sliced:
            pool.apply_async(self._fetch_season_data, args=(sids,), callback=self.store)
        pool.close()
        pool.join()
        print("done")


class AmtSHStockCrawler(ICrawler, WindStockTimeSeriesCrawler):
    cols_query = ["lastradeday_s", "amt"]
    cols_db = ["last_trading_day", "trans_amount"]

    cols_store = {
        "base_finance.stock_price": ["stock_id", "date", "last_trading_day", "trans_amount"],
    }

    @property
    def ids_to_crawl(self):
        q = sorted([x[0] for x in self.engine.execute(
            "SELECT DISTINCT stock_id FROM base_finance.stock_price WHERE stock_id LIKE '%%.SH'").fetchall()])
        return q


def main():
    a = WindStockSeasonDataCrawler(dt.datetime(2007, 1, 1), dt.datetime(2017, 12, 31))
    # a._fetch_season_data(["000001.SZ"])
    a.crawl()

if __name__ == "__main__":
    main()
