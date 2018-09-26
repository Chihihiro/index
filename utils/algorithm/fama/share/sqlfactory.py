import datetime as dt
import pandas as pd
from utils.decofactory import common


# SQL Factory
class DataLoader:
    from sqlalchemy import create_engine
    engine = create_engine("mysql+pymysql://jr_test_yu:smyt1234@182.254.128.241:4171/base")

    @classmethod
    def load_fdate(cls, fund_id):
        sql = "SELECT IFNULL(fi.foundation_date, MIN(statistic_date)) as fdate " \
              "FROM fund_info fi " \
              "JOIN fund_nv_data_standard fnds ON fi.fund_id = fnds.fund_id " \
              "WHERE fi.fund_id = '{fid}' " \
              "GROUP BY fi.fund_id".format(fid=fund_id)

        res = cls.engine.execute(sql).fetchone()
        if res is None:
            return None
        return res[0]

    @classmethod
    def load_nvdata(cls, fund_id, start, end):
        sql = "SELECT statistic_date, swanav as value " \
              "FROM base.fund_nv_data_standard " \
              "WHERE fund_id = '{fid}' " \
              "AND statistic_date > '{start}' AND statistic_date <= '{end}'".format(fid=fund_id, start=start, end=end)
        df_nv = pd.read_sql(sql, cls.engine)

        return df_nv.set_index("statistic_date")

    @classmethod
    def load_bmdata(cls, start, end):
        sql = "SELECT date as statistic_date, value " \
              "FROM base_finance.index_value " \
              "WHERE index_id = '930903.CSI' AND date > '{start}' AND date <= '{end}'".format(start=start, end=end)
        df_bm = pd.read_sql(sql, cls.engine)

        return df_bm.set_index("statistic_date")

    @classmethod
    def load_rfdata(cls, start, end):
        sql = "SELECT statistic_date, y1_treasury_rate as value " \
              "FROM base.market_index " \
              "WHERE statistic_date <= '{end}' AND statistic_date > '{start}'".format(start=start - dt.timedelta(365),
                                                                                      end=end)
        df_rf = pd.read_sql(sql, cls.engine).fillna(method="ffill")
        try:
            start = start.date()
        except:
            pass
        df_rf = df_rf.loc[df_rf["statistic_date"] > start]

        return df_rf.set_index("statistic_date")

    @classmethod
    def load_factordata(cls, factor_ids, start, end, freq):
        from utils.sqlfactory.constructor import sqlfmt
        table = "factor_style_%s" % freq
        sql = "SELECT factor_id, date, factor_value as value " \
              "FROM factor.{tb} " \
              "WHERE factor_id IN ({fids}) " \
              "AND date > '{start}' AND date <= '{end}'".format(fids=sqlfmt(factor_ids), start=start, end=end, tb=table)
        df = pd.read_sql(sql, cls.engine)

        return df.pivot(index="date", columns="factor_id", values="value")

    @classmethod
    def load_stockdata(cls, start, end):
        sql = "SELECT sp.stock_id, sp.date, close, circulated_price " \
              "FROM base_finance.stock_price sp " \
              "JOIN base_finance.stock_valuation sv ON sp.stock_id = sv.stock_id AND sp.date = sv.date " \
              "WHERE sp.date > '{start}' AND sp.date <= '{end}' " \
              "AND sp.date = sp.last_trading_day AND sp.close <> 0 AND sp.close IS NOT NULL " \
              "AND sv.circulated_price <> 0 AND sv.circulated_price IS NOT NULL".format(start=start, end=end)
        df_sp = pd.read_sql(sql, cls.engine)

        return df_sp.pivot(index="date", columns="stock_id")


class FamaDataLoader(DataLoader):
    @classmethod
    @common.hash_inscache(paramhash=True, maxcache=1)
    def load_type_value(cls, start, end):
        sql = "SELECT stock_id, value_type " \
              "FROM base_finance.stock_type_style " \
              "WHERE `date` > '{start}' AND `date` <= '{end}'".format(start=start, end=end)
        df = pd.read_sql(sql, cls.engine)
        d = {0: "L", 1: "M", 2: "H"}
        df["value_type"] = df["value_type"].apply(lambda x: d.get(x))
        df = df.dropna()

        return df.groupby("value_type")["stock_id"].apply(set).to_dict()

    @classmethod
    @common.hash_inscache(paramhash=True, maxcache=1)
    def load_type_scale(cls, start, end):
        sql = "SELECT stock_id, scale_type " \
              "FROM base_finance.stock_type_style " \
              "WHERE `date` > '{start}' AND `date` <= '{end}'".format(start=start, end=end)
        df = pd.read_sql(sql, cls.engine)
        d = {0: "S", 1: "B"}
        df["scale_type"] = df["scale_type"].apply(lambda x: d.get(x))
        df = df.dropna()

        return df.groupby("scale_type")["stock_id"].apply(set).to_dict()


class CarhartDataLoader(FamaDataLoader):
    @classmethod
    @common.hash_inscache(paramhash=True, maxcache=1)
    def load_type_mom(cls, start, end):
        sql = "SELECT stock_id, mom_type " \
              "FROM base_finance.stock_type_style " \
              "WHERE `date` > '{start}' AND `date` <= '{end}'".format(start=start, end=end)
        df = pd.read_sql(sql, cls.engine)
        d = {0: "L", 1: "D", 2: "W"}
        df["mom_type"] = df["mom_type"].apply(lambda x: d.get(x))
        df = df.dropna()

        return df.groupby("mom_type")["stock_id"].apply(set).to_dict()


class FamaFrenchDataloader(FamaDataLoader):
    @classmethod
    @common.hash_inscache(paramhash=True, maxcache=1)
    def load_type_rmw(cls, start, end):
        """Robust minus Weak"""

        sql = "SELECT stock_id, rmw_type " \
              "FROM base_finance.stock_type_style " \
              "WHERE `date` > '{start}' AND `date` <= '{end}'".format(start=start, end=end)
        df = pd.read_sql(sql, cls.engine)
        d = {0: "W", 1: "M", 2: "R"}
        df["rmw_type"] = df["rmw_type"].apply(lambda x: d.get(x))
        df = df.dropna()

        return df.groupby("rmw_type")["stock_id"].apply(set).to_dict()

    @classmethod
    @common.hash_inscache(paramhash=True, maxcache=1)
    def load_type_cma(cls, start, end):
        """Conservative minus Aggressive"""

        sql = "SELECT stock_id, cma_type " \
              "FROM base_finance.stock_type_style " \
              "WHERE `date` > '{start}' AND `date` <= '{end}'".format(start=start, end=end)
        df = pd.read_sql(sql, cls.engine)
        d = {0: "C", 1: "M", 2: "A"}
        df["cma_type"] = df["cma_type"].apply(lambda x: d.get(x))
        df = df.dropna()

        return df.groupby("cma_type")["stock_id"].apply(set).to_dict()
