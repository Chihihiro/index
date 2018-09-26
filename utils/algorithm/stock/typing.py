import bisect
import calendar as cld
import datetime as dt
from dateutil.relativedelta import relativedelta
import pandas as pd
from utils.algorithm.perf import api
from utils.decofactory import common
from utils.sqlfactory.constructor import sqlfmt


class StyleType:
    from utils.database import config as cfg
    engine = cfg.load_engine()["2Gb"]
    quantile_mkt = (0.3, 0.7)
    quantile_scale = (0.5,)
    quantile_mom = (0.3, 0.7)
    quantile_rmw = (0.3, 0.7)
    quantile_cma = (0.3, 0.7)

    def __init__(self, year, season):
        self.year = year
        self.season = season

    @property
    def date_end(self):
        return self.date_range[-1]

    @property
    def date_range(self):
        """

        Returns:
            tuple[datetime.date, datetime.date]

        """

        dr = {
            1: (dt.date(self.year, 1, 1), dt.date(self.year, 3, 31)),
            2: (dt.date(self.year, 4, 1), dt.date(self.year, 6, 30)),
            3: (dt.date(self.year, 7, 1), dt.date(self.year, 9, 30)),
            4: (dt.date(self.year, 10, 1), dt.date(self.year, 12, 31)),
        }

        return dr[self.season]

    @property
    @common.unhash_inscache()
    def valid_stockids(self):
        """

        Returns:
            list[str]

        """

        start, end = self.date_range
        # 剔除银行, 非银金融
        # 剔除ST, *ST, PT股
        sql = "SELECT sts.stock_id, sv.pb_lf, sp.trans_amount " \
              "FROM base_finance.stock_type_sws sts " \
              "JOIN base_finance.stock_info si ON si.stock_id = sts.stock_id " \
              "JOIN base_finance.stock_price sp ON sts.stock_id = sp.stock_id " \
              "JOIN base_finance.stock_valuation sv ON sv.stock_id = sp.stock_id AND sv.date = sp.date " \
              "WHERE sts.type_code NOT IN ('480000', '490000') " \
              "AND si.name NOT LIKE '*ST%%' AND si.name NOT LIKE 'ST%%' AND si.name NOT LIKE 'PT%%'" \
              "AND sv.date BETWEEN {start} AND {end} " \
              "AND si.listing_date_a < '{endm3}'" \
              "AND sv.market_price != 0 AND sp.trans_amount != 0 ".format(
            start=start.strftime("%Y%m%d"), end=end.strftime("%Y%m%d"), endm3=end-relativedelta(months=3))

        df = pd.read_sql(sql, self.engine)

        df["bp"] = 1 / df["pb_lf"]
        g1 = df.dropna(subset=["trans_amount"]).groupby("stock_id").mean()
        g2 = df.dropna(subset=["bp"]).groupby("stock_id").mean()
        stock_low_transamt = set(g1.loc[g1["trans_amount"] <= g1["trans_amount"].quantile(0.05)].index)
        stock_negative_bp = set(g2.loc[g2["bp"] < 0].index)
        stock_drop = stock_negative_bp.union(stock_low_transamt)

        return df.loc[df.stock_id.apply(lambda x: x not in stock_drop)]["stock_id"].drop_duplicates().tolist()

    @property
    @common.unhash_inscache()
    def data_turnover(self):
        start, end = self.date_range
        stock_ids = self.valid_stockids
        sql = "SELECT stock_id, market_price, pb_lf " \
              "FROM base_finance.stock_valuation " \
              "WHERE date BETWEEN '{start}' AND '{end}' " \
              "AND stock_id IN ({sids}) " \
              "AND market_price != 0 AND market_price IS NOT NULL ".format(start=start.strftime("%Y%m%d"), end=end.strftime("%Y%m%d"), sids=sqlfmt(stock_ids))
        df = pd.read_sql(sql, self.engine)
        df["bp"] = 1 / df["pb_lf"]
        return df

    @property
    @common.unhash_inscache()
    def data_roe(self):
        start, end = self.date_range
        stock_ids = self.valid_stockids
        sql = "SELECT stock_id, roe_ttm2 as value " \
              "FROM base_finance.stock_ability_revenue " \
              "WHERE date BETWEEN '{start}' AND '{end}' " \
              "AND stock_id IN ({sids})".format(start=start.strftime("%Y%m%d"), end=end.strftime("%Y%m%d"), sids=sqlfmt(stock_ids))
        return pd.read_sql(sql, self.engine)

    @property
    @common.unhash_inscache()
    def data_totalasset(self):
        start, end = self.date_range
        stock_ids = self.valid_stockids
        sql = "SELECT stock_id, date, tot_assets as value " \
              "FROM base_finance.stock_asset " \
              "WHERE date BETWEEN '{start}' AND '{end}' " \
              "AND stock_id IN ({sids}) ".format(start=(start-relativedelta(years=1)).strftime("%Y%m%d"), end=end.strftime("%Y%m%d"), sids=sqlfmt(stock_ids))
        df = pd.read_sql(sql, self.engine).pivot(index="date", columns="stock_id", values="value")
        return df

    @property
    def data_price(self):
        start, end = self.date_range
        stock_ids = self.valid_stockids
        sql = "SELECT stock_id, `date`, `close` as `value` " \
              "FROM base_finance.stock_price " \
              "WHERE date BETWEEN '{start}' AND '{end}' " \
              "AND stock_id IN ({sids}) " \
              "AND last_trading_day = date " \
              "AND close != 0 AND close IS NOT NULL".format(start=start.strftime("%Y%m%d"), end=end.strftime("%Y%m%d"), sids=sqlfmt(stock_ids))
        df = pd.read_sql(sql, self.engine).pivot(index="date", columns="stock_id", values="value")
        return df

    def classify_by_scale(self):
        try:
            grouped = self.data_turnover.groupby("stock_id").mean()[["market_price"]]
            grouped = grouped.dropna(subset=["market_price"])
            grouped["rank"] = grouped["market_price"].rank(ascending=True, pct=True)  # 升序, 即字段值越小, 排名的数值越小
            grouped["scale_type"] = grouped["rank"].apply(lambda x: bisect.bisect_left(self.quantile_scale, x))

            return grouped.reset_index()[["stock_id", "scale_type"]]
        except:
            return pd.DataFrame(None, columns=["stock_id", "scale_type"])

    def classify_by_value(self):
        try:
            grouped = self.data_turnover.groupby("stock_id").mean()[["bp"]]
            grouped = grouped.dropna(subset=["bp"])
            grouped["rank"] = grouped["bp"].rank(ascending=True, pct=True)  # 升序, 即字段值越小, 排名的数值越小
            grouped["value_type"] = grouped["rank"].apply(lambda x: bisect.bisect_left(self.quantile_mkt, x))

            return grouped.reset_index()[["stock_id", "value_type"]]

        except:
            return pd.DataFrame(None, columns=["stock_id", "value_type"])

    def classify_by_mom(self):
        try:
            df = self.data_price
            grouped = pd.DataFrame(pd.Series(api.accumulative_return(df.values), index=df.columns), columns=["rs"]).reset_index()
            grouped = grouped.dropna(subset=["rs"])
            grouped["rank"] = grouped["rs"].rank(ascending=True, pct=True)
            grouped["mom_type"] = grouped["rank"].apply(lambda x: bisect.bisect_left(self.quantile_mom, x))

            return grouped[["stock_id", "mom_type"]]
        except:
            return pd.DataFrame(None, columns=["stock_id", "mom_type"])

    def classify_by_rmw(self):
        try:
            grouped = self.data_roe.groupby("stock_id").mean()[["value"]]
            grouped = grouped.dropna(subset=["value"])
            grouped["rank"] = grouped["value"].rank(ascending=True, pct=True)  # 升序, 即字段值越小, 排名的数值越小
            grouped["rmw_type"] = grouped["rank"].apply(lambda x: bisect.bisect_left(self.quantile_rmw, x))

            return grouped.reset_index()[["stock_id", "rmw_type"]]

        except:
            return pd.DataFrame(None, columns=["stock_id", "rmw_type"])

    def classify_by_cma(self):
        try:
            df = self.data_totalasset
            grouped = pd.DataFrame(pd.Series(api.accumulative_return(df.values), index=df.columns), columns=["goa"]).reset_index()
            grouped = grouped.dropna(subset=["goa"])
            grouped["rank"] = grouped["goa"].rank(ascending=True, pct=True)
            grouped["cma_type"] = grouped["rank"].apply(lambda x: bisect.bisect_left(self.quantile_mom, x))

            return grouped.reset_index()[["stock_id", "cma_type"]]

        except:
            return pd.DataFrame(None, columns=["stock_id", "cma_type"])

    @property
    def classified(self):
        tp_val = self.classify_by_value()
        tp_scale = self.classify_by_scale()
        tp_mom = self.classify_by_mom()
        tp_rmw = self.classify_by_rmw()
        tp_cma = self.classify_by_cma()

        kw = {"on": "stock_id", "how": "outer"}
        res = tp_val.merge(tp_scale, **kw).merge(tp_mom, **kw).merge(tp_rmw, **kw).merge(tp_cma, **kw)
        res["date"] = self.date_end

        return res


def main():
    for year, season in [*[(y, s) for y in range(2015, 2018) for s in range(1, 5)][:0], (2018, 1)]:
        print(year, season)
        s = StyleType(year, season)
        print(s.classified)


if __name__ == "__main__":
    main()
