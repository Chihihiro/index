import bisect
import calendar as cld
import datetime as dt
import numpy as np
from scipy import stats
import pandas as pd
from dateutil.relativedelta import relativedelta
from utils.algorithm.base import exceptions
from utils.algorithm.perf import api
from utils.algorithm.ranking import const, share
from utils.database import config as cfg
from utils.decofactory import common
from utils.sqlfactory.constructor import sqlfmt
from utils.timeutils import basetype


def date2datetime(date):
    return dt.datetime(date.year, date.month, date.day)


# SQL工厂
class FundDataLoader:
    engine = cfg.load_engine()["2Gb"]

    def __init__(self, **kwargs):
        [self.__setattr__(k, v) for k, v in kwargs.items()]

    def load_nv(self):
        sql = "SELECT fund_id, statistic_date as date, swanav as nav " \
              "FROM base_public.fund_nv " \
              "WHERE fund_id in ({fid}) AND statistic_date BETWEEN '{sd}' AND '{ed}'".format(
            fid=sqlfmt(self.fund_id), sd=self.start, ed=self.end)
        df = pd.read_sql(sql, self.engine)  # SQL Table
        df["date"] = df["date"].apply(lambda x: dt.datetime(x.year, x.month, x.day))
        return df.pivot(index="date", columns="fund_id", values="nav")

    @classmethod
    def load_foundation_date(cls):
        sql = "SELECT fund_id, MIN(statistic_date) as foundation_date FROM base_public.fund_nv GROUP BY fund_id"
        return pd.read_sql(sql, cls.engine)

    @classmethod
    def load_funds_structured(cls):
        ids = const.MUTFUNDS_STRUCTURED

        return ids

    @classmethod
    def load_funds_open(cls):
        sql = "SELECT fund_id " \
              "FROM base_public.fund_type_mapping " \
              "WHERE typestandard_code = '01' AND type_code = '0102'"
        return set([x[0] for x in cls.engine.execute(sql).fetchall()])

    @classmethod
    def load_funds_daily(cls):
        sql = "SELECT fund_id " \
              "FROM base_public.fund_info " \
              "WHERE nv_freq = '日度'"
        return set([x[0] for x in cls.engine.execute(sql).fetchall()])

    @classmethod
    def load_funds_of_type(cls, args: tuple):
        tcode2, tcode4 = args
        if len(tcode2) == 4:
            s = "type_code"
        elif len(tcode4) == 6:
            s = "stype_code"

        sql = "SELECT fund_id FROM base_public.fund_type_mapping " \
              "WHERE typestandard_code = '02' AND `{s}` = '{tcode2}' " \
              "AND fund_id IN (" \
              "SELECT fund_id FROM base_public.fund_type_mapping " \
              "WHERE typestandard_code = '04' AND stype_code = '{tcode4}')".format(s=s, tcode2=tcode2, tcode4=tcode4)
        return set([x[0] for x in cls.engine.execute(sql).fetchall()])


class IndexDataLoader:
    engine = cfg.load_engine()["2Gb"]

    def __init__(self, **kwargs):
        [self.__setattr__(k, v) for k, v in kwargs.items()]

    @common.unhash_inscache()
    def load_bm(self):
        sql = "SELECT index_id, date, value " \
              "FROM base_finance.index_value " \
              "WHERE index_id in ({iids}) AND date BETWEEN '{sd}' AND '{ed}'".format(
            iids=sqlfmt(self.index_id), sd=self.start, ed=self.end)
        df = pd.read_sql(sql, self.engine)  # SQL Table
        df["date"] = df["date"].apply(lambda x: dt.datetime(x.year, x.month, x.day))
        return df.pivot(index="date", columns="index_id", values="value")

    @common.unhash_inscache()
    def load_rf(self):
        # 一年期国债利率多取一年, 以确保有值填充
        sql = "SELECT statistic_date as date, y1_treasury_rate as value " \
              "FROM base.market_index " \
              "WHERE statistic_date BETWEEN '{sd}' AND '{ed}'".format(
            sd=self.start - relativedelta(years=1), ed=self.end)
        df = pd.read_sql(sql, self.engine)  # SQL Table
        df["date"] = df["date"].apply(lambda x: dt.datetime(x.year, x.month, x.day))
        return df.set_index("date")


INDEX_IDS = ["000300.CSI", "000905.CSI"]


# 算法类
class Ranking:
    RANK_RANGES = (0.1, 0.325, 0.675, 0.9)

    def __init__(self, obj):
        self.obj = obj

    @property
    def rank(self):
        df = self.obj.score_frame.copy()
        if len(df) == 0:
            return df
        df["__flag"] = df.apply(lambda x: 3 - self._first_not_null(x[::-1]), axis=1)
        df["compre"] = df.apply(lambda x: self._composite_rank(x), axis=1)
        df.drop(axis=1, labels=["__flag"], inplace=True)

        df_rank_pct = df.apply(lambda x: x.dropna().rank(ascending=False, pct=True))
        df_star = df_rank_pct.applymap(lambda x: 5 - bisect.bisect_right(self.RANK_RANGES, x) if ~np.isnan(x) else x)
        df_star["statistic_date"] = self.obj.end
        return df.join(df_star, rsuffix="_rank")

    @classmethod
    def _first_not_null(cls, l):
        """
            返回数组中第一个非空的元素下标;

        Args:
            l: list<int>

        Returns:
            int

        """

        for idx, x in enumerate(l):
            if ~np.isnan(x):
                return idx

        return idx

    @classmethod
    def _composite_rank(cls, l):
        """

        Args:
            l: list[4]<int>
                前三元素分别为y1_rar, y2_rar, y3_rar, 第四个元素为__flag<1-3>, 用于表示可计算的评级年限;

        Returns:

        """

        if l[3] == 1:
            weights = [1]
        elif l[3] == 2:
            weights = [0.7, 0.3]
        elif l[3] == 3:
            weights = [0.5, 0.3, 0.2]
        else:
            raise NotImplementedError

        weights = [weight if ~np.isnan(score) else 0 for (weight, score) in zip(weights, l)]
        scale_ratio = 1 / sum(weights)
        scaled_weight = [weight * scale_ratio for weight in weights]  # 对评分有数据缺失的部分, 评级时剩余部分的权重按比例缩放

        weighed_score = [w * s for (w, s) in zip(scaled_weight, l)]

        return np.nansum(weighed_score)  # 权重序列比不为空, 但是N-年期评分可能为空, 因此使用nansum;


class MutRanking:
    TYPES_CN = {
        # 主动型
        "+1s1": "股票型(主动)",
        "+1b1": "纯债型(主动)",
        "+1b2": "混合债券型(主动)",
        "+1bl1": "偏股混合型(主动)",
        "+1bl2": "偏债混合型(主动)",
        "+1bl3": "平衡混合型(主动)",
        "+1bl4": "灵活配置型(主动)",
        "+1bl5": "FOF(主动)",

        # 被动型
        "-1s1": "股票型(复制指数)",
        "-2s1": "股票型(指数增强)",
        "-1b": "债券型(复制指数)",
        "-2b": "债券型(指数增强)",
    }

    def __init__(self, rank_date, types_=None):
        """

        Args:
            rank_date: datetime.date
                评级日期, 支持的季度末(3, 6, 9, 12)
            types_: list[str]
                用作计算的评级类型, 目前支持。
                被动型目前benchmark默认选择为沪深300, 尚未支持自定义benchmark组合;

                # 主动型
                    "+1s1": "股票型(主动)",
                    "+1b1": "纯债型(主动)",
                    "+1b2": "混合债券型(主动)",
                    "+1bl1": "偏股混合型(主动)",
                    "+1bl2": "偏债混合型(主动)",
                    "+1bl3": "平衡混合型(主动)",
                    "+1bl4": "灵活配置型(主动)",
                    "+1bl5": "FOF(主动)";

                # 被动型
                    "-1s1": "股票型(复制指数)",
                    "-2s1": "股票型(指数增强)",
                    "-1b": "债券型(复制指数)",
                    "-2b": "债券型(指数增强)";
        """

        self.rank_date = rank_date
        self.types_ = types_ if types_ is not None else ['+1b1', '+1b2', '+1bl1', '+1bl2', '+1bl3', '+1bl4', '+1bl5', '+1s1']
        self.sampler = Sampler(rank_date)

    def rank_type(self, type_):
        """

        Args:
            type_: str, optional {
            "+1s1": "股票型(主动)", "+1b1": "纯债型(主动)", "+1b2": "混合债券型(主动)", "+1bl1": "偏股混合型(主动)",
            "+1bl2": "偏债混合型(主动)", "+1bl3": "平衡混合型(主动)", "+1bl4": "灵活配置型(主动)", "+1bl5": "FOF(主动)"
            }


        Returns:

        """

        fids = self.sampler.funds_pass_check(type_)
        if len(fids) < 10:
            raise exceptions.NotEnoughSampleError("Not enough funds for %s type ranking" % type_)
        funds = MutFunds(sorted(fids), end=self.rank_date, ftype=int(type_[0] == "-"))

        r = Ranking(funds)
        return r.rank

    @property
    @common.unhash_inscache()
    def rank_all(self):
        res = pd.DataFrame()
        for type_ in self.types_:
            try:
                print(self.TYPES_CN[type_])
                tmp = self.rank_type(type_)
                tmp["fund_type"] = self.TYPES_CN[type_]
                res = res.append(tmp)
            except exceptions.NotEnoughSampleError as e1:
                print(e1)
            except exceptions.DataError as e2:
                print(e2)

        res.reset_index(inplace=True)
        if len(res) > 0:
            res.columns = ["fund_id", "y1_zscore", "y3_zscore", "y5_zscore", "compre_zscore",
                           "y1_rank", "y3_rank", "y5_rank", "compre_rank",
                           "statistic_date", "fund_type"]
        return res


class MutFunds:
    def __init__(self, fund_ids, end, fb_map=None, ftype=0):
        """

        Args:
            fund_ids: str
            start: datetime.date
            end: datetime.date
            fb_map: ditc{fund_id: bm_id}
                如果基金有不同benchmark, 以字典形式传入;
            ftype: int, optional {0, 1}, default 0
                0: 主动型
                1: 被动型

        """

        self._fund_ids = fund_ids
        self.end = end
        self.rank_year = 5
        self.fdataloader = FundDataLoader(fund_id=self._fund_ids, start=end - relativedelta(years=self.rank_year), end=end)
        self.fb_map = fb_map
        self.ftype = ftype

    @property
    def start(self):
        s = self.end - relativedelta(years=self.rank_year)
        return dt.date(s.year, s.month, cld.monthrange(s.year, s.month)[1])

    @property
    def idataloader(self):
        kw = {"start": self.start, "end": self.end}
        if self.fb_map:
            kw.update(index_id=self.fb_map.keys())
        else:
            kw.update(index_id=["000300.CSI"])
        return IndexDataLoader(**kw)

    @property
    def fund_ids(self):
        return self.portfolio.value_series.columns.tolist()

    @property
    @common.unhash_inscache()
    def ts(self):
        # 构造变长时序
        return basetype.VarTimeSeries(self.fdataloader.load_nv(), self.start, self.end, check=True)

    @property
    @common.unhash_inscache()
    def ts_bm(self):
        # 构造变长时序
        data = self.idataloader.load_bm()
        if self.fb_map:
            cols = [self.fb_map.get(fid, data.columns[0]) for fid in self.fund_ids]
        else:
            cols = data.columns

        if len(set(cols)) == 1:
            data = data[cols[0]]  # 降维至1维
        elif len(set(cols)) > 1:
            data = data[cols]
        else:
            raise ValueError("无可用基准数据")
        return basetype.VarTimeSeries(data, self.start, self.end)

    @property
    @common.unhash_inscache()
    def ts_rf(self):
        # 构造变长时序
        return basetype.VarTimeSeries(self.idataloader.load_rf()["value"], self.start, self.end, fill=True, shift=1, lmbd=lambda x: x / 36500)

    @property
    def portfolio(self):
        # 将变长时序时间区间设置为与实例时间区间一致
        self.ts.start, self.ts.end = date2datetime(self.start), date2datetime(self.end)
        return self.ts

    @property
    def benchmark(self):
        # 将变长时序时间区间设置为与实例时间区间一致
        self.ts_bm.start, self.ts_bm.end = date2datetime(self.start), date2datetime(self.end)
        return self.ts_bm

    @property
    def benchmark_rf(self):
        # 将变长时序时间区间设置为与实例时间区间一致
        self.ts_rf.start, self.ts_rf.end = date2datetime(self.start), date2datetime(self.end)
        return self.ts_rf

    @property
    def calargs(self):
        return self.portfolio.value_mtx, self.benchmark.value_mtx, self.benchmark_rf.value_mtx, self.portfolio.tmstmp_mtx, 250

    @property
    @common.inscache("_perf", selfhash=True, maxcache=1)
    def perf(self):
        M_var = {1: 1250, 3: 3750, 5: 6250}
        return api.Calculator(*self.calargs, **{"m": M_var[self.rank_year]})

    @property
    def zscore(self):
        if self.ftype == 0:
            # res shape: (N_indacators, M_funds)
            res = np.array([self.perf.return_a, self.perf.sharpe_a, self.perf.periods_pos_prop, self.perf.value_at_risk])
        elif self.ftype == 1:
            res = np.array([self.perf.excess_return_a, self.perf.tracking_error_a, self.perf.info_a, self.perf.periods_pos_prop])

        res = np.apply_along_axis(share.trunc_quarter, 1, res)
        res = stats.zscore(res, axis=1, ddof=1)
        return res

    @property
    @common.inscache("_score", selfhash=True, maxcache=1)
    def score(self):
        res = self.zscore
        tmp = res.flatten()
        tmp[tmp > 3] = 3
        tmp[tmp < -3] = -3
        res = tmp.reshape(res.shape)
        if self.ftype == 0:
            return res[0] + res[1] + res[2] - res[3]
        elif self.ftype == 1:

            return res[0] - res[1] + res[2] + res[3]

    @property
    def score_series(self):
        return pd.Series(self.score, self.fund_ids)

    @property
    @common.unhash_inscache()
    def score_frame(self):
        s = []
        for y in (1, 3, 5):
            self.rank_year = y
            s.append(self.score_series)
        df = pd.DataFrame(s).T
        df.columns = ["y1_zscore", "y3_zscore", "y5_zscore"]
        return df

    def __hash__(self):
        return hash((self.start, self.end))


class Sampler:
    TYPES = {
        # 主动型
        "+1s1": ("020101", "040101"),  # 股票型(主动)
        "+1b1": ("020201", "040101"),  # 纯债型(主动)
        "+1b2": ("020202", "040101"),  # 混合债券型(主动)
        "+1bl1": ("020301", "040101"),  # 偏股混合型(主动)
        "+1bl2": ("020302", "040101"),  # 偏债混合型(主动)
        "+1bl3": ("020303", "040101"),  # 平衡混合型(主动)
        "+1bl4": ("020304", "040101"),  # 灵活配置型(主动)
        "+1bl5": ("020305", "040101"),  # FOF(主动)

        # 被动型
        "-1s1": ("020101", "040201"),  # 股票型(复制指数)
        "-2s1": ("020101", "040202"),  # 股票型(指数增强),
        "-1b": ("0202", "040201"),  # 债券型(复制指数),
        "-2b": ("0202", "040202"),  # 债券型(指数增强),
    }

    def __init__(self, rank_date):
        self.rank_date = rank_date

    @property
    @common.unhash_inscache()
    def funds_18m(self):
        df = FundDataLoader.load_foundation_date()
        lb = self.rank_date - relativedelta(months=6)
        return set(df.loc[df["foundation_date"].apply(lambda x: x <= lb)]["fund_id"])

    @property
    @common.unhash_inscache()
    def funds_open(self):
        return FundDataLoader.load_funds_open()

    @property
    @common.unhash_inscache()
    def fund_daily(self):
        return FundDataLoader.load_funds_daily()

    @property
    def funds_structured(self):
        return const.MUTFUNDS_STRUCTURED

    def funds_of_type(self, type_):
        return FundDataLoader.load_funds_of_type(self.TYPES[type_])

    def funds_pass_check(self, type_):
        return self.funds_of_type(type_).intersection(self.funds_18m).intersection(self.funds_open).intersection(self.fund_daily) - self.funds_structured


def main():
    from utils.database import io
    from utils.algorithm.ranking.ranking import CalculateHelper
    # t = dt.date.today()
    t = CalculateHelper.last_rank_date(dt.date.today())
    if (t.month, t.day) in {(3, 31), (6, 30), (9, 30), (12, 31)}:
        print("RANK DATE: ", t)
        types = ['+1b1', '+1b2', '+1bl1', '+1bl2', '+1bl3', '+1bl4', '+1bl5', '+1s1']
        for type in types:
            f = MutRanking(t, [type])
            res = f.rank_all
            if len(res):
                io.to_sql("base_public.fund_rank", cfg.load_engine()["2Gbp"], res)
                print(res.head())


def test1():
    from utils.database import io
    for y in range(2016, 2018):
        for m in (3, 6, 9, 12):
            d = cld.monthrange(y, m)[1]
            date = dt.date(y, m, d)
            print("RANK DATE: ", date)
            f = MutRanking(date)
            a = f.rank_all
            io.to_sql("base_public.fund_rank", cfg.load_engine()["2Gb"], a)


def test2():
    end = dt.date(2018, 6, 30)
    f = MutRanking(end)
    f.rank_type("+1s1")


if __name__ == "__main__":
    main()
    # test2()
