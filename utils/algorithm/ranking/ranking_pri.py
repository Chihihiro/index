import bisect
import datetime as dt
import numpy as np
import pandas as pd
import time
from utils.algorithm.ranking import share
from utils.decofactory.common import inscache
from utils.database import config as cfg, io
from dateutil.relativedelta import relativedelta

ENGINE_RD = cfg.load_engine()["2Gb"]


def shift_to_monday(date, year_shift) -> dt.date:
    """
        根据输入日期, 前推3个自然年, 再前推至周一;

    Args:
        date: datetime.date

    Returns:
        datetime.date

    """

    date_start = date - relativedelta(years=year_shift)  # rank_date前推一个自然rank_year
    date_start -= dt.timedelta(date_start.weekday() + 7)  # 将起始日期对齐至一年前的周一
    return date_start


def date2datetime(date) -> dt.datetime:
    return dt.datetime.fromtimestamp(time.mktime(date.timetuple()))


class PriFund:
    engine = ENGINE_RD

    def __init__(self, fund_id):
        self.fund_id = fund_id

    @property
    @inscache("_cached")
    def struct_type(self) -> int or None:
        """
            基金的结构类型信息

        Returns:
            int, or None

        """

        sql = "SELECT type_code FROM `fund_type_mapping` " \
              "WHERE fund_id = '{fid}' AND typestandard_code = 602 AND flag = 1".format(fid=self.fund_id)
        res = self.engine.execute(sql).fetchone()
        res = res[0] if res is not None else None
        return res

    @inscache("_cached", True)
    def _fetch_nvdata(self, rank_date) -> pd.DataFrame:
        """
            根据评级日期, 调取最长的可能要用到的数据(三年+前推数日至周一)

        Args:
            rank_date: datetime.date

        Returns:
            pd.DataFrame{
                index: "statistic_date"<datetime.datetime>;
                columns: "adj_nav"<float>
            }

        """

        start_date = shift_to_monday(rank_date, 3)
        start_date, rank_date = [str(x) for x in (start_date, rank_date)]

        sql = "SELECT statistic_date, swanav as adj_nv FROM fund_nv_data_standard " \
              "WHERE fund_id = '{fid}' AND statistic_date BETWEEN '{sd}' AND '{ed}' AND swanav IS NOT NULL".format(
               fid=self.fund_id, sd=start_date, ed=rank_date)

        nvdata = pd.read_sql(sql, self.engine)

        nvdata["statistic_date"] = nvdata["statistic_date"].apply(lambda x: date2datetime(x))
        nvdata.set_index("statistic_date", inplace=True)
        nvdata = nvdata.resample("B").last()  # 转化为交易日
        return nvdata

    def nvdata(self, rank_date, rank_year, resampled=True, fill_na=True) -> pd.DataFrame:
        """
            调取计算RAR所需的净值数据(包含区间首的NV_0, 以计算R_1)

        Args:
            rank_date: datetime.date
                评级日期
            rank_year: int
                评级年限
            resampled: bool, default True
                是否对工作日日频数据, 按照周频对齐至周五;

        Returns:
            pd.DataFrame{
                index: "statistic_date"<datetime.datetime>;
                columns: "adj_nav"<float>
            }

        """

        start_date = shift_to_monday(rank_date, rank_year)
        nv = self._fetch_nvdata(rank_date)

        if resampled:
            nv = nv.resample("W-FRI").last()  # 再由标准交易日序列对齐至每周周五;
        if fill_na:
            nv = nv.fillna(method="ffill")  # 用历史数据填充未来日期缺失;

        return nv.loc[(nv.index >= date2datetime(start_date)) & (nv.index <= date2datetime(rank_date))]

    def return_series(self, rank_date, rank_year) -> pd.DataFrame:
        """
            计算给定评级区间中的基金收益率

        Args:
            rank_date: datetime.date
            rank_year: int

        Returns:
            pd.DataFrame{
                index: "statistic_date"<datetime.datetime>;
                columns: "rs"<float>
            }

        """

        start_date = rank_date - relativedelta(years=rank_year)
        nvdata = self.nvdata(rank_date, rank_year, True, True)
        rsdata = (nvdata / nvdata.shift(1) - 1)[1:]
        rsdata = rsdata.loc[(rsdata.index >= date2datetime(start_date)) & (rsdata.index <= date2datetime(rank_date))]
        rsdata.columns = ["rs"]

        # 需要做四分位数截尾
        return rsdata

    def RAR(self, rank_date, rank_year, gamma=2):
        """
            根据评级日期, 评级年限, 计算基金的RAR指标;

        Args:
            rank_date: datetime.date
            rank_year: int
            gamma: int, default 2

        Returns:
            float

        """

        pass_check = Sampler.check(self, rank_date, rank_year)
        if pass_check:
            rs = self.return_series(rank_date, rank_year)["rs"]
            share.truncate_by_quarter(rs)

            return self._RAR(rs, gamma)
        return None

    @classmethod
    def _RAR(cls, return_series, gamma) -> float:
        """
            RAR指标底层算法;

        Args:
            return_series: pandas.Series
            gamma: int

        Returns:
            float

        """

        return (((1 + return_series) ** (-gamma)).sum() / len(return_series)) ** (-52 / gamma) - 1


class Sampler:
    TYPE_STRUCTURED = 60202  # "结构化"的分类代码

    @classmethod
    def check(cls, fund: PriFund, rank_date, rank_year) -> bool:
        """
            判断基金是否符合各种纳入计算N-年RAR指标的条件;

        Args:
            fund:
            rank_date:
            rank_year:

        Returns:

        """
        def conditions():
            # 周频, 或更高频, 且数据缺失小于50%
            yield cls._is_weekly_freq(fund, rank_date, rank_year)

            # 每个评级区间中最后一个季度, 最后一个的后半月(15~月底)中的工作日, 有复权累计净值
            yield cls._has_data_in_last_period(fund, rank_date, rank_year)

            # 每个评级区间中第一个季度, 第一个月的前半月(1~15)中的工作日, 有复权累计净值
            yield cls._has_data_in_first_period(fund, rank_date, rank_year)

            # 属于"非结构化"分类
            yield cls._is_not_structured(fund)

        # for pass_check in conditions():
        #     if not pass_check:
        #         return False

        # DEBUG
        for pass_check, pnt in zip(conditions(), ("频度及样本缺失要求: ", "区间最后一个月后半月有数据: ", "区间首首月前半月有数据: ", "不属于'结构化': ")):
            if not pass_check:
                # print(fund.fund_id, rank_date, rank_year, pnt, pass_check)
                return False
        return True

    # @classmethod
    # def truncate_by_quantile(cls, return_series) -> None:
    #     """
    #        对收益序列作四分位数截尾
    #
    #     Args:
    #         return_series: pd.DataFrame{
    #             index: "statistic_date"<datetime.datetime>,
    #             column: "rs"<float>
    #         }
    #
    #     Returns:
    #         None
    #
    #     """
    #
    #     q1, q3 = [return_series.quantile(q) for q in (0.25, 0.75)]
    #     lower_bound, upper_bound = q1 - 1.5 * (q3 - q1), q3 + 1.5 * (q3 - q1)
    #     return_series[return_series >= upper_bound] = upper_bound
    #     return_series[return_series <= lower_bound] = lower_bound
    #
    # 自定义检查条件
    @classmethod
    def _is_weekly_freq(cls, fund: PriFund, rank_date, rank_year) -> bool:
        """
            判断是否满足条件——"50%以上的净值数据非空"

        Args:
            fund: Fund
            rank_date: datetime.date
            rank_year: int

        Returns:
            bool

        """

        sd = rank_date - relativedelta(years=rank_year)
        ts_std = pd.date_range(sd, rank_date, freq="W-FRI")

        nv = fund.nvdata(rank_date, rank_year, resampled=True, fill_na=False)
        nv = nv.loc[nv.index > date2datetime(sd)]  # 去除区间外的点首个净值点

        return nv["adj_nv"].notnull().sum() / len(ts_std) >= 0.5

    @classmethod
    def _has_data_in_last_month_of_every_season(cls, fund: PriFund, rank_date, rank_year) -> bool:
        """
            判断是否满足条件——"每个季度最后一个月15号至月底有净值披露"

        Args:
            fund:
            rank_date:
            rank_year:

        Returns:

        """

        sd = rank_date - relativedelta(years=rank_year)
        # 去除区间首点的标准时序列, 如参数为2014.12.31, 2015.12.31时, 则第一个点为2014.12.31需要被去除;
        ts_std = pd.date_range(sd, rank_date, freq="Q")
        t0 = ts_std[0]  # 记录标准序列首点时间
        ts_std = ts_std[1:]

        nv = fund.nvdata(rank_date, rank_year, resampled=False, fill_na=False)
        if len(nv) == 0:
            return False

        nv["date"] = nv.index.tolist()
        nv = nv.resample("Q").last()

        # 区间首点(上个区间末)用于计算r1, 但不用于该检查, r1可以为空;
        nv = nv.loc[(nv.index > date2datetime(t0)) & (nv.index <= date2datetime(rank_date))]

        # 第一步, 判断是否有缺少季度(resample方法无法处理收尾缺失);
        if len(nv) != len(ts_std):
            return False

        nv["month_mid"] = [dt.datetime(x.year, x.month, 15) for x in nv.index]

        # 第二步, 判断每个季度resample之后的最后一个日期, 是否大于该季度最后一个月的月中旬日期;
        nv["has_data_in_last_month"] = nv["date"] >= nv["month_mid"]

        return nv["has_data_in_last_month"].all()

    @classmethod
    def _has_data_in_last_period(cls, fund: PriFund, rank_date, rank_year) -> bool:
        """
            判断是否满足条件——"是否在评级期末最后一个季度15号至月底号有净值披露"

        Args:
            fund: Fund
            rank_date:
            rank_year:

        Returns:

        """

        start, end = dt.datetime(rank_date.year, rank_date.month, 15), date2datetime(rank_date)

        nv = fund.nvdata(rank_date, rank_year, resampled=False, fill_na=False).dropna()
        for datetime in nv.index:
            if start <= datetime <= end:
                return True
        return False

    @classmethod
    def _has_data_in_first_period(cls, fund: PriFund, rank_date, rank_year) -> bool:
        """
            判断是否满足条件——"是否在评级期初第一个季度1号至15号有净值披露"

        Args:
            fund: Fund
            rank_date:
            rank_year:

        Returns:

        """

        sd = rank_date - relativedelta(years=rank_year)
        first_month = sd + relativedelta(months=1)
        start, end = dt.datetime(first_month.year, first_month.month, 1), dt.datetime(first_month.year, first_month.month, 15)

        nv = fund.nvdata(rank_date, rank_year, resampled=False, fill_na=False).dropna()
        for datetime in nv.index:
            if start <= datetime <= end:
                return True
        return False

    @classmethod
    def _is_not_structured(cls, fund: PriFund) -> bool:
        """
            判断是否满足条件——"属于非结构化"

        Args:
            fund: Fund

        Returns:

        """

        return fund.struct_type != cls.TYPE_STRUCTURED  # 判断基金是否符合条件——"不是非结构化"


class PriRanking:
    RANK_RANGES = (0.1, 0.325, 0.675, 0.9)

    def __init__(self, rank_date):
        self.rank_date = rank_date

    def rank(self, rank_year):
        score = self.score(rank_year)
        return self._rank(score)

    def _rank(self, score):
        """
            pd.Series

        Args:
            score:

        Returns:

        """

        df_rank_pct = score.rank(ascending=False, pct=True)  # 收益越高, 排名百分比越靠前(即越小), (0, 1]
        df_rank_pct = df_rank_pct.apply(lambda x: 5 - bisect.bisect_right(self.RANK_RANGES, x))

        return df_rank_pct

    def score(self, rank_year):
        """
            返回每只基金在评级日期下的得分

        Args:
            rank_date:
            rank_year:

        Returns:

        """

        return self._scored_by_rar(rank_year)

    # Helper Function
    @property
    @inscache("_cached", True)
    def rar(self) -> pd.DataFrame:
        """
            获取基金的风险调整后收益(risk-adjusted return)

        Returns:
            pd.DataFrame{
                columns: "fund_id"<str>, "statistic_date"<datetime.date>, "y1_rar"<float>, "y2_rar"<float>, "y3_rar"<float>
            }

        """

        sql = "SELECT fund_id, y1_rar, y2_rar, y3_rar " \
              "FROM fund_rank WHERE statistic_date = '{sd}'".format(sd=str(self.rank_date))
        return pd.read_sql(sql, ENGINE_RD)

    def _scored_by_rar(self, rank_year):
        """
            根据基金的风险调整后收益(RAR), 计算不同年限的评级得分：

        Args:
            rank_year: int, or str, optional {1, 2, 3, "composite"}

        Returns:
            pd.DataFrame{
                index: "fund_id"<str>
                column: "score"<float>
            }

        """

        df_rar = self.rar.set_index("fund_id")
        if rank_year == 1:
            df_rar["score"] = df_rar["y1_rar"] * 1

        elif rank_year == 2:
            df_rar["score"] = df_rar["y2_rar"] * 1

        elif rank_year == 3:
            df_rar["score"] = df_rar["y3_rar"] * 1

        elif rank_year == "composite":
            # 对y3_rar, y2_rar, y1_rar列按序寻找第一个非空值, 作为可评级的最长年限
            df_rar["__flag"] = df_rar.apply(lambda x: 3 - self._first_not_null(x[::-1]), axis=1)
            df_rar["score"] = df_rar.apply(lambda x: self._composite_rank(x), axis=1)

        return df_rar["score"].dropna()

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


class CalculateHelper:
    @classmethod
    def last_rank_date(cls, date):
        from dateutil.relativedelta import relativedelta
        from calendar import monthrange
        date -= relativedelta(months=3)
        m = (bisect.bisect_left([3, 6, 9, 12], date.month) + 1) * 3
        return dt.date(date.year, m, monthrange(date.year, m)[1])

    @classmethod
    def calculate_rar(cls, fund_id, rank_date):

        f = PriFund(fund_id)
        rar1, rar2, rar3 = [f.RAR(rank_date, r_y) for r_y in (1, 2, 3)]
        res = pd.DataFrame([fund_id, rank_date, rar1, rar2, rar3]).T
        res.columns = ["fund_id", "statistic_date", "y1_rar", "y2_rar", "y3_rar"]
        res = res.dropna(subset=["y1_rar", "y2_rar", "y3_rar"], how="all")
        return res

    @classmethod
    def calculate_rar_all(cls, rank_date):

        from functools import partial
        from multiprocessing import Pool

        date_mid = dt.date(rank_date.year, rank_date.month, 15)
        f_calculate = partial(cls.calculate_rar, rank_date=rank_date)
        f_store = lambda x: io.to_sql("fund_rank", ENGINE_RD, x)

        sql = "SELECT DISTINCT fund_id " \
              "FROM fund_nv_data_standard " \
              "WHERE statistic_date BETWEEN '{sd}' AND '{ed}' " \
              "AND swanav IS NOT NULL".format(
            sd=str(date_mid), ed=str(rank_date)
        )

        fids = sorted(pd.read_sql(sql, ENGINE_RD)["fund_id"].tolist())

        p = Pool(4)
        for fid in fids:
            # print(fid)
            p.apply_async(f_calculate, (fid,), callback=f_store)
        p.close()
        p.join()

    @classmethod
    def calculate_rank(cls, rank_date):

        rk = PriRanking(rank_date)
        rank_years = (1, 2, 3, "composite")
        cols = ("y1_rank", "y2_rank", "y3_rank", "total_rank")
        rank_of_periods = [rk.rank(y) for y in rank_years]
        df_res = pd.DataFrame(rank_of_periods).T
        df_res = df_res.reset_index()
        df_res.columns = ["fund_id", *cols]
        df_res["statistic_date"] = rank_date
        io.to_sql("base.fund_rank", ENGINE_RD, df_res)

        return df_res

    @classmethod
    def calculate(cls, rank_date):
        ENGINE_RD.execute("DELETE FROM base.fund_rank WHERE statistic_date = '{sd}'".format(sd=str(rank_date)))
        cls.calculate_rar_all(rank_date)
        cls.calculate_rank(rank_date)


def main():
    LAST_MONTH = CalculateHelper.last_rank_date(dt.date.today())
    CalculateHelper.calculate(LAST_MONTH)


def test():
    import calendar as cld
    # for y, s in [(y, s) for y in range(2015, 2018) for s in (3, 6, 9, 12)]:
    for y, s in [(y, s) for y in range(2015, 2016) for s in (3,)]:
        d = dt.date(y, s, cld.monthrange(y, s)[1])
        print(d)
        CalculateHelper.calculate(d)


if __name__ == "__main__":
    test()
