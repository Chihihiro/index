import calendar as cld
import datetime as dt
import numpy as np
import pandas as pd
from dateutil.relativedelta import relativedelta
from utils.decofactory import common
from utils.sqlfactory.constructor import sqlfmt
from utils.algorithm.sharpe.factors_sharpe import SharpeFactor, TsProcessor, BaseConstraints
from utils.algorithm.base.exceptions import DataError


# 静态变量
class FactorClassification:
    """
    各因子所属的类别

    """

    STOCK_CODE = "stock"
    BOND_CODE = "bond"
    CASH_CODE = "cash"
    NHF_CODE = "nhf"
    FUT_CODE = "future"

    STYLE = {
        "000919.CSI": STOCK_CODE,  # 沪深300价值指数
        "000918.CSI": STOCK_CODE,  # 沪深300成长指数
        "H30351.CSI": STOCK_CODE,  # 中证500成长指数
        "H30352.CSI": STOCK_CODE,  # 中证500价值指数
        "000852.CSI": STOCK_CODE,  # 中证1000指数
        "A00221.CB": BOND_CODE,  # 中债-总指数(1-3年, 财富)
        "A00231.CB": BOND_CODE,  # 中债-总指数(3-5年, 财富)
        "A00241.CB": BOND_CODE,  # 中债-总指数(5-7年, 财富)
        "A00251.CB": BOND_CODE,  # 中债-总指数(7-10年, 财富)
        "A00261.CB": BOND_CODE,  # 中债-总指数(10年以上, 财富)
        "A00701.CB": BOND_CODE,  # 中债-信用债总指数(总值, 财富)
        "R001.CM": CASH_CODE,  # 银行间质押债
        "NHCI.NHF": NHF_CODE,  # 南华商品指数
        "H11062.CSI": FUT_CODE,  # 农产CFCI
        "H11063.CSI": FUT_CODE,  # 金属CFCI
        "H11064.CSI": FUT_CODE,  # 化工CFCI
        "H11065.CSI": FUT_CODE,  # 能源CFCI
        "H11066.CSI": FUT_CODE,  # 粮食CFCI
        "H11067.CSI": FUT_CODE,  # 工金CFCI
        "H11068.CSI": FUT_CODE,  # 纺织CFCI
        "H11069.CSI": FUT_CODE,  # 油脂CFCI
    }

    INDUSTRY = {
        '801010.SI': "农林牧渔",
        '801020.SI': '采掘',
        '801030.SI': '化工',
        '801040.SI': '钢铁',
        '801050.SI': '有色金属',
        '801080.SI': '电子元器件',
        '801110.SI': '家用电器',
        '801120.SI': '食品饮料',
        '801130.SI': '纺织服装',
        '801140.SI': '轻工制造',
        '801150.SI': '医药生物',
        '801160.SI': '公用事业',
        '801170.SI': '交通运输',
        '801180.SI': '房地产',
        '801200.SI': '商业贸易',
        '801210.SI': '餐饮旅游',
        '801230.SI': '综合',
        '801710.SI': '建筑材料',
        '801720.SI': '建筑装饰',
        '801730.SI': '电气设备',
        '801740.SI': '国防军工',
        '801750.SI': '计算机',
        '801760.SI': '传媒',
        '801770.SI': '通信',
        '801780.SI': '银行',
        '801790.SI': '非银金融',
        '801880.SI': '汽车',
        '801890.SI': '机械设备',
        "R001.CM": CASH_CODE,
    }

    INDUSTRY_28 = {
        '801010.SI': "农林牧渔",
        '801020.SI': '采掘',
        '801030.SI': '化工',
        '801040.SI': '钢铁',
        '801050.SI': '有色金属',
        '801080.SI': '电子元器件',
        '801110.SI': '家用电器',
        '801120.SI': '食品饮料',
        '801130.SI': '纺织服装',
        '801140.SI': '轻工制造',
        '801150.SI': '医药生物',
        '801160.SI': '公用事业',
        '801170.SI': '交通运输',
        '801180.SI': '房地产',
        '801200.SI': '商业贸易',
        '801210.SI': '餐饮旅游',
        '801230.SI': '综合',
        '801710.SI': '建筑材料',
        '801720.SI': '建筑装饰',
        '801730.SI': '电气设备',
        '801740.SI': '国防军工',
        '801750.SI': '计算机',
        '801760.SI': '传媒',
        '801770.SI': '通信',
        '801780.SI': '银行',
        '801790.SI': '非银金融',
        '801880.SI': '汽车',
        '801890.SI': '机械设备',
    }


class FactorUsed:
    """
    各类型基金使用的因子

    """

    STOCK = {
        k: FactorClassification.STYLE[k]
        for k in (
        "000919.CSI",  # 沪深300价值指数
        "000918.CSI",  # 沪深300成长指数
        "H30351.CSI",  # 中证500成长指数
        "H30352.CSI",  # 中证500价值指数
        "000852.CSI",  # 中证1000指数
        "R001.CM",  # 银行间质押债
    )
    }

    BOND = {
        k: FactorClassification.STYLE[k]
        for k in (
        "A00221.CB",  # 中债-总指数(1-3年, 财富)
        "A00231.CB",  # 中债-总指数(3-5年, 财富)
        "A00241.CB",  # 中债-总指数(5-7年, 财富)
        "A00251.CB",  # 中债-总指数(7-10年, 财富)
        "A00261.CB",  # 中债-总指数(10年以上, 财富)
        "A00701.CB",  # 中债-信用债总指数(总值, 财富)
        "R001.CM",  # 银行间质押债
    )
    }

    BLEND = {
        k: FactorClassification.STYLE[k]
        for k in (
        "000919.CSI",  # 沪深300价值指数
        "000918.CSI",  # 沪深300成长指数
        "H30351.CSI",  # 中证500成长指数
        "H30352.CSI",  # 中证500价值指数
        "000852.CSI",  # 中证1000指数
        "A00221.CB",  # 中债-总指数(1-3年, 财富)
        "A00231.CB",  # 中债-总指数(3-5年, 财富)
        "A00241.CB",  # 中债-总指数(5-7年, 财富)
        "A00251.CB",  # 中债-总指数(7-10年, 财富)
        "A00261.CB",  # 中债-总指数(10年以上, 财富)
        "A00701.CB",  # 中债-信用债总指数(总值, 财富)
        "R001.CM",  # 银行间质押债
    )
    }

    COMMODITY = {
        k: FactorClassification.STYLE[k]
        for k in (
        "NHCI.NHF",  # 南华商品指数
    )
    }

    FUTURE = {
        k: FactorClassification.STYLE[k]
        for k in (
        "H11062.CSI",  # 农产CFCI
        "H11063.CSI",  # 金属CFCI
        "H11064.CSI",  # 化工CFCI
        "H11065.CSI",  # 能源CFCI
        "H11066.CSI",  # 粮食CFCI
        "H11067.CSI",  # 工金CFCI
        "H11068.CSI",  # 纺织CFCI
        "H11069.CSI",  # 油脂CFCI
        "R001.CM",  # 银行间质押债
    )
    }

    SWS_28 = {
        k: FactorClassification.INDUSTRY[k]
        for k in (
        '801010.SI',
        '801020.SI',
        '801030.SI',
        '801040.SI',
        '801050.SI',
        '801080.SI',
        '801110.SI',
        '801120.SI',
        '801130.SI',
        '801140.SI',
        '801150.SI',
        '801160.SI',
        '801170.SI',
        '801180.SI',
        '801200.SI',
        '801210.SI',
        '801230.SI',
        '801710.SI',
        '801720.SI',
        '801730.SI',
        '801740.SI',
        '801750.SI',
        '801760.SI',
        '801770.SI',
        '801780.SI',
        '801790.SI',
        '801880.SI',
        '801890.SI',
        "R001.CM"
    )
    }


# 业务逻辑SQL
class DataLoader:
    from utils.database import config as cfg
    engine = cfg.load_engine()["2Gb"]  #

    def __init__(self, **kwargs):
        [setattr(self, k, v) for (k, v) in kwargs.items()]


class FundDataLoader(DataLoader):
    @common.inscache("cache")
    def load_type(self):
        sql = "SELECT stype_code FROM base_public.fund_type_mapping " \
              "WHERE fund_id = '{fid}' AND typestandard_code = '02'" \
              "AND stype_code IS NOT NULL".format(fid=self.fund_id)
        tp = self.engine.execute(sql).fetchone()

        if tp is None:
            raise DataError("Fund {fid} does not have a classification".format(fid=self.fund_id))

        return tp[0]

    @common.inscache("cache")
    def load_fundnv(self):
        sql = "SELECT statistic_date as date, swanav as value " \
              "FROM base_public.fund_nv " \
              "WHERE fund_id = '{fid}' AND statistic_date BETWEEN '{sd}' AND '{ed}'".format(
            fid=self.fund_id, sd=self.start, ed=self.end
        )
        df = pd.read_sql(sql, self.engine).set_index("date")

        return df

    @common.inscache("cache")
    def load_allnv(self):
        sql = "SELECT statistic_date as date, swanav as value " \
              "FROM base_public.fund_nv " \
              "WHERE fund_id = '{fid}' ".format(
            fid=self.fund_id
        )
        df = pd.read_sql(sql, self.engine).set_index("date")

        return df


class MIIndexDataLoader(DataLoader):
    @common.inscache("cache")
    def load_indexvalue(self):
        if len(self.index_ids) == 0:
            return None

        sql = "SELECT index_id, date, value " \
              "FROM base_finance.index_value " \
              "WHERE index_id IN ({iids}) AND date BETWEEN '{sd}' AND '{ed}'".format(
            iids=sqlfmt(self.index_ids), sd=self.start, ed=self.end)
        df = pd.read_sql(sql, self.engine).pivot(index="date", columns="index_id", values="value")

        if len(df) == 0:
            return None

        return df


class RatioIndexDataLoader(DataLoader):
    @common.inscache("cache")
    def load_indexvalue(self):
        if len(self.index_ids) == 0:
            return None

        sql = "SELECT curr_id as index_id, date, weighted_ratio as value " \
              "FROM base_finance.currency_pledge_ratio " \
              "WHERE curr_id IN ({iids}) AND date BETWEEN '{sd}' AND '{ed}'".format(
            iids=sqlfmt(self.index_ids), sd=self.start, ed=self.end)
        df = pd.read_sql(sql, self.engine).pivot(index="date", columns="index_id", values="value")

        if len(df) == 0:
            return None

        return df


# 数据结构(时间序列)
class Portfolio(TsProcessor):
    def __init__(self, fund_id, start, end, freq):
        TsProcessor.__init__(self, start, end, freq)
        self.fund_id = fund_id
        self._data_loader = FundDataLoader(fund_id=self.fund_id, start=start, end=end)

    @property
    @common.inscache("_cache")
    def price_series_whole(self):
        df_nv = self._data_loader.load_allnv()
        df_nv = self.resample(df_nv)["value"]
        return df_nv

    @property
    @common.inscache("_cache")
    def stype(self):
        return str(self._data_loader.load_type())

    @property
    def ls(self):
        return "l"  # 公募只有多头类型

    @property
    @common.inscache("_cache")
    def price_series(self):
        df_nv = self._data_loader.load_fundnv()
        df_nv = self.preprocess(df_nv)["value"]
        return df_nv

    @property
    @common.inscache("_cache")
    def return_series(self):
        nv = self.price_series.fillna(method="ffill").dropna()
        return (nv / nv.shift(1) - 1)[1:]

    @property
    @common.unhash_inscache()
    def return_mtx(self):
        return self.return_series.as_matrix()


class Factors(TsProcessor):
        def __init__(self, factor_map: dict, start, end, freq):
            TsProcessor.__init__(self, start, end, freq)
            self._factor_ids = sorted(factor_map)
            self.factor_map = factor_map
            self.start, self.end = start, end
            self.freq = freq

            # split different type of indexes
            self.val_factors = [x for x in self._factor_ids if x.split(".")[-1] not in {"CM"}]
            self.ret_factors = [x for x in self._factor_ids if x.split(".")[-1] in {"CM"}]  # CM后缀表示中国货币网的利率指数

            self._midata_loader = MIIndexDataLoader(index_ids=self.val_factors, start=self.start, end=self.end)
            self._rdata_loader = RatioIndexDataLoader(index_ids=self.ret_factors, start=self.start, end=self.end)

        @property
        @common.inscache("_cache")
        def price_series(self):
            """
            价格类型指数的价格序列(预处理后)

            Returns:

            """

            df_val = self._midata_loader.load_indexvalue()
            if df_val is None:
                return df_val

            return self.preprocess(df_val)

        @property
        def return_series_p(self):
            """
            价格类型指数的收益率序列

            Returns:

            """

            if self.price_series is None:
                return None
            df_price = self.price_series.fillna(method="ffill").dropna()

            return (df_price / df_price.shift(1) - 1)[1:]

        @property
        def return_series_r(self):
            """
            利率类型指数的收益率序列

            Returns:

            """

            periods = {"w": 52, "d": 250}
            df_ratio = self._rdata_loader.load_indexvalue()
            if df_ratio is None:
                return None

            df_ratio = df_ratio / (100 * periods[self.freq])  # single
            # df_ratio = df_ratio.applymap(lambda x: (1 + x / 100) ** (1 / periods[self.freq]) - 1)  # compounds
            return self.preprocess(df_ratio)[1:].fillna(method="ffill").dropna()

        @property
        @common.inscache("_cache")
        def return_series(self):
            """
            所有指数的收益率序列

            Returns:

            """

            r_pindexes, r_rindexes = self.return_series_p, self.return_series_r

            if r_pindexes is not None and r_rindexes is not None:
                return r_pindexes.join(self.return_series_r)
            elif r_pindexes is not None and r_rindexes is None:
                return r_pindexes
            elif r_pindexes is None and r_rindexes is not None:
                return r_rindexes
            else:
                return None

        @property
        @common.unhash_inscache()
        def return_mtx(self):
            return self.return_series.as_matrix()

        @property
        def mtx_ids(self):
            """

            Returns:
                list[str]

            """

            return self.return_series.columns.tolist()

        @property
        @common.unhash_inscache()
        def factor_type(self):
            return np.array([self.factor_map[x] for x in self.mtx_ids])

        @property
        @common.unhash_inscache()
        def type_flags(self):
            return {tp: self.factor_type == tp for tp in set(self.factor_type)}

        @property
        @common.unhash_inscache()
        def init_weight(self):
            l = len(self.mtx_ids)
            return np.array([1 / l] * l)


# 数据结构(约束)
class LsConstraints:
    def __init__(self, long_short, variable_num):
        self.ls = long_short
        self.num = variable_num

    def _init_bnds_by_ls(self):
        if self.ls == "l":
            self._bnds.extend([(0, 1)] * self.num)
        elif self.ls == "s":
            self._bnds.extend([(-1, 1)] * self.num)

    def _init_cons_by_ls(self):
        if self.ls == "l":
            self._cons.extend([
                {"type": "eq", "fun": lambda x: sum(x) - 1},
            ])
        elif self.ls == "s":
            self._cons.extend([
                {"type": "ineq", "fun": lambda x: sum(x) + 1},
                {"type": "ineq", "fun": lambda x: 1 - sum(x)},
            ])


class StyleConstraints(BaseConstraints, LsConstraints):
    def __init__(self, long_short: str, stype: str, type_flag: dict):
        BaseConstraints.__init__(self)
        LsConstraints.__init__(self, long_short, len(list(type_flag.values())[0]))

        self.ls = long_short
        self.stype = stype
        self.tf = type_flag

    def _init_cons_by_stype(self):
        # 股票型
        if self.stype in {"020101"}:
            self._cons.extend([
                {"type": "ineq", "fun": lambda x: sum(x[self.tf[FactorClassification.STOCK_CODE]]) - 0.8},
                {"type": "ineq", "fun": lambda x: sum(x[self.tf[FactorClassification.CASH_CODE]]) - 0},

            ])

        # 债券策略
        elif self.stype in {"020201", "020202"}:
            self._cons.extend([
                {"type": "ineq", "fun": lambda x: sum(x[self.tf[FactorClassification.BOND_CODE]]) - 0.8},
                {"type": "ineq", "fun": lambda x: sum(x[self.tf[FactorClassification.CASH_CODE]]) - 0},
            ])

        # 混合策略
        elif self.stype in {
                "020301", "020302", "020303", "020304", "020305"
            }:
            self._cons.extend([
                {"type": "ineq", "fun": lambda x: 0.8 - sum(x[self.tf[FactorClassification.STOCK_CODE]])},
                {"type": "ineq", "fun": lambda x: 0.8 - sum(x[self.tf[FactorClassification.BOND_CODE]])},
                {"type": "ineq", "fun": lambda x: sum(x[self.tf[FactorClassification.CASH_CODE]]) - 0},
            ])
        else:
            raise DataError("No supported fund type")

    def initialize(self):
        if not self.initialized:
            self._init_cons_by_stype()  # 初始化类型约束
            self._init_cons_by_ls()
            self._init_bnds_by_ls()
            self.initialized = True


class IndustryConstraints(BaseConstraints, LsConstraints):
    def __init__(self, long_short: str, type_flag: dict):
        BaseConstraints.__init__(self)
        LsConstraints.__init__(self, long_short, len(list(type_flag.values())[0]))

        self.tf = type_flag

    def _init_cons_by_indus(self):
        self._cons.extend([
            {"type": "ineq", "fun": lambda x: sum(x[~self.tf[FactorClassification.CASH_CODE]]) - 0.8},
            {"type": "ineq", "fun": lambda x: sum(x[self.tf[FactorClassification.CASH_CODE]]) - 0},
        ])

    def initialize(self):
        if not self.initialized:
            self._init_cons_by_indus()
            self._init_bnds_by_ls()
            self._init_cons_by_ls()
            self.initialized = True


# 私募基金接口调用
class MutSharpeFactor(SharpeFactor):
    def __init__(self, fund_id: str, start: dt.date, end: dt.date, freq: str, factor_type: str, ftypes=None, **kwargs):
        """

        Args:
            fund_id: str
            start: datetime.date, or None
            end: datetime.date
            freq: str, optional {"w", "m"}
            factor_type: str, optional {"style", "industry"}
            ftypes: dict, or None

            **kwargs:
                arguments for `scipy.optimize.minize` function, including

                tol: float, default 1e-18

                options: dict, default None
        """

        self.start, self.end = start, end
        self.factor_type = factor_type
        self.freq = freq
        if self.start is None:
            if self.factor_type == "style":
                self.start = self.end - relativedelta(months=6)
            elif self.factor_type == "industry":
                self.start = self.end - relativedelta(years=1)
            self.start = dt.date(self.start.year, self.start.month, cld.monthrange(self.start.year, self.start.month)[1]) + dt.timedelta(1)

        self.fund = Portfolio(fund_id, self.start, self.end, self.freq)
        self.sampler = Sampler(self.fund, self.freq, factor_type)
        if not self.sampler.pass_check():
            raise DataError("do not pass check")

        if ftypes is None:
            print("ftypes not set, automatically selecting...")
            # 股票型
            if self.fund.stype in {"020101"}:
                if self.factor_type == "style":
                    ftypes = FactorUsed.STOCK
                elif self.factor_type == "industry":
                    ftypes = FactorUsed.SWS_28

            # 纯债型, 混合债券型
            elif self.fund.stype in {"020201", "020202"}:
                if self.factor_type == "style":
                    ftypes = FactorUsed.BOND

            # 混合策略
            elif self.fund.stype in {
                "020301", "020302", "020303", "020304", "020305"
            }:
                if self.factor_type == "style":
                    ftypes = FactorUsed.BLEND

        if ftypes is None:
            raise DataError("Unsupported fund type for {} sharpe model".format(self.factor_type))

        self.factors = Factors(ftypes, self.start, self.end, freq)
        SharpeFactor.__init__(self, self.fund, self.factors, **kwargs)

        self.factor_type = factor_type

    @property
    @common.unhash_inscache()
    def constraints(self):
        """
        Constraints depending on long-short type, fund classification

        Returns:

        """

        if self.factor_type == "style":
            cons = StyleConstraints(self.fund.ls, self.fund.stype, self.factors.type_flags)
        elif self.factor_type == "industry":
            cons = IndustryConstraints(self.fund.ls, self.factors.type_flags)
        else:
            cons = BaseConstraints()  # 未设置约束
        return cons


# 工具类
class ResultProxy:
    def __init__(self, s: SharpeFactor):
        self.s = s

    def _format_factormtx(self, mtx):
        return pd.DataFrame(mtx.reshape((1, len(mtx))), columns=self.s.factors.mtx_ids)

    @property
    def frame_coef(self):
        df = self._format_factormtx(self.s.solver.x)
        df["resd"] = self.s.residual.mean()
        df["rsquare"] = self.s.rsquare
        # df["notnull"] = (self.s.portfolio.price_series.notnull() & self.s.factors.price_series.notnull().all(1)).sum()
        df["notnull"] = len(self.s.mtx_pf)
        return df

    @property
    def frame_coef_grouped(self):
        w = self.s.solver.x
        df = pd.DataFrame.from_dict({
            tp: sum(w[self.s.factors.type_flags[tp]])
            for tp in self.s.factors.type_flags
        }, orient="index").T
        df["resd"] = self.s.residual.mean()  # equivalent to s.residual.var()
        df["rsquare"] = self.s.rsquare
        df["notnull"] = (self.s.portfolio.price_series.notnull() & self.s.factors.price_series.notnull().all(1)).sum()
        return df

    @property
    def frame_pval(self):
        return self._format_factormtx(self.s.p_value)

    @property
    def frame_retcontri(self):
        tmp = self._format_factormtx(self.s.ret_contri)
        tmp["alpha"] = self.s.ret_avg - self.s.ret_contri.sum()
        tmp["ret_avg"] = self.s.ret_avg

        return tmp

    @property
    def frame_rskcontri(self):
        tmp = self._format_factormtx(self.s.rsk_contri)
        tmp["alpha"] = self.s.rsk_avg - self.s.rsk_contri.sum()
        tmp["rsk_avg"] = self.s.rsk_avg

        return tmp

    @property
    def frame_table1(self):
        w = self.s.solver.x
        # df_grouped = [[tp, sum(w[self.s.factors.type_flags[tp]])] for tp in self.s.factors.type_flags]
        #
        df_res = pd.DataFrame([self.s.factors.mtx_ids, self.s.solver.x.tolist(), self.s.p_value.tolist()]).T
        # df_res = df_res.append(df_grouped)
        df_res.columns = ["factor_id", "weight", "p_value"]
        df_res["factor_type"] = df_res["factor_id"].apply(lambda x: self.s.factors.factor_map[x])
        df_res["fund_id"] = self.s.portfolio.fund_id
        df_res["date"] = self.s.end
        df_res["r_square"] = self.s.rsquare
        df_res["r_square_trunc"] = self.s.rsquare_truncated
        df_res["residual"] = self.s.residual.mean()
        df_res["sample_num"] = len(self.s.mtx_factors)

        return df_res

    @property
    def frame_table2(self):
        df_res = pd.DataFrame([self.s.factors.mtx_ids, self.s.ret_contri.tolist(), self.s.rsk_contri.tolist()]).T
        df_res.columns = ["factor_id", "ret_contri", "rsk_contri"]

        df_res["ret_residual"] = self.s.ret_avg - self.s.ret_contri.sum()
        df_res["rsk_residual"] = self.s.ret_avg - self.s.ret_contri.sum()
        df_res["ret_avg"] = self.s.ret_avg
        df_res["rsk_avg"] = self.s.rsk_avg
        df_res["fund_id"] = self.s.portfolio.fund_id
        df_res["date"] = self.s.end

        return df_res

    def QUICK_TEST(self):
        """
        only for testing

        Returns:

        """
        print("Sampler Check: ", self.s.sampler.pass_check())
        print("Optimization Convergence: ", self.s.solver.success)
        print("Residual equal to optimize target: ", self.s.residual.var(ddof=1) == self.s.solver.fun)


# 数据检查
class Sampler:
    def __init__(self, fund, freq: str, factor_type: str):
        """

        Args:
            fund: Fund
                fund instance

            freq: str, optional {"d", "w"}
            factor_type: str, optional {"style", "industry"}

        """

        self.fund = fund
        self.freq = freq
        self.factor_type = factor_type

    @property
    def _first_month_end(self):
        y, m = self.fund.start.year, self.fund.start.month
        return dt.datetime(y, m, cld.monthrange(y, m)[1])

    @property
    def _last_month_start(self):
        y, m = self.fund.end.year, self.fund.end.month
        return dt.datetime(y, m, 1)

    def check_totalnv_len(self):
        min_lengths = {"w": {"style": 24, "industry": 52}, "d": {"style": 24, "industry": 52}}
        n = len(self.fund.price_series_whole.dropna())
        N = min_lengths[self.freq][self.factor_type]
        if n < N:
            raise DataError("Total NV length doesn't satisfy, {n}/{N}".format(n=n, N=N))

        return True

    def check_nv(self):
        # check whether first/last period is missing
        ps = self.fund.price_series
        if (~ps[ps.index[ps.index <= self._first_month_end]].notnull()).all() \
                or (~ps[ps.index[ps.index >= self._last_month_start]].notnull()).all():
            raise DataError("First/Last-period NV Missing")

        # check whether all return equals 0
        if (self.fund.return_series.dropna() == 0).all():
            raise DataError("All return equal to Zero")

        min_lengths = {"w": {"style": 12, "industry": 24}, "d": {"style": 12, "industry": 24}}
        n = len(self.fund.price_series.dropna())
        if n < min_lengths[self.freq][self.factor_type]:
            raise DataError("Data Missing Too much, only got {n}".format(n=n))

        return True

    def pass_check(self):
        if self.factor_type in {"style", "industry"}:
            # EFTP style check
            try:
                return self.check_nv() & self.check_totalnv_len()

            except DataError as err:
                print("Not Pass Check", err)
                return False

        return True  # 未设置样本检查的归因类型, 默认为通过检查;


def main():
    from utils.algorithm.sharpe.test import pprint
    fid, s, e, freq, factor_type = "000008", None, dt.date(2018, 5, 31), "d", "industry"
    res = MutSharpeFactor(fid, s, e, freq, factor_type, tol=1e-18, options={"maxiter": 1000})
    pprint(res)


if __name__ == '__main__':
    main()
