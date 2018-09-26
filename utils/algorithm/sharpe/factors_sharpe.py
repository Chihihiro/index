import numpy as np
import pandas as pd
from utils.algorithm import timeutils as tu
from utils.timeutils.const import bday_chn
from utils.decofactory import common
from scipy import optimize, stats
from abc import ABCMeta, abstractmethod


# 主数据结构
class TsProcessor:
    def __init__(self, start, end, freq, use_bday=True):
        self.start = start
        self.end = end
        self.freq = freq
        self.use_bday = use_bday

    def resample(self, dataframe):
        return tu.resample(dataframe, self.freq, weekday=4 if self.freq == "w" else None)

    def align(self, dataframe):
        freqs = {
            "d": bday_chn,
            "w": "W-FRI",
        }
        date_rng = pd.bdate_range(self.start, self.end, freq=freqs[self.freq])
        return dataframe.reindex(date_rng)

    def preprocess(self, dataframe):
        return self.align(self.resample(dataframe))


class BaseConstraints(metaclass=ABCMeta):
    """
    cons: list[dict]
    bnds: list[tuple]

    """

    def __init__(self):
        self._cons = []
        self._bnds = []
        self.initialized = False

    @abstractmethod
    def initialize(self):
        """
        Should be overwritten in subclass,
        Initialized the `_cons` and `_bnds` attribute in this method, and set `_initialized` to True after initializing

        Returns:

        """

    @property
    def cons(self):
        """

        Returns:
            list[dict], each dict should be {"fun": constraint function, "type": "eq"/"ineq"}

        """

        self.initialize()
        return self._cons

    @property
    def bnds(self):
        """

        Returns:
            list[tuple], each tuple should be (lower_bound<numeric>, upper_bound<numeric>)

        """

        self.initialize()
        return self._bnds


# 核心算法类
class SharpeFactor:
    def __init__(self, portfolio, factors, **kwargs):
        """

        Args:
            portfolio:
            factors:
            **kwargs:

        """

        self.portfolio = portfolio
        self.factors = factors

        self._tol = kwargs.get("tol", 1e-18)
        self._options = kwargs.get("options")

    @property
    @common.unhash_inscache()
    def aligned_matrixes(self):
        if len(self.portfolio.return_series.index) != len(self.factors.return_series.index)\
                or (self.portfolio.return_series.index != self.factors.return_series.index).any():  # 收益序列日期索引有任何不相同的情况
            aligned = self.portfolio.return_series.align(self.factors.return_series, join="inner")

            return aligned[0].as_matrix(), aligned[1].as_matrix()

        return self.portfolio.return_series.as_matrix(), self.factors.return_series.as_matrix()

    @property
    def mtx_pf(self):
        return self.aligned_matrixes[0]

    @property
    def mtx_factors(self):
        return self.aligned_matrixes[1]

    @property
    def constraints(self):
        return BaseConstraints()

    @classmethod
    def func_obj(cls, weights: np.array, r_portfolio: np.array, r_factors: np.array):
        """
        Optimization target function, target = Min( VaR([epsilon_1, ...,  epsilon_T]) )

        Args:
            weights: np.array[float]
                shape of ndarray should be as (N_factors, )

            r_portfolio: np.array[float]
                shape of np.array should be as (T_period, )

            r_factors: np.array[float]
                shape of np.array should be as (T_period, N_factors)

        Returns:
            scalar, Var(epsilon_t)

        """

        r_ft = (r_factors * weights).sum(axis=1)
        return (r_portfolio - r_ft).var(ddof=1)

    @property
    @common.inscache("cache")
    def solver(self):
        """
        返回模拟持仓结果

        Returns:
            scipy.optimize.optimize.OptimizeResult

        """

        res = optimize.minimize(
            self.func_obj, x0=self.factors.init_weight,
            args=(self.mtx_pf, self.mtx_factors),
            method="SLSQP", constraints=self.constraints.cons, bounds=self.constraints.bnds, tol=self._tol,
            options=self._options
        )

        return res

    @property
    @common.inscache("cache")
    def residual(self):
        """
        返回根据模拟持仓和因子收益计算投资组合收益的残差序列;

        Returns:
            np.array, shape=(T periods, )

        """

        return self.mtx_pf - (self.mtx_factors * self.solver.x).sum(axis=1)

    @property
    @common.inscache("cache")
    def residual_k(self):
        """
        Residual series of the K factors

        Returns:
            np.array, shape=(k Factors, T periods)

        """

        from statsmodels.api import OLS
        res = []
        idx_lst = list(range(self.K))
        for i in range(self.K):
            x_idx, y_idx = [*idx_lst[0: i], *idx_lst[i + 1:]], idx_lst[i: i + 1]
            x_data, y_data = self.mtx_factors[:, x_idx], self.mtx_factors[:, y_idx]
            # from sklearn.linear_model import LinearRegression
            # model = LinearRegression(fit_intercept=True).fit(x_data, y_data)

            # intercept is not added by default in OLS implement
            model = OLS(y_data, x_data).fit()
            res.append(model.resid)

        return np.array(res)

    @property
    def dof(self):
        """
        Degree of freedom

        Returns:
            int

        """

        return self.T - self.K - 1

    @property
    @common.unhash_inscache()
    def T(self):
        return len(self.mtx_pf)

    @property
    @common.unhash_inscache()
    def K(self):
        return self.mtx_factors.shape[1]

    @property
    def _sigma_w(self):
        return (self.residual.var(ddof=1) / self.residual_k.var(ddof=1, axis=1) / (self.T - self.K - 1)) ** .5

    @property
    def t_value(self):
        return self.solver.x / self._sigma_w

    @property
    def p_value(self):
        return stats.t.sf(abs(self.t_value), self.dof) * 2

    @property
    @common.unhash_inscache()
    def rsquare(self):
        return 1 - self.residual.var(ddof=1) / self.mtx_pf.var(ddof=1) * (self.T - 1) / (self.T - self.K - 1)

    @property
    def rsquare_truncated(self):
        min_, max_ = self.K / (1 - self.T + self.K), 1
        if self.rsquare > 1:
            return max_
        elif self.rsquare < min_:
            return min_
        return self.rsquare

    @property
    @common.inscache("cache")
    def ret_contri(self):
        return (self.mtx_factors * self.solver.x).mean(axis=0)

    @property
    def ret_avg(self):
        return self.mtx_pf.mean()

    @property
    @common.inscache("cache")
    def rsk_contri(self):
        var_k = np.array([
            np.cov(self.mtx_pf, self.mtx_factors[:, k], ddof=1)[0][1]  # Cov(r_fund, r_factor_k)
            for k in range(self.mtx_factors.shape[1])
        ])
        return var_k * self.solver.x

    @property
    def rsk_avg(self):
        return self.mtx_pf.var(ddof=1)  # 2018.5.14
        # return self.rsk_contri.sum() + self.residual.var(ddof=1)

    @property
    @common.inscache("cache")
    def ret_contri_ratio(self):
        """
        Return contribution Matrix of the K factors

        Returns:
            np.array, shape=(K,)

        """

        return self.ret_contri / self.ret_avg

    @property
    @common.inscache("cache")
    def rsk_contri_ratio(self):
        """
        Risk Contribution Matrix of the K factors

        Returns:
            np.array, shape=(K,)

        """

        return self.rsk_contri / self.rsk_avg
