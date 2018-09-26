import datetime as dt

import pandas as pd
from dateutil.relativedelta import relativedelta

from utils.algorithm.fama import factors_fama3 as ff3


class FamaThreeFactor:
    """
    调用示例
    f3 = FamaThreeFactor(dt.date((yyyy, mm, dd), "d", "JR000001")

    · 相关系数
    f3.coef

    · 收益贡献
    f3.return_attribution

    · 风险贡献
    f3.risk_attribution

    · P值
    f3.pvalue

    · 拟合优度
    f3.r_square

    """
    from collections import OrderedDict
    FACTORS = OrderedDict([
        ("000003", "MKT"),
        ("000001", "SMB",),
        ("000002", "HML",),
    ])

    def __init__(self, statistic_date, freq, id):
        """
        根据所给产品, 计算日期, 频度, 构造三因子归因实例

        Args:
            statistic_date: datetime.date
                三因子归因日期(归因日期);

            freq: str, optional {"d", "w"}
                归因频度

            id: str
                用于归因的基金产品ID

        """

        self._id = id
        self._statistic_date = statistic_date
        self._freq = freq
        self._lower_bound = self._statistic_date - relativedelta(months=6)
        self._weekdays = {0: "MON", 1: "TUE", 2: "WED", 3: "THU", 4: "FRI", 5: "SAT", 6: "SUN"}
        if freq == "w":
            self._date_range = pd.date_range(self._lower_bound, self._statistic_date,
                                             freq="W-{wd}".format(wd=self._weekdays[self._statistic_date.weekday()]))
        elif freq == "d":
            self._date_range = pd.date_range(self._lower_bound, self._statistic_date, freq="B")

        self._fund = ff3.Fund(statistic_date, freq, id)
        self._factor = ff3.Factors(statistic_date, freq, list(self.FACTORS.keys()))
        self._cached = {}

    @property
    def data(self):
        """
            获取并缓存计算所需的基金收益, 因子序列数据.

        Returns:
            tuple(x_data<ndarray>, y_data<ndarray>)

        """

        if "data" not in self._cached:
            y_data = self._fund.excess_return_series()
            x_data = self._factor.value_series().rename(columns=self.FACTORS).reindex(columns=list(self.FACTORS.values())).fillna(method="ffill")
            # num_periods_std = len(self._date_range)
            periods_x, periods_y = set(x_data.index), set(y_data.index)
            dates_used = sorted(periods_x.intersection(periods_y))
            num_periods_used = len(dates_used)

            # Check Foundation Date(or First Nv Date)
            if self._fund.foundation_date() is not None:
                if self._statistic_date - relativedelta(months=6) < self._fund.foundation_date():
                    raise ValueError(
                        "Founded less than 6 months: foundation_date: {fd}, statistic_date: {sd}".format(
                            fd=self._fund.foundation_date(), sd=self._statistic_date
                        )
                    )
            else:
                raise ValueError("Missing Foundation Date / First Nv Date")

            if num_periods_used < 12:
                    raise ValueError("样本个数小于12个")
            x_data, y_data = x_data.loc[dates_used].as_matrix(), y_data.loc[dates_used].as_matrix()
            self._cached["data"] = (x_data, y_data)

        return self._cached["data"]

    @property
    def model(self):
        """
        实例, 缓存并返回计算中使用的回归模型

        Returns:
            statsmodels.regression.linear_model.RegressionResults

        """

        if "model" not in self._cached:
            from statsmodels.api import OLS, add_constant
            x_data, y_data = self.data
            from sklearn.linear_model import LinearRegression
            self._cached["model2"] = LinearRegression(fit_intercept=True).fit(x_data, y_data)

            # intercept is not added by default in OLS implement
            self._cached["model"] = OLS(y_data, add_constant(x_data)).fit()

        return self._cached["model"]

    def _residual_series(self):

        x_data, y_data = self.data
        resd = [x[0] for x in y_data] - self.model.predict()

        return resd

    def _intercept(self):

        return self.model.params[0]

    def _coef(self):

        return self.model.params[1:]

    def coef(self):
        """
        相关系数

        Returns:
            dict{
                因子名称<str>: 相关系数<float>
            }

        """

        return {"alpha": self._intercept(), **dict(zip(self.FACTORS.values(), self._coef()))}

    def return_attribution(self):
        """
        收益分解

        Returns:
            dict{
                因子名称<str>: 收益贡献度<float>
            }

        """

        return_mean = self.data[0].mean(axis=0)

        return {"alpha": self._intercept(), **dict(zip(self.FACTORS.values(), return_mean * self._coef()))}

    def risk_attribution(self):
        """
        风险分解

        Returns:
            dict{
                因子名称<str>: 风险贡献度<float>
            }

        """

        cov = pd.DataFrame(self.data[0]).cov().as_matrix()
        beta_im, beta_is, beta_ih = [self.coef()[k] for k in self.FACTORS.values()]
        epsilon = self._residual_series()

        risk_MKT = (beta_im ** 2 * cov[0, 0]) + (beta_is * beta_im * cov[1, 0]) + (beta_ih * beta_im * cov[2, 0])
        risk_SMB = (beta_is ** 2 * cov[1, 1]) + (beta_im * beta_is * cov[0, 1]) + (beta_ih * beta_is * cov[2, 1])
        risk_HML = (beta_ih ** 2 * cov[2, 2]) + (beta_im * beta_ih * cov[0, 2]) + (beta_is * beta_ih * cov[1, 2])
        risk_alpha = epsilon.var()

        return {"MKT": risk_MKT, "SMB": risk_SMB, "HML": risk_HML, "alpha": risk_alpha}

    @property
    def pvalue(self):
        """
        返回回归模型的p值

        Returns:
            dict{
            因子名称<str>: 因子p-value<float>
            }

        """

        return {"alpha": self.model.pvalues[0], **dict(zip(self.FACTORS.values(), self.model.pvalues[1:]))}

    @property
    def r_square(self):
        """
        返回回归模型的拟合优度

        Returns:
            float

        """

        return self.model.rsquared


def test():
    ff = FamaThreeFactor(dt.date(2018, 2, 28), "d", "JR000063")
    print(ff.coef())
    print(ff.return_attribution())
    print(ff.risk_attribution())
    print(ff.pvalue)
    print(ff.r_square)


if __name__ == "__main__":
    test()
