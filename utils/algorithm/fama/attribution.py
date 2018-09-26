import numpy as np
from abc import ABCMeta, abstractmethod
from utils.decofactory import common


__all__ = ["IExternalAttribution"]


class IExternalAttribution(metaclass=ABCMeta):
    @property
    @abstractmethod
    def data(self):
        """
        Should return two-element tuple, (x_data, y_data)
        x_data is a DataFrame of shape (t, K), where t is number of time series periods, K is the number of regression
        factors.
        y_data is a Series of shape (t,), where t is same to x_data's

        Returns:
            tuple(pd.DataFrame, pd.Series)

        """

    @property
    def _ordered_names(self):
        """Ordered factor names of matrix"""

        return self.data[0].columns.values

    @property
    def _xdata(self):
        return self.data[0].values

    @property
    def _ydata(self):
        return self.data[1].values

    @property
    @common.unhash_inscache()
    def _model(self):
        """
        Regression model used in external attribution

        Returns:
            statsmodels.regression.linear_model.RegressionResults

        """

        from statsmodels.api import OLS, add_constant
        # intercept is not added by default in OLS implementation
        model = OLS(self._ydata, add_constant(self._xdata)).fit()

        # following api are equivalent
        # from sklearn.linear_model import LinearRegression
        # model = LinearRegression(fit_intercept=True).fit(self.xdata, self.ydata)
        # model = OLS(y_data.reshape((len(self.ydata), 1)), add_constant(self.xdata)).fit()
        return model

    @property
    def _residual_series(self):
        return self.data[1] - self._model.predict()  # y - y_bar

    @property
    def _intercept(self):
        return self._model.params[0]

    @property
    def _coef(self):
        return self._model.params[1:]

    @property
    def coef(self):
        """
        Coefficient of factors

        Returns:
            dict{
                factor name<str>: coefficient<float>
            }

        """

        return {"alpha": self._intercept, **dict(zip(self._ordered_names, self._coef))}

    @property
    def return_attribution(self):
        """
        Return attribution with K factors, and intercept

        Returns:
            dict{
                factor name<str>: return attribution<float>
            }

        """

        retattr = self._xdata.mean(axis=0) * self._coef
        return {"alpha": self._intercept, **dict(zip(self._ordered_names, retattr))}

    @property
    def risk_attribution(self):
        """
        Risk attribution with K factors, and residual var

        Returns:
            dict{
                factor name<str>: risk attribution<float>
            }

        """

        risk_alpha = self._residual_series.var(ddof=1)
        # implementation equivalent to np.cov(self.xdata.T, ddof=1),
        # which considers each column as a variable
        cov = np.cov(self._xdata, ddof=1, rowvar=False)
        risk_attr = (cov * self._coef * self._coef.reshape((len(self._coef), 1))).sum(1)

        return {"alpha": risk_alpha, **dict(zip(self._ordered_names, risk_attr))}

    @property
    def pvalue(self):
        """
        P-Value of K factors, and intercept

        Returns:
            dict{
                factor name<str>: p-value<float>
            }

        """

        return {"alpha": self._model.pvalues[0], **dict(zip(self._ordered_names, self._model.pvalues[1:]))}

    @property
    def r_square(self):
        """
        R-square

        Returns:
            float

        """

        return self._model.rsquared
