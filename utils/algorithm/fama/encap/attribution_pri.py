from dateutil.relativedelta import relativedelta
from utils.algorithm.fama.attribution import IExternalAttribution
from utils.algorithm.base.exceptions import DataError
from utils.algorithm.fama.share.datatype import Fund, Factors, TreasuryRate
from utils.algorithm.fama.share.const import FACTORS_ID_NAME_WITHOUT_SUFFIX
from utils.decofactory import common


__all__ = [
    "FamaThreeAttr", "CarhartFourAttr", "FamaThreeFactor", "DataError"
]


class FamaThreeAttr(IExternalAttribution):
    """
    调用示例
    f3 = FamaThreeFactor(dt.date((yyyy, mm, dd), "d", "your_fund_id")

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

    factor_names = FACTORS_ID_NAME_WITHOUT_SUFFIX

    def __init__(self, date, freq, fund_id):
        """
        根据所给产品, 计算日期, 频度, 构造Fama三因子外部归因模型

        Args:
            date: datetime.date
                归因日期(归因日期);

            freq: str, optional {"d", "w"}
                归因频度

            fund_id: str
                用于归因的基金产品ID

        """

        self.date = date
        self.start = date - relativedelta(months=6)
        self.portfolio = Fund(date, freq, fund_id, start=self.start)
        self.benchmark_rf = TreasuryRate(date, freq, start=self.start)
        self.factors = Factors(date, freq, ["000001", "000002", "000003"], start=self.start)

    def check_before_pre(self):
        # Check Foundation Date(or First Nv Date)
        if self.portfolio.foundation_date is not None:
            if self.date - relativedelta(months=6) < self.portfolio.foundation_date:
                raise DataError(
                    "Founded less than 6 months: foundation_date: {fd}, statistic_date: {sd}".format(
                        fd=self.portfolio.foundation_date, sd=self.date))
        else:
            raise DataError("Missing Foundation Date / First Nv Date")

        if len(self.portfolio.return_series.dropna()) < 12:
            raise DataError("Valid Sample less than 12")

    @property
    @common.unhash_inscache()
    def data(self):
        self.check_before_pre()
        x_data = self.factors.data.rename(columns=self.factor_names).fillna(method="ffill").dropna()
        y_data = (self.portfolio.return_series.fillna(method="ffill")
                  - self.benchmark_rf.return_series.fillna(method="ffill")).dropna()
        x_data, y_data = [_ for _ in x_data.align(y_data, axis=0, join="inner")]

        if len(x_data) < 12:
            raise DataError("Valid Sample less than 12 after align ")

        return x_data, y_data


class CarhartFourAttr(FamaThreeAttr):
    """
    调用示例
    f4 = FamaThreeFactor(dt.date((yyyy, mm, dd), "d", "your_fund_id")

    · 相关系数
    f4.coef

    · 收益贡献
    f4.return_attribution

    · 风险贡献
    f4.risk_attribution

    · P值
    f4.pvalue

    · 拟合优度
    f4.r_square

    """

    def __init__(self, date, freq, fund_id):
        """
        根据所给产品, 计算日期, 频度, 构造Carhart四因子外部归因模型

        Args:
            date: datetime.date
                归因日期(归因日期);

            freq: str, optional {"d", "w"}
                归因频度

            fund_id: str
                用于归因的基金产品ID

        """

        FamaThreeAttr.__init__(self, date, freq, fund_id)
        self.factors = Factors(date, freq, ["000011", "000012", "000013", "000014"], start=self.start)


class FamaFrenchFiveAttr(FamaThreeAttr):
    """
    调用示例
    f5 = FamaThreeFactor(dt.date((yyyy, mm, dd), "d", "your_fund_id")

    · 相关系数
    f5.coef

    · 收益贡献
    f5.return_attribution

    · 风险贡献
    f5.risk_attribution

    · P值
    f5.pvalue

    · 拟合优度
    f5.r_square

    """

    def __init__(self, date, freq, fund_id):
        """
        根据所给产品, 计算日期, 频度, 构造FamaFrench五因子外部归因模型

        Args:
            date: datetime.date
                归因日期(归因日期);

            freq: str, optional {"d", "w"}
                归因频度

            fund_id: str
                用于归因的基金产品ID

        """

        FamaThreeAttr.__init__(self, date, freq, fund_id)
        self.factors = Factors(date, freq, ["000021", "000022", "000023", "000024", "000025"], start=self.start)


# compatible to previous ver
FamaThreeFactor = FamaThreeAttr
