import pandas as pd
from utils.algorithm.fama.factors_fama import (
    FamaStyleFactor, CarhartStyleFactor, FamaFrenchStyleFactor, MarketFactor)
from utils.algorithm.fama.share import const


class ResultProxy:
    factor_id = const.FACTORS_NAME_ID
    factor_name_suffix = ""

    def __init__(self, *args):
        self.args = args

    @classmethod
    def _dict2frame(cls, d):
        return pd.DataFrame.from_dict(d).T.stack().reset_index()

    @property
    def result(self):
        d0 = self._dict2frame(self.args[0].factor)
        for d in self.args[1:]:
            d0 = d0.append(self._dict2frame(d.factor))

        d0.columns = ["date", "factor_name", "factor_value"]
        d0["factor_name"] = d0["factor_name"].apply(lambda x: x + self.factor_name_suffix)
        d0["factor_id"] = d0["factor_name"].apply(lambda x: self.factor_id.get(x))
        del d0["factor_name"]
        return d0


class Fama3(ResultProxy):
    factor_name_suffix = "(Fama)"

    def __init__(self, end, freq):
        f1 = FamaStyleFactor(end, freq)
        f2 = MarketFactor(end, freq)
        ResultProxy.__init__(self, f1, f2)


class Carhart4(ResultProxy):
    factor_name_suffix = "(Carhart)"

    def __init__(self, end, freq):
        f1 = CarhartStyleFactor(end, freq)
        f2 = MarketFactor(end, freq)
        ResultProxy.__init__(self, f1, f2)


class FamaFrench5(ResultProxy):
    factor_name_suffix = "(FamaFrench)"

    def __init__(self, end, freq):
        f1 = FamaFrenchStyleFactor(end, freq)
        f2 = MarketFactor(end, freq)
        ResultProxy.__init__(self, f1, f2)
