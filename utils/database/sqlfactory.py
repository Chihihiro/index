import calendar as cld
import datetime as dt
from dateutil.relativedelta import relativedelta
from sqlalchemy import create_engine
from collections import Iterable

_NV = "swanav"


class CreateEngine:
    def __init__(self, ip="120.55.69.127", port="3306", usr="jr_admin_1",
                 pwd="b77c4131fead5885374a2c182bff9baf1a66a80c", dialect="mysql", driver="pymysql", dbname="base",
                 **kwargs):
        self.__connect_args = kwargs
        if dialect == "mysql" and "charset" not in self.__connect_args.keys():
            self.__connect_args["charset"] = "utf8"
        self.__usr = usr
        self.__pwd = pwd
        self.__url = "{0}+{1}://{2}:{3}@{4}:{5}/{6}".format(dialect, driver, self.__usr, self.__pwd, ip, port, dbname)
        self.__engine = create_engine(self.__url, connect_args=self.__connect_args)

    @property
    def engine(self):
        return self.__engine

    @property
    def connect_args(self):
        return self.__connect_args


class Table:
    def __init__(self, freq, table):
        self.__freq = freq
        self.__table = table
        self.__table_name = {
            "w": {
                "return": "fund_weekly_return",  #
                "risk": "fund_weekly_risk",  #
                "sub": "fund_subsidiary_weekly_index",  #
                "bm": "index_weekly_table",
                "index": "fund_weekly_index",
                "indicator": "fund_weekly_indicator"
            },

            "m": {
                "return": "fund_month_return",
                "risk": "fund_month_risk",
                "sub": "fund_subsidiary_month_index",
                "oreturn": "org_month_return",
                "orisk": "org_month_risk",
                "oresearch": "org_month_research",
                "oroutine": "org_month_routine",
                "bm": "index_month_table",
                "index": "fund_month_index",
                "indicator": "fund_month_indicator"
            }
        }

    @property
    def intervals(self):
        return [1, 3, 6, 12, 24, 36, 60, "w", "m", "q", "a"]

    @property
    def name(self):
        return self.__table_name[self.__freq][self.__table]

    def columns(self):
        interval_ = {1: "m1", 3: "m3", 6: "m6", 12: "y1", 24: "y2", 36: "y3", 60: "y5", "w": "week", "m": "month",
                     "q": "quarter", "a": "year", "whole": "total"}
        if self.__table == "return":
            if self.__freq == "w":
                _indicator = {
                    1: ["return"],
                    2: ["return_a", "excess_a"],
                    3: ["odds", "sharp_a", "calmar_a", "sor_a", "inf_a", "jensen_a", "tr_a"]
                }
                _intervals = {
                    1: [1, 3, 6, 12, 24, 36, 60, "w", "m", "q", "a", "whole"],
                    2: [1, 3, 6, 12, 24, 36, 60, "m", "q", "a", "whole"],
                    3: [1, 3, 6, 12, 24, 36, 60, "a", "whole"]
                }
            elif self.__freq == "m":
                _indicator = {
                    4: ["return", "return_a", "excess_a"],
                    5: ["odds", "sharp_a", "calmar_a", "sor_a", "inf_a", "jensen_a", "tr_a"]
                }
                _intervals = {
                    4: [3, 6, 12, 24, 36, 60, "q", "a", "whole"],
                    5: [3, 6, 12, 24, 36, 60, "a", "whole"]
                }

        elif self.__table == "risk":
            if self.__freq == "w":
                _indicator = {3: ["stdev", "stdev_a", "dd_a", "max_retracement", "beta", "benchmark_r"]}
                _intervals = {3: [1, 3, 6, 12, 24, 36, 60, "a", "whole"]}
            elif self.__freq == "m":
                _indicator = {5: ["stdev", "stdev_a", "dd_a", "max_retracement", "beta", "benchmark_r"]}
                _intervals = {5: [3, 6, 12, 24, 36, 60, "a", "whole"]}

        elif self.__table == "sub":
            if self.__freq == "w":
                _indicator = {3: ["con_rise_weeks", "con_fall_weeks", "s_time", "s_security", "persistence",
                                  "odds", "unsys_risk", "tracking_error_a", "rvalue", "p_earning_weeks",
                                  "n_earning_weeks",
                                  "min_rate_of_return", "max_rate_of_return", "mdd_repair_time", "mdd_time", "skewness",
                                  "kurtosis",
                                  "rvalue_adjustment_ratio"]}
                _intervals = {3: [1, 3, 6, 12, 24, 36, 60, "a", "whole"]}

            if self.__freq == "m":
                _indicator = {5: ["con_rise_months", "con_fall_months", "s_time", "s_security", "persistence",
                                  "odds", "unsys_risk", "tracking_error_a", "rvalue", "p_earning_months",
                                  "n_earning_months",
                                  "min_rate_of_return", "max_rate_of_return", "mdd_repair_time", "mdd_time", "skewness",
                                  "kurtosis",
                                  "rvalue_adjustment_ratio"]}
                _intervals = {5: [3, 6, 12, 24, 36, 60, "a", "whole"]}

        elif self.__table == "bm":
            if self.__freq == "w":
                _indicator = {1: ["return", "return_a"],
                              3: ["stdev_a", "dd_a", "max_retracement", "sharp_a", "calmar_a", "sor_a",
                                  "p_earning_weeks", "n_earning_weeks", "con_rise_weeks", "con_fall_weeks"]}

                _intervals = {1: [1, 3, 6, 12, 24, 36, 60, "w", "m", "q", "a", "whole"],
                              3: [1, 3, 6, 12, 24, 36, 60, "a", "whole"]}

            elif self.__freq == "m":
                _indicator = {2: ["return", "return_a"],
                              3: ["stdev_a", "dd_a", "max_retracement", "sharp_a", "calmar_a", "sor_a",
                                  "p_earning_months", "n_earning_months", "con_rise_months", "con_fall_months"]}

                _intervals = {2: [3, 6, 12, 24, 36, 60, "q", "a", "whole"],
                              3: [3, 6, 12, 24, 36, 60, "a", "whole"]}

        elif self.__table == "oreturn":
            _indicator = {4: ["return", "return_a", "excess_a"],
                          5: ["calmar_a", "sor_a", "tr_a", "in_a", "jense_a", "sharp_a"]}

            _intervals = {4: [3, 6, 12, 24, 36, 60, "q", "a", "whole"],
                          5: [3, 6, 12, 24, 36, 60, "a", "whole"]}

        elif self.__table == "orisk":
            _indicator = {5: ["stdev_a", "dd_a", "beta", "strategy_r", "mdd", "mdd_time", "structured_ratio", "rvalue"]}

            _intervals = {5: [3, 6, 12, 24, 36, 60, "a", "whole"]}

        elif self.__table == "oresearch":
            _indicator = {5: ["odds", "persistence"],
                          6: ["s_time", "s_security"]}

            _intervals = {5: [3, 6, 12, 24, 36, 60, "a", "whole"],
                          6: [6, 12, 24, 36, 60, "a", "whole"]}

        elif self.__table == "indicator":
            if self.__freq == "w":
                _indicator = {
                    1: ["return"],
                    2: ["return_a", "excess_a"],
                    3: ["odds", "sharp_a", "calmar_a", "sor_a", "inf_a", "jensen_a", "tr_a", "stdev_a",
                        "dd_a", "max_retracement", "beta", "s_time", "s_security", "persistence",
                        "odds", "rvalue", "p_earning_weeks", "n_earning_weeks"]
                    # 4: ["return_a", "excess_a", "odds", "sharp_a", "calmar_a", "sor_a", "inf_a", "jensen_a", "tr_a", "stdev_a",
                    #     "dd_a", "max_retracement", "beta", "s_time", "s_security", "persistence",
                    #     "odds", "rvalue", "p_earning_weeks", "n_earning_weeks"]

                }
                _intervals = {
                    1: [1, 3, 6, 12, 24, 36, 60, "w", "m", "q", "a", "whole"],
                    2: [1, 3, 6, 12, 24, 36, 60, "q", "a", "whole"],
                    3: [1, 3, 6, 12, 24, 36, 60, "a", "whole"],
                    # 4: [1, 3, 6, 12, 24, 36, 60, "a", "whole"]
                }
            elif self.__freq == "m":
                _indicator = {
                    4: ["return"],  # return_a, excess_a -> 5
                    5: ["return_a", "excess_a", "odds", "sharp_a", "calmar_a", "sor_a", "inf_a", "jensen_a", "tr_a", "stdev_a",
                        "dd_a", "max_retracement", "beta", "s_time", "s_security", "persistence",
                        "odds", "rvalue", "p_earning_months", "n_earning_months"]
                }
                _intervals = {
                    4: [3, 6, 12, 24, 36, 60, "q", "a", "whole"],
                    5: [3, 6, 12, 24, 36, 60, "a", "whole"]
                }

        _cols = []
        for k in _indicator.keys():
            for _ind in _indicator[k]:
                for _int in _intervals[k]:
                    _cols.append("{0}_{1}".format(interval_[_int], _ind))
        return _cols


class PEIndex:
    def __init__(self, idx=None):
        self.__idx = idx

        self.__id = dict([(i, "FI" + "0" * (1 - i // 10) + str(i)) for i in range(1, 14)])

        self.__name = {
            1: "私募全市场指数", 2: "阳光私募指数", 3: "私募FOF指数",
            4: "股票多头策略私募指数", 5: "股票多空策略私募指数", 6: "市场中性策略私募指数",
            7: "债券基金私募指数", 8: "管理期货策略私募指数", 9: "宏观策略私募指数",
            10: "事件驱动策略私募指数", 11: "相对价值策略私募指数", 12: "多策略私募指数", 13: "组合投资策略私募指数"
        }

        self.__typestandard = {
            1: {"code": 600, "name": "按全产品"},
            2: {"code": 604, "name": "按发行主体分类"},
            3: {"code": 601, "name": "按投资策略分类"},
            4: {"code": 601, "name": "按投资策略分类"},
            5: {"code": 601, "name": "按投资策略分类"},
            6: {"code": 601, "name": "按投资策略分类"},
            7: {"code": 601, "name": "按投资策略分类"},
            8: {"code": 601, "name": "按投资策略分类"},
            9: {"code": 601, "name": "按投资策略分类"},
            10: {"code": 601, "name": "按投资策略分类"},
            11: {"code": 601, "name": "按投资策略分类"},
            12: {"code": 601, "name": "按投资策略分类"},
            13: {"code": 601, "name": "按投资策略分类"}
        }

        self.__type = {
            1: {"code": 60001, "name": "全产品"},
            2: {"code": 60401, "name": "信托"},
            3: {"code": 60107, "name": "组合策略"},
            4: {"code": 60101, "name": "股票策略"},
            5: {"code": 60101, "name": "股票策略"},
            6: {"code": 60101, "name": "股票策略"},
            7: {"code": 60105, "name": "债券策略"},
            8: {"code": 60102, "name": "管理期货"},
            9: {"code": 60106, "name": "宏观策略"},
            10: {"code": 60104, "name": "事件驱动"},
            11: {"code": 60103, "name": "相对价值"},
            12: {"code": 60108, "name": "多策略"},
            13: {"code": 60107, "name": "组合策略"}
        }

        self.__stype = {
            1: {"code": 6000101, "name": "全产品"},
            3: {"code": 6010702, "name": "FOF"},
            4: {"code": 6010101, "name": "股票多头"},
            5: {"code": 6010102, "name": "股票多空"},
            6: {"code": 6010103, "name": "市场中性"},
        }

        self.__firstyear = {
            1: 2007, 2: 2007, 3: 2015, 4: 2007, 5: 2013, 6: 2013, 7: 2012,
            8: 2014, 9: 2014, 10: 2012, 11: 2014, 12: 2014, 13: 2011
        }

        self.__firstmonday = dict(
            [(i, dt.datetime(self.__firstyear[i], 1, 1 + (7 - cld.monthrange(self.__firstyear[i], 1)[0]) % 7)) for i in
             range(1, 14)])

    @property
    def idx(self):
        return self.__idx

    @property
    def id(self):
        return self.__id[self.__idx] if self.__idx is not None else self.__id

    @property
    def name(self):
        return self.__name[self.__idx] if self.__idx is not None else self.__name

    @property
    def typestandard(self):
        return self.__typestandard[self.__idx] if self.__idx is not None else self.__typestandard

    @property
    def type(self):
        return self.__type[self.__idx] if self.__idx is not None else self.__type

    @property
    def stype(self):
        if self.__idx in self.__stype.keys():
            return self.__stype[self.__idx] if self.__idx is not None else self.__stype
        else:
            return {"code": None, "name": None}

    @property
    def firstyear(self):
        return self.__firstyear[self.__idx] if self.__idx is not None else self.__firstyear

    @property
    def firstmonday(self):
        return self.__firstmonday[self.__idx] if self.__idx is not None else self.__firstmonday


class Time:
    '''
    '''

    def __init__(self, now=dt.datetime.now()):
        self.__now = now
        self.__timetuple = now.timetuple()
        self.__today = dt.date(self.__timetuple.tm_year, self.__timetuple.tm_mon, self.__timetuple.tm_mday)
        self.__month_range = cld.monthrange(self.__timetuple.tm_year, self.__timetuple.tm_mon)[1]
        self.__weekday = cld.weekday(self.__timetuple.tm_year, self.__timetuple.tm_mon, self.__timetuple.tm_mday)

    @property
    def now(self):
        return self.__now

    @property
    def timetuple(self):
        return self.now.timetuple()

    @property
    def year(self):
        return self.__timetuple.tm_year

    @property
    def month(self):
        return self.__timetuple.tm_mon

    @property
    def mday(self):
        return self.__timetuple.tm_mday

    @property
    def today(self):
        return self.__today

    @property
    def month_range(self):
        return self.__month_range

    @property
    def weekday(self):
        return self.__weekday


class Map:
    # mp_interval = {
    #     1: "m1",
    #     3: "m3",
    #     6: "m6",
    #     12: "y1",
    #     24: "y2",
    #     36: "y3",
    #     60: "y5",
    #     "w": "week",
    #     "m": "month",
    #     "q": "quarter",
    #     "a": "year",
    #     "whole": "total"
    # }
    # mp_ind = {
    #     "d": {
    #         "accumulative_return": "return",
    #         "return_a": "return_a",
    #         "excess_return_a": "excess_a",
    #         "odds": "odds",
    #         "sharpe_a": "sharp_a",
    #         "calmar_a": "calmar_a",
    #         "sortino_a": "sor_a",
    #         "info_a": "inf_a",
    #         "jensen_a": "jensen_a",
    #         "treynor_a": "tr_a",
    #         "stdev": "stdev",
    #         "stdev_a": "stdev_a",
    #         "downside_deviation_a": "dd_a",
    #         "max_drawdown": "max_retracement",
    #         "beta": "beta",
    #         "corr": "benchmark_r",
    #         "con_rise_periods": "con_rise_days",
    #         "con_fall_periods": "con_fall_days",
    #         "ability_timing": "s_time",
    #         "ability_security": "s_security",
    #         "persistence": "persistence",
    #         "unsystematic_risk": "unsys_risk",
    #         "tracking_error_a": "tracking_error_a",
    #         "VaR": "rvalue",
    #         "p_earning_periods": "p_earning_days",
    #         "n_earning_periods": "n_earning_days",
    #         "min_return": "min_rate_of_return",
    #         "max_return": "max_rate_of_return",
    #         "mdd_repair_time": "mdd_repair_time",
    #         "mdd_time": "mdd_time",
    #         "skewness": "skewness",
    #         "kurtosis": "kurtosis",
    #         "ERVaR": "rvalue_adjustment_ratio"
    #     },
    #     "w": {
    #         "accumulative_return": "return",
    #         "return_a": "return_a",
    #         "excess_return_a": "excess_a",
    #         "odds": "odds",
    #         "sharpe_a": "sharp_a",
    #         "calmar_a": "calmar_a",
    #         "sortino_a": "sor_a",
    #         "info_a": "inf_a",
    #         "jensen_a": "jensen_a",
    #         "treynor_a": "tr_a",
    #         "stdev": "stdev",
    #         "stdev_a": "stdev_a",
    #         "downside_deviation_a": "dd_a",
    #         "max_drawdown": "max_retracement",
    #         "beta": "beta",
    #         "corr": "benchmark_r",
    #         "con_rise_periods": "con_rise_weeks",
    #         "con_fall_periods": "con_fall_weeks",
    #         "ability_timing": "s_time",
    #         "ability_security": "s_security",
    #         "persistence": "persistence",
    #         "unsystematic_risk": "unsys_risk",
    #         "tracking_error_a": "tracking_error_a",
    #         "VaR": "rvalue",
    #         "p_earning_periods": "p_earning_weeks",
    #         "n_earning_periods": "n_earning_weeks",
    #         "min_return": "min_rate_of_return",
    #         "max_return": "max_rate_of_return",
    #         "mdd_repair_time": "mdd_repair_time",
    #         "mdd_time": "mdd_time",
    #         "skewness": "skewness",
    #         "kurtosis": "kurtosis",
    #         "ERVaR": "rvalue_adjustment_ratio"
    #     },
    #     "m": {
    #         "accumulative_return": "return",
    #         "return_a": "return_a",
    #         "excess_return_a": "excess_a",
    #         "odds": "odds",
    #         "sharpe_a": "sharp_a",
    #         "calmar_a": "calmar_a",
    #         "sortino_a": "sor_a",
    #         "info_a": "inf_a",
    #         "jensen_a": "jensen_a",
    #         "treynor_a": "tr_a",
    #         "stdev": "stdev",
    #         "stdev_a": "stdev_a",
    #         "downside_deviation_a": "dd_a",
    #         "max_drawdown": "max_retracement",
    #         "beta": "beta",
    #         "corr": "benchmark_r",
    #         "con_rise_periods": "con_rise_months",
    #         "con_fall_periods": "con_fall_months",
    #         "ability_timing": "s_time",
    #         "ability_security": "s_security",
    #         "persistence": "persistence",
    #         "unsystematic_risk": "unsys_risk",
    #         "tracking_error_a": "tracking_error_a",
    #         "VaR": "rvalue",
    #         "p_earning_periods": "p_earning_months",
    #         "n_earning_periods": "n_earning_months",
    #         "min_return": "min_rate_of_return",
    #         "max_return": "max_rate_of_return",
    #         "mdd_repair_time": "mdd_repair_time",
    #         "mdd_time": "mdd_time",
    #         "skewness": "skewness",
    #         "kurtosis": "kurtosis",
    #         "ERVaR": "rvalue_adjustment_ratio"
    #     }
    # }

    class Private:
        mp_interval = {
            1: "m1",
            3: "m3",
            6: "m6",
            12: "y1",
            24: "y2",
            36: "y3",
            60: "y5",
            "w": "week",
            "m": "month",
            "q": "quarter",
            "a": "year",
            "whole": "total"
        }
        mp_ind = {
            "d": {
                "accumulative_return": "return",
                "return_a": "return_a",
                "excess_return_a": "excess_a",
                "odds": "odds",
                "sharpe_a": "sharp_a",
                "calmar_a": "calmar_a",
                "sortino_a": "sor_a",
                "info_a": "inf_a",
                "jensen_a": "jensen_a",
                "treynor_a": "tr_a",
                "stdev": "stdev",
                "stdev_a": "stdev_a",
                "downside_deviation_a": "dd_a",
                "max_drawdown": "max_retracement",
                "beta": "beta",
                "corr": "benchmark_r",
                "con_rise_periods": "con_rise_days",
                "con_fall_periods": "con_fall_days",
                "ability_timing": "s_time",
                "ability_security": "s_security",
                "persistence": "persistence",
                "unsystematic_risk": "unsys_risk",
                "tracking_error_a": "tracking_error_a",
                "VaR": "rvalue",
                "p_earning_periods": "p_earning_days",
                "n_earning_periods": "n_earning_days",
                "min_return": "min_rate_of_return",
                "max_return": "max_rate_of_return",
                "mdd_repair_time": "mdd_repair_time",
                "mdd_time": "mdd_time",
                "skewness": "skewness",
                "kurtosis": "kurtosis",
                "ERVaR": "rvalue_adjustment_ratio",
            },
            "w": {
                "accumulative_return": "return",
                "return_a": "return_a",
                "excess_return_a": "excess_a",
                "odds": "odds",
                "sharpe_a": "sharp_a",
                "calmar_a": "calmar_a",
                "sortino_a": "sor_a",
                "info_a": "inf_a",
                "jensen_a": "jensen_a",
                "treynor_a": "tr_a",
                "stdev": "stdev",
                "stdev_a": "stdev_a",
                "downside_deviation_a": "dd_a",
                "max_drawdown": "max_retracement",
                "beta": "beta",
                "corr": "benchmark_r",
                "con_rise_periods": "con_rise_weeks",
                "con_fall_periods": "con_fall_weeks",
                "ability_timing": "s_time",
                "ability_security": "s_security",
                "persistence": "persistence",
                "unsystematic_risk": "unsys_risk",
                "tracking_error_a": "tracking_error_a",
                "VaR": "rvalue",
                "p_earning_periods": "p_earning_weeks",
                "n_earning_periods": "n_earning_weeks",
                "min_return": "min_rate_of_return",
                "max_return": "max_rate_of_return",
                "mdd_repair_time": "mdd_repair_time",
                "mdd_time": "mdd_time",
                "skewness": "skewness",
                "kurtosis": "kurtosis",
                "ERVaR": "rvalue_adjustment_ratio",
                #v2
                "ddr3_a": "ddr3_a",
                "pain_index": "pain_index",
                "CVaR": "CVaR",
                "average_drawdown": "average_drawdown",
                "upsidecap": "upsidecap",
                "downsidecap": "downsidecap",
                "pain_ratio": "pain_ratio",
                "ERCVaR": "ERCVaR",
                "return_Msqr": "return_Msqr",
                "adjusted_jensen_a": "adjusted_jensen_a",
                "assess_ratio": "assess_ratio",
                "sterling_a": "sterling_a",
                "hurst": "hurst",
                "timing_hm": "timing_hm",
                "stock_hm": "stock_hm",
                "upbeta_cl": "upbeta_cl",
                "downbeta_cl": "downbeta_cl",
                "burke_a": "burke_a",
                "kappa_a": "kappa_a",
                "omega": "omega",
                "excess_pl": "excess_pl",
                "beta_timing_camp": "beta_timing_camp",
                "corr_spearman": "corr_spearman",
            },
            "m": {
                "accumulative_return": "return",
                "return_a": "return_a",
                "excess_return_a": "excess_a",
                "odds": "odds",
                "sharpe_a": "sharp_a",
                "calmar_a": "calmar_a",
                "sortino_a": "sor_a",
                "info_a": "inf_a",
                "jensen_a": "jensen_a",
                "treynor_a": "tr_a",
                "stdev": "stdev",
                "stdev_a": "stdev_a",
                "downside_deviation_a": "dd_a",
                "max_drawdown": "max_retracement",
                "beta": "beta",
                "corr": "benchmark_r",
                "con_rise_periods": "con_rise_months",
                "con_fall_periods": "con_fall_months",
                "ability_timing": "s_time",
                "ability_security": "s_security",
                "persistence": "persistence",
                "unsystematic_risk": "unsys_risk",
                "tracking_error_a": "tracking_error_a",
                "VaR": "rvalue",
                "p_earning_periods": "p_earning_months",
                "n_earning_periods": "n_earning_months",
                "min_return": "min_rate_of_return",
                "max_return": "max_rate_of_return",
                "mdd_repair_time": "mdd_repair_time",
                "mdd_time": "mdd_time",
                "skewness": "skewness",
                "kurtosis": "kurtosis",
                "ERVaR": "rvalue_adjustment_ratio",
                #v2
                "ddr3_a": "ddr3_a",
                "pain_index": "pain_index",
                "CVaR": "CVaR",
                "average_drawdown": "average_drawdown",
                "upsidecap": "upsidecap",
                "downsidecap": "downsidecap",
                "pain_ratio": "pain_ratio",
                "ERCVaR": "ERCVaR",
                "return_Msqr": "return_Msqr",
                "adjusted_jensen_a": "adjusted_jensen_a",
                "assess_ratio": "assess_ratio",
                "sterling_a": "sterling_a",
                "hurst": "hurst",
                "timing_hm": "timing_hm",
                "stock_hm": "stock_hm",
                "upbeta_cl": "upbeta_cl",
                "downbeta_cl": "downbeta_cl",
                "burke_a": "burke_a",
                "kappa_a": "kappa_a",
                "omega": "omega",
                "excess_pl": "excess_pl",
                "beta_timing_camp": "beta_timing_camp",
                "corr_spearman": "corr_spearman",
            }
        }

    class Mutual:
        mp_interval = {
            1: "m1",
            3: "m3",
            6: "m6",
            12: "y1",
            24: "y2",
            36: "y3",
            60: "y5",
            "w": "week",
            "m": "month",
            "q": "quarter",
            "a": "year",
            "whole": "total"
        }
        mp_ind = {
            "d": {
                "accumulative_return": "return",
                "return_a": "return_a",
                "excess_return_a": "excess_a",
                "odds": "odds",
                "sharpe_a": "sharpe_a",  # sharp_a
                "calmar_a": "calmar_a",
                "sortino_a": "sor_a",
                "info_a": "info_a",  # inf_a
                "jensen_a": "jensen_a",
                "treynor_a": "tr_a",
                "stdev": "stdev",
                "stdev_a": "stdev_a",
                "downside_deviation_a": "dd_a",
                "max_drawdown": "max_drawdown",  # max_retracement
                "beta": "beta",
                "corr": "corr",  # benchmark_r
                "con_rise_periods": "con_rise_days",
                "con_fall_periods": "con_fall_days",
                "ability_timing": "s_time",
                "ability_security": "s_security",
                "persistence": "persistence",
                "unsystematic_risk": "unsys_risk",
                "tracking_error_a": "tracking_error_a",
                "VaR": "VaR",  # rvalue
                "p_earning_periods": "p_earning_days",
                "n_earning_periods": "n_earning_days",
                "min_return": "min_return",  # min_rate_of_return
                "max_return": "max_return",  # max_rate_of_return
                "mdd_repair_time": "mdd_repair_time",
                "mdd_time": "mdd_time",
                "skewness": "skewness",
                "kurtosis": "kurtosis",
                "ERVaR": "ERVaR",  # rvalue_adjustment_ratio
                # v2
                "ddr3_a": "ddr3_a",
                "pain_index": "pain_index",
                "CVaR": "CVaR",
                "average_drawdown": "average_drawdown",
                "upsidecap": "upsidecap",
                "downsidecap": "downsidecap",
                "pain_ratio": "pain_ratio",
                "ERCVaR": "ERCVaR",
                "return_Msqr": "return_Msqr",
                "adjusted_jensen_a": "adjusted_jensen_a",
                "assess_ratio": "assess_ratio",
                "sterling_a": "sterling_a",
                "hurst": "hurst",
                "timing_hm": "timing_hm",
                "stock_hm": "stock_hm",
                "upbeta_cl": "upbeta_cl",
                "downbeta_cl": "downbeta_cl",
                "burke_a": "burke_a",
                "kappa_a": "kappa_a",
                "omega": "omega",
                "excess_pl": "excess_pl",
                "beta_timing_camp": "beta_timing_camp",
                "corr_spearman": "corr_spearman",
            },
            "w": {
                "accumulative_return": "return",
                "return_a": "return_a",
                "excess_return_a": "excess_a",
                "odds": "odds",
                "sharpe_a": "sharpe_a",  # sharp_a
                "calmar_a": "calmar_a",
                "sortino_a": "sor_a",
                "info_a": "info_a",  # inf_a
                "jensen_a": "jensen_a",
                "treynor_a": "tr_a",
                "stdev": "stdev",
                "stdev_a": "stdev_a",
                "downside_deviation_a": "dd_a",
                "max_drawdown": "max_drawdown",  # max_retracement
                "beta": "beta",
                "corr": "corr",  # benchmark_r
                "con_rise_periods": "con_rise_days",
                "con_fall_periods": "con_fall_days",
                "ability_timing": "s_time",
                "ability_security": "s_security",
                "persistence": "persistence",
                "unsystematic_risk": "unsys_risk",
                "tracking_error_a": "tracking_error_a",
                "VaR": "VaR",  # rvalue
                "p_earning_periods": "p_earning_days",
                "n_earning_periods": "n_earning_days",
                "min_return": "min_return",  # min_rate_of_return
                "max_return": "max_return",  # max_rate_of_return
                "mdd_repair_time": "mdd_repair_time",
                "mdd_time": "mdd_time",
                "skewness": "skewness",
                "kurtosis": "kurtosis",
                "ERVaR": "ERVaR",
                # v2
                "ddr3_a": "ddr3_a",
                "pain_index": "pain_index",
                "CVaR": "CVaR",
                "average_drawdown": "average_drawdown",
                "upsidecap": "upsidecap",
                "downsidecap": "downsidecap",
                "pain_ratio": "pain_ratio",
                "ERCVaR": "ERCVaR",
                "return_Msqr": "return_Msqr",
                "adjusted_jensen_a": "adjusted_jensen_a",
                "assess_ratio": "assess_ratio",
                "sterling_a": "sterling_a",
                "hurst": "hurst",
                "timing_hm": "timing_hm",
                "stock_hm": "stock_hm",
                "upbeta_cl": "upbeta_cl",
                "downbeta_cl": "downbeta_cl",
                "burke_a": "burke_a",
                "kappa_a": "kappa_a",
                "omega": "omega",
                "excess_pl": "excess_pl",
                "beta_timing_camp": "beta_timing_camp",
                "corr_spearman": "corr_spearman",
            },  # rvalue_adjustment_ratio            },
            "m": {
                "accumulative_return": "return",
                "return_a": "return_a",
                "excess_return_a": "excess_a",
                "odds": "odds",
                "sharpe_a": "sharpe_a",  # sharp_a
                "calmar_a": "calmar_a",
                "sortino_a": "sor_a",
                "info_a": "info_a",  # inf_a
                "jensen_a": "jensen_a",
                "treynor_a": "tr_a",
                "stdev": "stdev",
                "stdev_a": "stdev_a",
                "downside_deviation_a": "dd_a",
                "max_drawdown": "max_drawdown",  # max_retracement
                "beta": "beta",
                "corr": "corr",  # benchmark_r
                "con_rise_periods": "con_rise_days",
                "con_fall_periods": "con_fall_days",
                "ability_timing": "s_time",
                "ability_security": "s_security",
                "persistence": "persistence",
                "unsystematic_risk": "unsys_risk",
                "tracking_error_a": "tracking_error_a",
                "VaR": "VaR",  # rvalue
                "p_earning_periods": "p_earning_days",
                "n_earning_periods": "n_earning_days",
                "min_return": "min_return",  # min_rate_of_return
                "max_return": "max_return",  # max_rate_of_return
                "mdd_repair_time": "mdd_repair_time",
                "mdd_time": "mdd_time",
                "skewness": "skewness",
                "kurtosis": "kurtosis",
                "ERVaR": "ERVaR",  # rvalue_adjustment_ratio
                # v2
                "ddr3_a": "ddr3_a",
                "pain_index": "pain_index",
                "CVaR": "CVaR",
                "average_drawdown": "average_drawdown",
                "upsidecap": "upsidecap",
                "downsidecap": "downsidecap",
                "pain_ratio": "pain_ratio",
                "ERCVaR": "ERCVaR",
                "return_Msqr": "return_Msqr",
                "adjusted_jensen_a": "adjusted_jensen_a",
                "assess_ratio": "assess_ratio",
                "sterling_a": "sterling_a",
                "hurst": "hurst",
                "timing_hm": "timing_hm",
                "stock_hm": "stock_hm",
                "upbeta_cl": "upbeta_cl",
                "downbeta_cl": "downbeta_cl",
                "burke_a": "burke_a",
                "kappa_a": "kappa_a",
                "omega": "omega",
                "excess_pl": "excess_pl",
                "beta_timing_camp": "beta_timing_camp",
                "corr_spearman": "corr_spearman",
            }
        }

    class Org4R:
        mp_interval = {
            1: "m1",
            3: "m3",
            6: "m6",
            12: "y1",
            24: "y2",
            36: "y3",
            60: "y5",
            "w": "week",
            "m": "month",
            "q": "quarter",
            "a": "year",
            "whole": "total"
        }
        mp_ind = {
            "m": {
                "accumulative_return": "return",
                "return_a": "return_a",
                "excess_return_a": "excess_a",
                "odds": "odds",
                "sharpe_a": "sharpe_a",  # sharp_a
                "calmar_a": "calmar_a",
                "sortino_a": "sor_a",
                "info_a": "info_a",  # inf_a
                "jensen_a": "jensen_a",
                "treynor_a": "tr_a",
                "stdev": "stdev",
                "stdev_a": "stdev_a",
                "downside_deviation_a": "dd_a",
                "max_drawdown": "max_drawdown",  # max_retracement
                "beta": "beta",
                "corr": "corr",  # benchmark_r
                "con_rise_periods": "con_rise_months",
                "con_fall_periods": "con_fall_months",
                "ability_timing": "s_time",
                "ability_security": "s_security",
                "persistence": "persistence",
                "unsystematic_risk": "unsys_risk",
                "tracking_error_a": "tracking_error_a",
                "VaR": "VaR",  # rvalue
                "p_earning_periods": "p_earning_months",
                "n_earning_periods": "n_earning_months",
                "min_return": "min_return",  # min_rate_of_return
                "max_return": "max_return",  # max_rate_of_return
                "mdd_repair_time": "mdd_repair_time",
                "mdd_time": "mdd_time",
                "skewness": "skewness",
                "kurtosis": "kurtosis",
                "ERVaR": "ERVaR",  # rvalue_adjustment_ratio
                # v2
                "ddr3_a": "ddr3_a",
                "pain_index": "pain_index",
                "CVaR": "CVaR",
                "average_drawdown": "average_drawdown",
                "upsidecap": "upsidecap",
                "downsidecap": "downsidecap",
                "pain_ratio": "pain_ratio",
                "ERCVaR": "ERCVaR",
                "return_Msqr": "return_Msqr",
                "adjusted_jensen_a": "adjusted_jensen_a",
                "assess_ratio": "assess_ratio",
                "sterling_a": "sterling_a",
                "hurst": "hurst",
                "timing_hm": "timing_hm",
                "stock_hm": "stock_hm",
                "upbeta_cl": "upbeta_cl",
                "downbeta_cl": "downbeta_cl",
                "burke_a": "burke_a",
                "kappa_a": "kappa_a",
                "omega": "omega",
                "excess_pl": "excess_pl",
                "beta_timing_camp": "beta_timing_camp",
                "corr_spearman": "corr_spearman",
            }
        }


class SQL:
    @staticmethod
    def values4sql(values, usage="tuple", **kwargs):
        if isinstance(values, Iterable) and not isinstance(values, dict):
            if isinstance(values, (str, int, float)):
                values = [values]
            if len(values) > 1:
                result = str(tuple(values))
            elif len(values) == 1:
                result = "('{0}')".format(tuple(values)[0])

            if usage == "column":
                result = result[1:-1].replace("'", "`")

            if kwargs.get("operator"):
                result = kwargs.get("operator") + result

        elif isinstance(values, dict):
            value_min = values.get("min")
            value_max = values.get("max")
            if value_min is not None and value_max is not None:
                if value_min != value_max:
                    result = "BETWEEN {rng_min} AND {rng_max}".format(rng_min=value_min, rng_max=value_max)
                else:
                    result = "= {rng_eql}".format(rng_eql=value_min)
            elif value_min is not None and value_max is None:
                result = "> {rng_min}".format(rng_min=value_min)

            elif value_min is None and value_max is not None:
                result = "<= {rng_max}".format(rng_max=value_max)

        return result

    @staticmethod
    def ids_updated_sd(date, freq="w"):
        """
        Return sql statement for querying funds which have new get_data in corresponding period.
        Args:
            date: datetime.date
            freq: str, optional {"w", "m", "om"}, default "w"
                frequency of

        Returns:

        """
        _tmtp = date.timetuple()
        if freq == "w":
            _sql_base = "SELECT DISTINCT fund_id FROM fund_nv_data_standard \
            WHERE statistic_date = '{date}' AND {nv} > 0.2 AND statistic_date >= '1970-01-02'".format(
                date=date,
                nv=_NV
            )
            _criteria = "AND fund_id in (SELECT fund_id FROM fund_info WHERE data_freq IN ('周度', '日度'))"

        elif freq == "m":
            _sql_base = "SELECT DISTINCT fnd.fund_id FROM fund_nv_data_standard fnd JOIN fund_info fi ON fnd.fund_id = fi.fund_id \
            WHERE fnd.statistic_date BETWEEN '{date_s}' AND '{date_e}' AND fnd.{nv} > 0.2 AND statistic_date >= '1970-01-02'".format(
                date_s=dt.date(_tmtp.tm_year, _tmtp.tm_mon, 1),
                date_e=dt.date(_tmtp.tm_year, _tmtp.tm_mon, cld.monthrange(_tmtp.tm_year, _tmtp.tm_mon)[1]),
                nv=_NV
            )
            _criteria = "AND fi.data_freq IN ('月度', '周度', '日度')"

        elif freq == "om":
            _sql_base = "SELECT DISTINCT fnd.fund_id FROM fund_nv_data_standard fnd JOIN fund_info fi ON fnd.fund_id = fi.fund_id \
            WHERE fnd.statistic_date BETWEEN '{date_s}' AND '{date_e}' AND fnd.{nv} > 0.2 AND statistic_date >= '1970-01-02'".format(
                date_s=dt.date(_tmtp.tm_year, _tmtp.tm_mon, 1),
                date_e=dt.date(_tmtp.tm_year, _tmtp.tm_mon, cld.monthrange(_tmtp.tm_year, _tmtp.tm_mon)[1]),
                nv=_NV
            )
            _criteria = "AND fi.data_freq IN ('月度', '周度', '日度') AND fi.foundation_date <= '{0}'".format(
                date - relativedelta(months=1)
            )

        return _sql_base + _criteria

    @staticmethod
    def ids4sql(ids, usage="tuple"):
        if type(ids) is str:
            ids = [ids]
        if len(ids) > 1:
            result = str(tuple(ids))
        elif len(ids) == 1:
            result = "('{0}')".format(tuple(ids)[0])

        if usage == "tuple":
            return result
        elif usage == "column":
            result = result[1:-1].replace("'", "`")
            return result

    @classmethod
    def market_index(cls, date, benchmarks=["hs300", "csi500", "sse50", "cbi", "nfi", "y1_treasury_rate"], whole=False):
        """
            Optional `HS300`, `CSI500`, `SSE50`, `SSIA`, `CBI`, `1y_treasury_rate`, default "HS300", "CSI500", "SSE50", "CBI", "NFI", "1y_treasury,rate"

        Args:
            date:
            benchmarks:
            whole:

        Returns:

        """
        if whole is False:
            __sql = "SELECT {0}, `statistic_date` FROM market_index WHERE statistic_date <= '{1}' AND statistic_date >= '{2}'\
            ORDER BY statistic_date DESC".format(
                # (cls.values4sql(benchmarks)[1:-1]+",").replace("'", ""),
                cls.ids4sql(benchmarks, usage="column"),
                date,
                date - dt.timedelta(3660)
            )
        else:
            __sql = "SELECT {0}, `statistic_date` FROM market_index WHERE statistic_date <= '{1}'\
            ORDER BY statistic_date DESC".format(
                cls.ids4sql(benchmarks, usage="column"),
                date
            )

        return __sql

    @classmethod
    def pe_index(cls, date, index_id=["FI01"], freq="w"):
        __freq = {"w": "weekly", "m": "month"}
        #20170511 修改为使用fund_weekly/month/index_static表调取数据
        return "SELECT index_id, index_value, statistic_date FROM fund_{0}_index_static WHERE index_id IN {1} AND statistic_date <= '{2}' \
            ORDER BY index_id ASC, statistic_date DESC".format(__freq[freq], cls.ids4sql(index_id), date)

    @classmethod
    def nav(cls, ids: object, LIMIT: bool = True) -> object:
        ids = cls.ids4sql(ids)
        if LIMIT:
            return "SELECT fund_id, {nv} as nav, statistic_date FROM fund_nv_data_standard WHERE fund_id in {ids} \
                    AND {nv} IS NOT NULL AND statistic_date IS NOT NULL AND statistic_date >= '1970-01-02' \
                    AND {nv} > 0.3 AND {nv} < 80 \
                    ORDER BY fund_id ASC, statistic_date DESC".format(
                ids=ids,
                nv=_NV
            )
        else:
            return "SELECT fund_id, {nv} as nav, statistic_date FROM fund_nv_data_standard WHERE fund_id in {ids} \
                    AND {nv} IS NOT NULL AND statistic_date IS NOT NULL AND statistic_date >= '1970-01-02' \
                    AND {nv} > 0.3 AND {nv} < 80\
                    ORDER BY fund_id ASC, statistic_date DESC".format(
                ids=ids,
                nv=_NV
            )

    @classmethod
    def generate_min_date(cls, update_time_l, update_time_r=None, freq="w"):
        freqs = {
            "w": str(tuple(["日度", "周度"])),
            "m": str(tuple(["日度", "周度", "月度"]))
        }[freq]

        if update_time_r is None:
            sql = "SELECT fund_id, MIN(statistic_date) as msd \
            FROM fund_nv_data_standard \
            WHERE fund_id IN (SELECT fund_id FROM fund_info WHERE data_freq IN {freqs}) AND update_time >= '{update_time}' AND statistic_date >= '1970-01-02' AND {nv} > 0.3 AND {nv} < 80 \
            GROUP BY fund_id".format(
                update_time=update_time_l.strftime("%Y%m%d%H%M%S"), nv=_NV, freqs=freqs
            )
        else:
            sql = "SELECT fund_id, MIN(statistic_date) as msd \
            FROM fund_nv_data_standard \
            WHERE fund_id IN (SELECT fund_id FROM fund_info WHERE data_freq IN {freqs}) AND update_time BETWEEN '{update_time_l}' AND '{update_time_r}' AND statistic_date >= '1970-01-02' AND {nv} > 0.3 AND {nv} < 80 \
            GROUP BY fund_id".format(
                update_time_l=update_time_l.strftime("%Y%m%d%H%M%S"), update_time_r=update_time_r.strftime("%Y%m%d%H%M%S"), nv=_NV, freqs=freqs
            )
        return sql

    @staticmethod
    def fetch_dates(date_dict):
        funds = list(date_dict.keys())
        dates = [date_dict[f] for f in funds]
        base = "SELECT fund_id as fund_id, statistic_date as statistic_date FROM fund_nv_data_standard".format(nv=_NV)

        criterion = " OR ".join(
            list(
                # map(lambda x, y: "(fund_id = '{x}' AND statistic_date >= '{y}')".format(x=x, y=y.strftime("%Y%m%d")), funds, dates)
                map(lambda x, y: "(fund_id = '{x}' AND statistic_date >= '{y}' AND {nv} > 0.3 AND {nv} < 80)".format(
                    x=x, y=y.strftime("%Y%m%d"), nv=_NV
                ), funds, dates)
            )
        )
        sql = "{base} WHERE ({criterion})".format(base=base, criterion=criterion)
        return sql

    @classmethod
    def fund_type(cls, ids, dimension, level):
        """
        Args:
            ids: list
            dimension: str, optional {"strategy", "structure", "target", "issuance"}
            level: int, optional {1, 2}

        Returns:

        """
        ids = cls.ids4sql(ids)
        dimensions = {
            "strategy": 601,
            "structure": 602,
            "target": 603,
            "issuance": 604
        }
        levels = {
            1: "type_code",
            2: "stype_code"
        }
        base_sql = "SELECT fund_id, {key_type} as code \
                    FROM fund_type_mapping \
                    WHERE typestandard_code = {tsc} AND flag = 1 AND  fund_id IN {ids}".format(
            key_type=levels[level], tsc=dimensions[dimension], ids=ids
        )
        return base_sql

    @classmethod
    def fund_name(cls, ids, level):
        names = {
            1: "fund_name",
            2: "fund_full_name"
        }
        ids = cls.ids4sql(ids)
        base_sql = "SELECT fund_id as fund_id, {name_level} as fund_name FROM fund_info WHERE fund_id IN {ids}".format(
            name_level=names[level], ids=ids
        )
        return base_sql

    @classmethod
    def foundation_date(cls, ids, id_type="fund"):
        ids = cls.ids4sql(ids)
        if id_type == "fund":
            return "SELECT fund_id, foundation_date as t_min FROM fund_info WHERE fund_id in {0} \
                    AND foundation_date IS NOT NULL \
                    ORDER BY fund_id ASC".format(ids)

        elif id_type == "org":
            return "SELECT org_id, found_date as t_min FROM fund_info WHERE org_id in {0} \
                    AND found_date IS NOT NULL \
                    ORDER BY org_id ASC".format(ids)

    @classmethod
    def firstnv_date(cls, ids):
        ids = cls.ids4sql(ids)
        return "SELECT fund_id, MIN(statistic_date) as t_min FROM fund_nv_data_standard WHERE fund_id in {0} \
                GROUP BY fund_id HAVING t_min IS NOT NULL ORDER BY fund_id ASC".format(ids)

    class Mutual:
        # _NV = "added_nav"
        _NV = "swanav"  # 2017.12.1 所有指标替换成使用复权累计净值计算;

        @classmethod
        def ids_updated_sd(cls, date, freq="w"):
            """
            Return sql statement for querying funds which have new get_data in corresponding period.
            Args:
                date: datetime.date
                freq: str, optional {"w", "m", "om"}, default "w"
                    frequency of

            Returns:

            """
            _tmtp = date.timetuple()
            if freq == "w":
                _sql_base = "SELECT DISTINCT fund_id FROM fund_nv \
                WHERE statistic_date = '{date}' AND {nv} > 0.2 AND statistic_date >= '1970-01-02'".format(
                    date=date,
                    nv=cls._NV
                )
                _criteria = "AND fund_id in (SELECT fund_id FROM fund_info WHERE nv_freq IN ('周度', '日度'))"

            elif freq == "m":
                _sql_base = "SELECT DISTINCT fnd.fund_id FROM fund_nv fnd JOIN fund_info fi ON fnd.fund_id = fi.fund_id \
                WHERE fnd.statistic_date BETWEEN '{date_s}' AND '{date_e}' AND fnd.{nv} > 0.2 AND statistic_date >= '1970-01-02'".format(
                    date_s=dt.date(_tmtp.tm_year, _tmtp.tm_mon, 1),
                    date_e=dt.date(_tmtp.tm_year, _tmtp.tm_mon, cld.monthrange(_tmtp.tm_year, _tmtp.tm_mon)[1]),
                    nv=cls._NV
                )
                _criteria = "AND fi.nv_freq IN ('月度', '周度', '日度')"

            elif freq == "d":
                _sql_base = "SELECT DISTINCT fnd.fund_id FROM fund_nv fnd JOIN fund_info fi ON fnd.fund_id = fi.fund_id \
                WHERE fnd.statistic_date BETWEEN '{date_s}' AND '{date_e}' AND fnd.{nv} > 0.2 AND statistic_date >= '1970-01-02'".format(
                    date_s=dt.date(_tmtp.tm_year, _tmtp.tm_mon, 1),
                    date_e=dt.date(_tmtp.tm_year, _tmtp.tm_mon, cld.monthrange(_tmtp.tm_year, _tmtp.tm_mon)[1]),
                    nv=cls._NV
                )
                _criteria = "AND fi.nv_freq IN ('日度')"

            elif freq == "om":
                _sql_base = "SELECT DISTINCT fnd.fund_id FROM fund_nv fnd JOIN fund_info fi ON fnd.fund_id = fi.fund_id \
                WHERE fnd.statistic_date BETWEEN '{date_s}' AND '{date_e}' AND fnd.{nv} > 0.2 AND statistic_date >= '1970-01-02'".format(
                    date_s=dt.date(_tmtp.tm_year, _tmtp.tm_mon, 1),
                    date_e=dt.date(_tmtp.tm_year, _tmtp.tm_mon, cld.monthrange(_tmtp.tm_year, _tmtp.tm_mon)[1]),
                    nv=cls._NV
                )
                _criteria = "AND fi.nv_freq IN ('月度', '周度', '日度') AND fi.foundation_date <= '{0}'".format(
                    date - relativedelta(months=1)
                )

            return _sql_base + _criteria

        @staticmethod
        def ids4sql(ids, usage="tuple"):
            if type(ids) is str:
                ids = [ids]
            if len(ids) > 1:
                result = str(tuple(ids))
            elif len(ids) == 1:
                result = "('{0}')".format(tuple(ids)[0])

            if usage == "tuple":
                return result
            elif usage == "column":
                result = result[1:-1].replace("'", "`")
                return result

        @classmethod
        def market_index(cls, date, benchmarks=["hs300", "csi500", "sse50", "cbi", "nfi", "y1_treasury_rate"],
                         whole=False):
            """
                Optional `HS300`, `CSI500`, `SSE50`, `SSIA`, `CBI`, `1y_treasury_rate`, default "HS300", "CSI500", "SSE50", "CBI", "NFI", "1y_treasury,rate"

            Args:
                date:
                benchmarks:
                whole:

            Returns:

            """
            if whole is False:
                __sql = "SELECT {0}, `statistic_date` FROM market_index WHERE statistic_date <= '{1}' AND statistic_date >= '{2}'\
                ORDER BY statistic_date DESC".format(
                    # (cls.values4sql(benchmarks)[1:-1]+",").replace("'", ""),
                    cls.ids4sql(benchmarks, usage="column"),
                    date,
                    date - dt.timedelta(3660)
                )
            else:
                __sql = "SELECT {0}, `statistic_date` FROM market_index WHERE statistic_date <= '{1}'\
                ORDER BY statistic_date DESC".format(
                    cls.ids4sql(benchmarks, usage="column"),
                    date
                )

            return __sql

        @classmethod
        def pe_index(cls, date, index_id=["FI01"], freq="w"):
            __freq = {"w": "weekly", "m": "month"}
            # 20170511 修改为使用fund_weekly/month/index_static表调取数据
            return "SELECT index_id, index_value, statistic_date FROM fund_{0}_index_static WHERE index_id IN {1} AND statistic_date <= '{2}' \
                ORDER BY index_id ASC, statistic_date DESC".format(__freq[freq], cls.ids4sql(index_id), date)

        @classmethod
        def nav(cls, ids: object, LIMIT: bool = True) -> object:
            ids = cls.ids4sql(ids)
            if LIMIT:
                return "SELECT fund_id, {nv} as nav, statistic_date FROM fund_nv WHERE fund_id in {ids} \
                        AND {nv} IS NOT NULL AND statistic_date IS NOT NULL AND statistic_date >= '1970-01-02' \
                        AND {nv} > 0.3 AND {nv} < 80 \
                        ORDER BY fund_id ASC, statistic_date DESC".format(
                    ids=ids,
                    nv=cls._NV
                )
            else:
                return "SELECT fund_id, {nv} as nav, statistic_date FROM fund_nv WHERE fund_id in {ids} \
                        AND {nv} IS NOT NULL AND statistic_date IS NOT NULL AND statistic_date >= '1970-01-02' \
                        AND {nv} > 0.3 \
                        ORDER BY fund_id ASC, statistic_date DESC".format(
                    ids=ids,
                    nv=cls._NV
                )

        @classmethod
        def generate_min_date(cls, update_time_l, update_time_r=None, freq=None):
            if update_time_r is None:
                sql = "SELECT fund_id, MIN(statistic_date) as msd \
                FROM fund_nv \
                WHERE fund_id WHERE update_time >= '{update_time}' AND statistic_date >= '1970-01-02' AND {nv} > 0.3 AND {nv} < 80 \
                GROUP BY fund_id".format(
                    update_time=update_time_l.strftime("%Y%m%d%H%M%S"), nv=cls._NV
                )
            else:
                sql = "SELECT fund_id, MIN(statistic_date) as msd \
                FROM fund_nv \
                WHERE update_time BETWEEN '{update_time_l}' AND '{update_time_r}' AND statistic_date >= '1970-01-02' AND {nv} > 0.3 AND {nv} < 80 \
                GROUP BY fund_id".format(
                    update_time_l=update_time_l.strftime("%Y%m%d%H%M%S"),
                    update_time_r=update_time_r.strftime("%Y%m%d%H%M%S"), nv=cls._NV
                )
            return sql

        @classmethod
        def fetch_dates(cls, date_dict):
            funds = list(date_dict.keys())
            dates = [date_dict[f] for f in funds]
            base = "SELECT fund_id as fund_id, statistic_date as statistic_date FROM fund_nv".format(
                nv=cls._NV)

            criterion = " OR ".join(
                list(
                    map(lambda x, y: "(fund_id = '{x}' AND statistic_date >= '{y}')".format(x=x,
                                                                                            y=y.strftime("%Y%m%d")),
                        funds, dates)
                )
            )
            sql = "{base} WHERE ({criterion})".format(base=base, criterion=criterion)
            return sql

        @classmethod
        def fund_type(cls, ids, dimension, level):
            """
            Args:
                ids:
                dimension:
                level:

            Returns:

            """
            ids = cls.ids4sql(ids)
            dimensions = {
                "strategy": "04",
                "structure": "??",
                "target": "02",
                "issuance": "??"
            }
            levels = {
                1: "type_code",
                2: "stype_code"
            }
            base_sql = "SELECT fund_id, {key_type} as code \
                        FROM fund_type_mapping \
                        WHERE typestandard_code = {tsc} AND  fund_id IN {ids}".format(
                key_type=levels[level], tsc=dimensions[dimension], ids=ids
            )
            return base_sql

        @classmethod
        def fund_name(cls, ids, level):
            names = {
                1: "fund_name",
                2: "fund_full_name"
            }
            ids = cls.ids4sql(ids)
            base_sql = "SELECT fund_id as fund_id, {name_level} as fund_name FROM fund_info WHERE fund_id IN {ids}".format(
                name_level=names[level], ids=ids
            )
            return base_sql

        @classmethod
        def foundation_date(cls, ids, id_type="fund"):
            ids = cls.ids4sql(ids)
            if id_type == "fund":
                return "SELECT fund_id, foundation_date as t_min FROM fund_info WHERE fund_id in {0} \
                        AND foundation_date IS NOT NULL \
                        ORDER BY fund_id ASC".format(ids)

            elif id_type == "org":
                return "SELECT org_id, found_date as t_min FROM fund_info WHERE org_id in {0} \
                        AND found_date IS NOT NULL \
                        ORDER BY org_id ASC".format(ids)

        @classmethod
        def firstnv_date(cls, ids):
            ids = cls.ids4sql(ids)
            return "SELECT fund_id, MIN(statistic_date) as t_min FROM fund_nv WHERE fund_id in {0} \
                    GROUP BY fund_id HAVING t_min IS NOT NULL ORDER BY fund_id ASC".format(ids)

    class Org4R:
        INDEXID = "OI01"  # 计算时设置使用的INDEXID
        _NV = "index_value"  # 2018.1.9 所有指标替换成使用投顾指数计算;

        @classmethod
        def ids_updated_sd(cls, date, freq="m"):
            """
            Return sql statement for querying funds which have new data in corresponding period of given freq.
            Args:
                date: datetime.date
                freq: str, optional {"w", "m",}, default "w"
                    frequency of

            Returns:

            """
            if freq == "w":
                _sql_base = "SELECT DISTINCT org_id as fund_id FROM org_weekly_index_static \
                WHERE statistic_date = '{date}' AND index_id = '{idxid}'".format(
                    date=date,
                    nv=cls._NV,
                    idxid=cls.INDEXID
                )

            elif freq == "m":
                _sql_base = "SELECT DISTINCT org_id as fund_id FROM org_monthly_index_static \
                WHERE statistic_date = '{date}' AND index_id = '{idxid}'".format(
                    date=date,
                    nv=cls._NV,
                    idxid=cls.INDEXID
                )

            return _sql_base

        @staticmethod
        def ids4sql(ids, usage="tuple"):
            if type(ids) is str:
                ids = [ids]
            if len(ids) > 1:
                result = str(tuple(ids))
            elif len(ids) == 1:
                result = "('{0}')".format(tuple(ids)[0])

            if usage == "tuple":
                return result
            elif usage == "column":
                result = result[1:-1].replace("'", "`")
                return result

        @classmethod
        def market_index(cls, date, benchmarks=["hs300", "csi500", "sse50", "cbi", "nfi", "y1_treasury_rate"],
                         whole=False):
            """
                Optional `HS300`, `CSI500`, `SSE50`, `SSIA`, `CBI`, `1y_treasury_rate`, default "HS300", "CSI500", "SSE50", "CBI", "NFI", "1y_treasury,rate"

            Args:
                date:
                benchmarks:
                whole:

            Returns:

            """
            if whole is False:
                __sql = "SELECT {0}, `statistic_date` FROM market_index WHERE statistic_date <= '{1}' AND statistic_date >= '{2}'\
                ORDER BY statistic_date DESC".format(
                    # (cls.values4sql(benchmarks)[1:-1]+",").replace("'", ""),
                    cls.ids4sql(benchmarks, usage="column"),
                    date,
                    date - dt.timedelta(3660)
                )
            else:
                __sql = "SELECT {0}, `statistic_date` FROM market_index WHERE statistic_date <= '{1}'\
                ORDER BY statistic_date DESC".format(
                    cls.ids4sql(benchmarks, usage="column"),
                    date
                )

            return __sql

        @classmethod
        def pe_index(cls, date, index_id=["FI01"], freq="w"):
            __freq = {"w": "weekly", "m": "month"}
            # 20170511 修改为使用fund_weekly/month/index_static表调取数据
            return "SELECT index_id, index_value, statistic_date FROM fund_{0}_index_static WHERE index_id IN {1} AND statistic_date <= '{2}' \
                ORDER BY index_id ASC, statistic_date DESC".format(__freq[freq], cls.ids4sql(index_id), date)

        @classmethod
        def nav(cls, ids):
            ids = cls.ids4sql(ids)
            return "SELECT org_id as fund_id, {nv} as nav, statistic_date FROM org_monthly_index_static \
            WHERE org_id in {ids} AND index_id = '{idxid}'\
            ORDER BY org_id ASC, statistic_date DESC".format(
                ids=ids, nv=cls._NV, idxid=cls.INDEXID
            )

        @classmethod
        def generate_min_date(cls, update_time_l, update_time_r=None, freq=None):
            if update_time_r is None:
                sql = "SELECT org_id as fund_id, MIN(statistic_date) as msd \
                FROM org_monthly_index_static \
                WHERE update_time >= '{update_time}' \
                GROUP BY org_id".format(
                    update_time=update_time_l.strftime("%Y%m%d%H%M%S"), nv=cls._NV
                )
            else:
                sql = "SELECT org_id as fund_id, MIN(statistic_date) as msd \
                FROM org_monthly_index_static \
                WHERE update_time BETWEEN '{update_time_l}' AND '{update_time_r}' \
                GROUP BY org_id".format(
                    update_time_l=update_time_l.strftime("%Y%m%d%H%M%S"),
                    update_time_r=update_time_r.strftime("%Y%m%d%H%M%S"), nv=cls._NV
                )
            return sql

        @classmethod
        def fetch_dates(cls, date_dict):
            funds = list(date_dict.keys())
            dates = [date_dict[f] for f in funds]
            base = "SELECT org_id as fund_id, statistic_date as statistic_date FROM org_monthly_index_static".format(
                nv=cls._NV)

            criterion = " OR ".join(
                list(
                    map(lambda x, y: "(org_id = '{x}' AND statistic_date >= '{y}')".format(
                        x=x, y=y.strftime("%Y%m%d")), funds, dates)
                )
            )
            sql = "{base} WHERE ({criterion})".format(base=base, criterion=criterion)
            return sql

        @classmethod
        def fund_type(cls, ids, dimension, level):
            return None

        @classmethod
        def fund_name(cls, ids, level):
            names = {
                1: "org_name",
                2: "org_full_name"
            }
            ids = cls.ids4sql(ids)
            base_sql = "SELECT org_id as fund_id, {name_level} as fund_name FROM org_info WHERE org_id IN {ids}".format(
                name_level=names[level], ids=ids
            )
            return base_sql

        @classmethod
        def foundation_date(cls, ids, id_type="fund"):
            ids = cls.ids4sql(ids)
            return "SELECT org_id as fund_id, MIN(statistic_date) as t_min FROM org_monthly_index_static WHERE org_id in {0} \
                    GROUP BY org_id HAVING t_min IS NOT NULL ORDER BY org_id ASC".format(ids)

        @classmethod
        def firstnv_date(cls, ids):
            ids = cls.ids4sql(ids)
            return "SELECT org_id as fund_id, MIN(statistic_date) as t_min FROM org_monthly_index_static WHERE org_id in {0} \
                    GROUP BY org_id HAVING t_min IS NOT NULL ORDER BY org_id ASC".format(ids)


class SQL_PEIndex(PEIndex):
    def __init__(self, idx=None, year=None):
        self.__idx = idx
        self.__year = year
        super().__init__(idx)

    @property
    def condition(self):
        sql_usetype = "AND fund_id IN (SELECT fund_id FROM fund_type_mapping WHERE flag=1 AND type_code = {0})"
        sql_usestype = "AND fund_id IN (SELECT fund_id FROM fund_type_mapping WHERE flag=1 AND stype_code = {0})"

        self.__condition = {
            1: "",
            2: sql_usetype.format("60401"),
            3: sql_usestype.format("6010702"),
            4: sql_usestype.format("6010101"),
            5: sql_usestype.format("6010102"),
            6: sql_usestype.format("6010103"),
            7: sql_usetype.format("60105"),
            8: sql_usetype.format("60102"),
            9: sql_usetype.format("60106"),
            10: sql_usetype.format("60104"),
            11: sql_usetype.format("60103"),
            12: sql_usetype.format("60108"),
            13: sql_usetype.format("60107")
        }
        return self.__condition

    @property
    def yeardata_m(self):
        sql = "\
            SELECT fund_id, {nv} as nav, statistic_date \
            FROM fund_nv_data_standard \
            WHERE statistic_date < '{date_s}' AND statistic_date >= '{date_e}' \
                {condition} \
                AND {nv} > 0.3 AND statistic_date IS NOT NULL \
            ORDER BY fund_id ASC, statistic_date DESC".format(
            date_s=dt.date(self.__year + 1, 1, 8),
            date_e=dt.date(self.__year - 1, 12, 1),
            condition=self.condition[self.__idx],
            nv=_NV
        )
        return sql

    @property
    def yeardata_w(self):
        sql = {}
        sql["nv"] = "\
        SELECT fund_id, {nv} as nav, statistic_date FROM fund_nv_data_standard \
        WHERE statistic_date <= '{date_s}' AND statistic_date >= '{date_e}' AND statistic_date <> '0000-00-00' \
            {condition} \
            AND {nv} >0.3 AND statistic_date IS NOT NULL \
        ORDER BY fund_id ASC, statistic_date DESC".format(
            date_s=dt.date(self.__year, 12, 31),
            date_e=dt.date(self.__year - 1, 12, 18),
            condition=self.condition[self.__idx],
            nv=_NV
        )
        sql["t_min"] = "\
        SELECT fund_id, MIN(statistic_date) as statistic_date_earliest FROM fund_nv_data_standard WHERE statistic_date  <> '0000-00-00' AND fund_id IN \
        (SELECT fund_id FROM fund_nv_data_standard \
        WHERE statistic_date <= '{date_s}' AND statistic_date >= '{date_e}' \
            {condition} \
            AND {nv} > 0.3 AND statistic_date IS NOT NULL \
        ORDER BY fund_id ASC, statistic_date DESC) \
        GROUP BY fund_id".format(
            date_s=dt.date(self.__year, 12, 31),
            date_e=dt.date(self.__year - 1, 12, 18),
            condition=self.condition[self.__idx],
            nv=_NV
        )
        return sql


def gen_process_range(dataframe, freq):
    if freq == "m":  # process the date (y, m, d) -> (y, m, 1)
        dataframe["statistic_date"] = dataframe["statistic_date"].apply(
            lambda x: dt.date(x.year, x.month, cld.monthrange(x.year, x.month)[1]))
        dataframe = dataframe.drop_duplicates(subset=["fund_id", "statistic_date"])

    dict_items = zip(dataframe["statistic_date"], dataframe["fund_id"])
    cal_tgt = {}
    for sd, fid in dict_items:
        k = sd
        v = str(fid)
        if k not in cal_tgt.keys():
            cal_tgt[k] = [v]
        else:
            cal_tgt[k].append(v)
    return cal_tgt

# sys._getframe().f_code.co_name
