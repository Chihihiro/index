from utils.database import sqlfactory as sf
from utils.algorithm import fund_indicator_v2 as fi
from utils.dev import calargs, calargs_mutual, calargs_org4r

# 182.254.128.241

_periods = {"w": 52, "m": 12, "d": 250}

_bms = {
    "hs300": "沪深300指数",
    "csi500": "中证500指数",
    "cbi": "中债指数",
    "sse50": "上证50指数",
    "ssia": "上证A股指数",
    "nfi": "南华商品期货指数",
    "y1_treasury_rate": "一年期国债利率",
    "FI01": "私募全市场指数",
    "FI02": "阳光私募指数",
    "FI03": "私募FOF指数",
    "FI04": "股票多头策略私募指数",
    "FI05": "股票多空策略私募指数",
    "FI06": "市场中性策略私募指数",
    "FI07": "债券基金私募指数",
    "FI08": "管理期货策略私募指数",
    "FI09": "宏观策略私募指数",
    "FI10": "事件驱动策略私募指数",
    "FI11": "相对价值策略私募指数",
    "FI12": "多策略私募指数",
    "FI13": "组合投资策略私募指数"
}


class Fund:
    def __init__(self, attr_dict, name=None):
        self.nv = attr_dict["nav"]
        self.sd = attr_dict["statistic_date"]
        self.id = attr_dict["id"]
        self.name = attr_dict.get("name", name)
        self.type_s = attr_dict.get("type_s")
        self.rs = {
            interval: fi.transform_series_type(serie, input_type="p", output_type="r",
                                               fill_none=False) if serie is not None else None
            for interval, serie in self.nv.items()
        }

    def __repr__(self):
        return "{fund} object".format(fund=self.name)


class Org(Fund):
    pass


class Benchmark:
    def __init__(self, attr_dict, id=None):
        self.price = attr_dict["index_value"]
        self.sd = attr_dict["statistic_date"]
        self.rs = {
            interval: fi.transform_series_type(serie, input_type="p", output_type="r",
                                               fill_none=False) if serie is not None else None
            for interval, serie in self.price.items()
        }
        self.id = id
        self.name = _bms[self.id]
        self.nv = self.price

    def __repr__(self):
        return "{bm} object".format(bm=self.name)


class Tbond:
    def __init__(self, attr_dict, name=None):
        self.sd = attr_dict["statistic_date"]
        self.rs = attr_dict["index_value"]
        self.name = name

    def __repr__(self):
        return "{bm} object".format(bm=self.name)


def calculate(funcs, intervals, bms_used, freq, statistic_date, fund: Fund, bms: Benchmark = None, tbond: Tbond = None,
              with_func_names=False):
    """

    Args:
        funcs:
        intervals:
        bms_used: str
            optional {"hs300", "csi500", "sse50", "cbi", "strategy", "FI01", "nfi"}
        freq:
        statistic_date:
        fund:
        bms:
        tbond:
        with_func_names: bool, default False
    Returns:

    """
    _func_names = []
    if bms_used is not None:
        bms_used = bms_used.copy()
        if "strategy" in bms_used:
            if fund.type_s is not None:
                index_id = calargs.tm.get(fund.type_s, calargs.tm.get(fund.type_s // 100))
                bms_used[bms_used.index("strategy")] = index_id

    res = []
    res_irrelative = []

    if ("max_drawdown" in funcs) or ("mdd_time" in funcs) or ("mdd_repair_time" in funcs):
        mdds = [fi.max_drawdown(fund.nv[interval], external=calargs.search[freq][interval]["external"]) for interval in
                intervals["max_drawdown"][freq]]

    if "accumulative_return" in funcs:
        r_accu = [
            fi.accumulative_return(fund.nv[interval], internal=calargs.search[freq][interval]["internal"], external=
            calargs.search[freq][interval]["external"]) for interval in calargs.intervals["accumulative_return"][freq]]
        res_irrelative.extend(r_accu)
        _func_names.append("accumulative_return")

    if "return_a" in funcs:
        return_a = [fi.return_a(fund.nv[interval], _periods[freq], calargs.mode, "p", internal=
        calargs.search[freq][interval]["internal"], external=calargs.search[freq][interval]["external"]) for interval in
                    intervals["return_a"][freq]]
        res_irrelative.extend(return_a)
        _func_names.append("return_a")

    if "sharpe_a" in funcs:
        sharpe_a = [
            fi.sharpe_a(fund.nv[interval], tbond.rs[interval], _periods[freq], calargs.mode, ["p", "r"], internal=
            calargs.search[freq][interval]["internal"], external=calargs.search[freq][interval]["external"]) for
            interval in intervals["sharpe_a"][freq]]
        res_irrelative.extend(sharpe_a)
        _func_names.append("sharpe_a")

    if "calmar_a" in funcs:
        calmar_a = [
            fi.calmar_a(fund.nv[interval], tbond.rs[interval], _periods[freq], calargs.mode, ["p", "r"], internal=
            calargs.search[freq][interval]["internal"], external=calargs.search[freq][interval]["external"]) for
            interval in intervals["calmar_a"][freq]]
        res_irrelative.extend(calmar_a)
        _func_names.append("calmar_a")

    if "sortino_a" in funcs:
        sortino_a = [
            fi.sortino_a(fund.nv[interval], tbond.rs[interval], _periods[freq], calargs.mode, ["p", "r"], internal=
            calargs.search[freq][interval]["internal"], external=calargs.search[freq][interval]["external"]) for
            interval in intervals["sortino_a"][freq]]
        res_irrelative.extend(sortino_a)
        _func_names.append("sortino_a")

    if "stdev" in funcs:
        stdev = [fi.standard_deviation(fund.rs[interval], external=calargs.search[freq][interval]["external"]) for
                 interval in intervals["stdev"][freq]]
        res_irrelative.extend(stdev)
        _func_names.append("stdev")

    if "stdev_a" in funcs:
        stdev_a = [fi.standard_deviation_a(fund.rs[interval], _periods[freq],
                                           external=calargs.search[freq][interval]["external"]) for interval in
                   intervals["stdev_a"][freq]]
        res_irrelative.extend(stdev_a)
        _func_names.append("stdev_a")

    if "downside_deviation_a" in funcs:
        dd = [fi.downside_deviation_a(fund.rs[interval], tbond.rs[interval], _periods[freq],
                                      external=calargs.search[freq][interval]["external"]) for interval in
              intervals["downside_deviation_a"][freq]]
        res_irrelative.extend(dd)
        _func_names.append("downside_deviation_a")

    if "max_drawdown" in funcs:
        mdd = [x[0] if x is not None else None for x in mdds]
        res_irrelative.extend(mdd)
        _func_names.append("max_drawdown")

    if "con_rise_periods" in funcs:
        con_rise_periods = [
            fi.periods_continuous_rise(fund.rs[interval], external=calargs.search[freq][interval]["external"])[0] for
            interval in intervals["con_rise_periods"][freq]]
        res_irrelative.extend(con_rise_periods)
        _func_names.append("con_rise_periods")

    if "con_fall_periods" in funcs:
        con_fall_periods = [
            fi.periods_continuous_fall(fund.rs[interval], external=calargs.search[freq][interval]["external"])[0] for
            interval in intervals["con_fall_periods"][freq]]
        res_irrelative.extend(con_fall_periods)
        _func_names.append("con_fall_periods")

    if "VaR" in funcs:
        VaR = [fi.value_at_risk(fund.rs[interval], external=calargs.search[freq][interval]["external"]) for interval in
               intervals["VaR"][freq]]
        res_irrelative.extend(VaR)
        _func_names.append("VaR")

    if "p_earning_periods" in funcs:
        p_earning_periods = [
            fi.periods_positive_return(fund.rs[interval], external=calargs.search[freq][interval]["external"]) for
            interval in intervals["p_earning_periods"][freq]]
        res_irrelative.extend(p_earning_periods)
        _func_names.append("p_earning_periods")

    if "n_earning_periods" in funcs:
        n_earning_periods = [
            fi.periods_negative_return(fund.rs[interval], external=calargs.search[freq][interval]["external"]) for
            interval in intervals["n_earning_periods"][freq]]
        res_irrelative.extend(n_earning_periods)
        _func_names.append("n_earning_periods")

    if "min_return" in funcs:
        min_return = [fi.min_return(fund.rs[interval], external=calargs.search[freq][interval]["external"])[0] for
                      interval in intervals["min_return"][freq]]
        res_irrelative.extend(min_return)
        _func_names.append("min_return")

    if "max_return" in funcs:
        max_return = [fi.max_return(fund.rs[interval], external=calargs.search[freq][interval]["external"])[0] for
                      interval in intervals["max_return"][freq]]
        res_irrelative.extend(max_return)
        _func_names.append("max_return")

    if "mdd_repair_time" in funcs:
        mdd_repair_time = [x[1][1] if x[1] is not None else None for x in mdds]
        res_irrelative.extend(mdd_repair_time)
        _func_names.append("mdd_repair_time")

    if "mdd_time" in funcs:
        mdd_time = [x[1][0] if x[1] is not None else None for x in mdds]
        res_irrelative.extend(mdd_time)
        _func_names.append("mdd_time")

    if "skewness" in funcs:
        skewness = [fi.skewness(fund.rs[interval], external=calargs.search[freq][interval]["external"]) for interval in
                    intervals["skewness"][freq]]
        res_irrelative.extend(skewness)
        _func_names.append("skewness")

    if "kurtosis" in funcs:
        kurtosis = [fi.kurtosis(fund.rs[interval], external=calargs.search[freq][interval]["external"]) for interval in
                    intervals["kurtosis"][freq]]
        res_irrelative.extend(kurtosis)
        _func_names.append("kurtosis")

    if "ERVaR" in funcs:
        # ERVaR = [fi.ERVaR(fund.nv[interval], tbond.rs[interval], _periods[freq], calargs.mode, ["p", "r"], internal=
        # calargs.search[freq][interval]["internal"], external=calargs.search[freq][interval]["external"]) for interval in
        #          intervals["ERVaR"][freq]]

        ERVaR = []
        for interval in intervals["ERVaR"][freq]:
            data = fi.ERVaR(fund.nv[interval], tbond.rs[interval], _periods[freq], calargs.mode, ["p", "r"], internal=
                calargs.search[freq][interval]["internal"], external=calargs.search[freq][interval]["external"])
            ERVaR.append(data)

        res_irrelative.extend(ERVaR)
        _func_names.append("ERVaR")

    if "ddr3_a" in funcs:
        val = [fi.downside_deviation_a(fund.rs[interval], tbond.rs[interval], _periods[freq], order=3, internal=
        calargs.search[freq][interval]["internal"], external=calargs.search[freq][interval]["external"]) for interval in
               intervals["ddr3_a"][freq]]
        res_irrelative.extend(val)
        _func_names.append("ddr3_a")

    if "pain_index" in funcs:
        val = [fi.pain_index(fund.nv[interval], external=calargs.search[freq][interval]["external"]) for interval in
               intervals["pain_index"][freq]]
        res_irrelative.extend(val)
        _func_names.append("pain_index")

    if "CVaR" in funcs:
        val = [fi.CVaR(fund.rs[interval], external=calargs.search[freq][interval]["external"]) for interval in
               intervals["CVaR"][freq]]
        res_irrelative.extend(val)
        _func_names.append("CVaR")

    if "average_drawdown" in funcs:
        val = [fi.average_drawdown(fund.rs[interval], internal=
        calargs.search[freq][interval]["internal"], external=calargs.search[freq][interval]["external"]) for interval in
               intervals["average_drawdown"][freq]]
        res_irrelative.extend(val)
        _func_names.append("average_drawdown")

    if "pain_ratio" in funcs:
        val = [fi.pain_ratio(fund.nv[interval], tbond.rs[interval], _periods[freq], calargs.mode, internal=
        calargs.search[freq][interval]["internal"], external=calargs.search[freq][interval]["external"]) for interval in
               intervals["pain_ratio"][freq]]
        res_irrelative.extend(val)
        _func_names.append("pain_ratio")

    if "ERCVaR" in funcs:
        val = [fi.ERCVaR(fund.nv[interval], tbond.rs[interval], _periods[freq], calargs.mode, ["p", "r"], internal=
        calargs.search[freq][interval]["internal"], external=calargs.search[freq][interval]["external"]) for interval in
               intervals["ERCVaR"][freq]]
        res_irrelative.extend(val)
        _func_names.append("ERCVaR")

    if "sterling_a" in funcs:
        val = [fi.sterling_a(fund.nv[interval], tbond.rs[interval], _periods[freq], calargs.mode, internal=
        calargs.search[freq][interval]["internal"], external=calargs.search[freq][interval]["external"]) for interval in
               intervals["sterling_a"][freq]]
        res_irrelative.extend(val)
        _func_names.append("sterling_a")

    if "burke_a" in funcs:
        val = [fi.burke_a(fund.nv[interval], tbond.rs[interval], _periods[freq], calargs.mode, ["p", "r"], internal=
        calargs.search[freq][interval]["internal"], external=calargs.search[freq][interval]["external"]) for interval in
               intervals["burke_a"][freq]]
        res_irrelative.extend(val)
        _func_names.append("burke_a")

    if "kappa_a" in funcs:
        val = [fi.kappa_a(fund.nv[interval], tbond.rs[interval], _periods[freq], calargs.mode, ["p", "r"], internal=
        calargs.search[freq][interval]["internal"], external=calargs.search[freq][interval]["external"]) for interval in
               intervals["kappa_a"][freq]]
        res_irrelative.extend(val)
        _func_names.append("kappa_a")

    if "omega" in funcs:
        val = [fi.omega(fund.rs[interval], tbond.rs[interval], internal=
        calargs.search[freq][interval]["internal"], external=calargs.search[freq][interval]["external"]) for interval in
               intervals["omega"][freq]]
        res_irrelative.extend(val)
        _func_names.append("omega")

    if "hurst" in funcs:
        val = [fi.hurst_holder(fund.rs[interval], internal=
        calargs.search[freq][interval]["internal"], external=calargs.search[freq][interval]["external"]) for interval in
               intervals["hurst"][freq]]
        res_irrelative.extend(val)
        _func_names.append("hurst")

    if bms_used is not None:
        for bm_used in bms_used:
            benchmark = bms.get(bm_used)
            if benchmark is None:
                continue

            tmp = [fund.id, fund.name, statistic_date, calargs.bm[bm_used]]
            tmp.extend(res_irrelative)

            res_relative = []

            if "excess_return_a" in funcs:
                er_a = [fi.excess_return_a(fund.nv[interval], benchmark.price[interval], _periods[freq], calargs.mode,
                                           ["p", "p"], internal=calargs.search[freq][interval]["internal"],
                                           external=calargs.search[freq][interval]["external"]) for interval in
                        intervals["excess_return_a"][freq]]
                res_relative.extend(er_a)
                _func_names.append("excess_return_a")

            if "odds" in funcs:
                odds = [fi.odds(fund.rs[interval], benchmark.rs[interval],
                                external=calargs.search[freq][interval]["external"]) for interval in
                        intervals["odds"][freq]]
                res_relative.extend(odds)
                _func_names.append("odds")

            if "info_a" in funcs:
                info_a = [
                    fi.info_a(fund.nv[interval], benchmark.price[interval], _periods[freq], calargs.mode, ["p", "p"],
                              internal=
                              calargs.search[freq][interval]["internal"],
                              external=calargs.search[freq][interval]["external"]) for interval in
                    intervals["info_a"][freq]]
                res_relative.extend(info_a)
                _func_names.append("info_a")

            if "jensen_a" in funcs:
                jensen_a = [fi.jensen_a(fund.rs[interval], benchmark.rs[interval], tbond.rs[interval], _periods[freq],
                                        external=calargs.search[freq][interval]["external"]) for interval in
                            intervals["jensen_a"][freq]]
                res_relative.extend(jensen_a)
                _func_names.append("jensen_a")

            if "treynor_a" in funcs:
                treynor_a = [
                    fi.treynor_a(fund.nv[interval], benchmark.price[interval], tbond.rs[interval], _periods[freq],
                                 calargs.mode, ["p", "p", "r"], internal=
                                 calargs.search[freq][interval]["internal"],
                                 external=calargs.search[freq][interval]["external"]) for interval in
                    intervals["treynor_a"][freq]]
                res_relative.extend(treynor_a)
                _func_names.append("treynor_a")

            if "beta" in funcs:
                beta = [fi.beta(fund.rs[interval], benchmark.rs[interval], tbond.rs[interval],
                                external=calargs.search[freq][interval]["external"]) for interval in
                        intervals["beta"][freq]]
                res_relative.extend(beta)
                _func_names.append("beta")

            if "corr" in funcs:
                corr = [fi.corr(fund.rs[interval], benchmark.rs[interval],
                                external=calargs.search[freq][interval]["external"])[0] for interval in
                        intervals["corr"][freq]]
                res_relative.extend(corr)
                _func_names.append("corr")

            if "ability_timing" in funcs:
                ability_timing = [fi.competency_timing(fund.rs[interval], benchmark.rs[interval], tbond.rs[interval],
                                                       external=calargs.search[freq][interval]["external"]) for interval
                                  in intervals["ability_timing"][freq]]
                res_relative.extend(ability_timing)
                _func_names.append("ability_timing")

            if "ability_security" in funcs:
                ability_security = [fi.competency_stock(fund.rs[interval], benchmark.rs[interval], tbond.rs[interval],
                                                        external=calargs.search[freq][interval]["external"]) for
                                    interval in intervals["ability_security"][freq]]
                res_relative.extend(ability_security)
                _func_names.append("ability_security")

            if "persistence" in funcs:
                persistence = [fi.persistence_er(fund.rs[interval], benchmark.rs[interval],
                                                 external=calargs.search[freq][interval]["external"]) for interval in
                               intervals["persistence"][freq]]
                res_relative.extend(persistence)
                _func_names.append("persistence")

            if "unsystematic_risk" in funcs:
                unsystematic_risk = [fi.unsystematic_risk(fund.rs[interval], benchmark.rs[interval], tbond.rs[interval],
                                                          external=calargs.search[freq][interval]["external"]) for
                                     interval in intervals["unsystematic_risk"][freq]]
                res_relative.extend(unsystematic_risk)
                _func_names.append("unsystematic_risk")

            if "tracking_error_a" in funcs:
                tracking_error_a = [fi.tracking_error_a(fund.rs[interval], benchmark.rs[interval], _periods[freq],
                                                        external=calargs.search[freq][interval]["external"]) for
                                    interval in intervals["tracking_error_a"][freq]]
                res_relative.extend(tracking_error_a)
                _func_names.append("tracking_error_a")

            if "upsidecap" in funcs:
                val = [fi.upside_capture(fund.rs[interval], benchmark.rs[interval], _periods[freq], internal=calargs.search[freq][interval]["internal"],
                                         external=calargs.search[freq][interval]["external"]) for
                       interval in intervals["upsidecap"][freq]]
                res_relative.extend(val)
                _func_names.append("upsidecap")

            if "downsidecap" in funcs:
                val = [fi.downside_capture(fund.rs[interval], benchmark.rs[interval], _periods[freq], internal=calargs.search[freq][interval]["internal"],
                                           external=calargs.search[freq][interval]["external"]) for
                       interval in intervals["downsidecap"][freq]]
                res_relative.extend(val)
                _func_names.append("downsidecap")

            if "return_Msqr" in funcs:
                val = [fi.msq_return_a(fund.nv[interval], benchmark.nv[interval], tbond.rs[interval], _periods[freq],
                                       calargs.mode, ["p", "p", "r"], internal=calargs.search[freq][interval]["internal"], external=calargs.search[freq][interval]["external"]) for
                       interval in intervals["return_Msqr"][freq]]
                res_relative.extend(val)
                _func_names.append("return_Msqr")

            if "adjusted_jensen_a" in funcs:
                val = [fi.adjusted_jensen_a(fund.rs[interval], benchmark.rs[interval], tbond.rs[interval], _periods[freq], internal=calargs.search[freq][interval]["internal"],
                                       external=calargs.search[freq][interval]["external"]) for
                       interval in intervals["adjusted_jensen_a"][freq]]
                res_relative.extend(val)
                _func_names.append("adjusted_jensen_a")

            if "assess_ratio" in funcs:
                val = [fi.assess_ratio(fund.rs[interval], benchmark.rs[interval], tbond.rs[interval], internal=calargs.search[freq][interval]["internal"],
                                       external=calargs.search[freq][interval]["external"]) for
                       interval in intervals["assess_ratio"][freq]]
                res_relative.extend(val)
                _func_names.append("assess_ratio")

            if "excess_pl" in funcs:
                val = [fi.excess_pl(fund.rs[interval], benchmark.rs[interval], internal=calargs.search[freq][interval]["internal"],
                                       external=calargs.search[freq][interval]["external"]) for
                       interval in intervals["excess_pl"][freq]]
                res_relative.extend(val)
                _func_names.append("excess_pl")

            if "corr_spearman" in funcs:
                val = [fi.corr_spearman(fund.rs[interval], benchmark.rs[interval], internal=calargs.search[freq][interval]["internal"],
                                       external=calargs.search[freq][interval]["external"]) for
                       interval in intervals["corr_spearman"][freq]]
                val = [x[0] for x in val]
                res_relative.extend(val)
                _func_names.append("corr_spearman")

            if "beta_timing_camp" in funcs:
                val = [fi.beta_timing_CAMP(fund.rs[interval], benchmark.rs[interval], tbond.rs[interval], internal=calargs.search[freq][interval]["internal"],
                                       external=calargs.search[freq][interval]["external"]) for
                       interval in intervals["beta_timing_camp"][freq]]
                res_relative.extend(val)
                _func_names.append("beta_timing_camp")

            if ("timing_hm" in funcs) or ("stock_hm" in funcs):
                HMS = [fi.competency_HM(fund.rs[interval], benchmark.rs[interval], tbond.rs[interval], internal=calargs.search[freq][interval]["internal"], external=calargs.search[freq][interval]["external"]) for
                       interval in intervals["timing_hm"][freq]]

            if ("upbeta_cl" in funcs) or ("downbeta_cl" in funcs):
                CLS = [fi.competency_CL(fund.rs[interval], benchmark.rs[interval], tbond.rs[interval], internal=calargs.search[freq][interval]["internal"], external=calargs.search[freq][interval]["external"]) for
                       interval in intervals["upbeta_cl"][freq]]

            if "stock_hm" in funcs:
                val = [x[0] if x is not None else None for x in HMS]
                res_relative.extend(val)
                _func_names.append("stock_hm")

            if "timing_hm" in funcs:
                val = [x[1] if x is not None else None for x in HMS]
                res_relative.extend(val)
                _func_names.append("timing_hm")

            if "downbeta_cl" in funcs:
                val = [x[1] if x is not None else None for x in CLS]
                res_relative.extend(val)
                _func_names.append("downbeta_cl")

            if "upbeta_cl" in funcs:
                val = [x[2] if x is not None else None for x in CLS]
                res_relative.extend(val)
                _func_names.append("upbeta_cl")

            tmp.extend(res_relative)
            res.append(tmp)
    else:
        tmp = [fund.id, fund.name, statistic_date, *res_irrelative]
        res.append(tmp)

    if with_func_names is False:
        return res, None
    else:
        return res, _func_names[:len(funcs)]


class TaskProcessor:
    def __init__(self, tasks):
        self._tasks = tasks
        pass

    @classmethod
    def cal_tasks(cls, tasks):
        pass


_inds = [
    "accumulative_return", "return_a", "sharpe_a", "calmar_a", "sortino_a", "stdev", "stdev_a",
    "downside_deviation_a", "max_drawdown", "con_rise_periods", "con_fall_periods", "VaR", "p_earning_periods",
    "n_earning_periods", "min_return", "max_return", "mdd_repair_time", "mdd_time", "skewness", "kurtosis", "ERVaR",
    "excess_return_a", "odds", "info_a", "jensen_a", "treynor_a", "beta", "corr", "ability_timing", "ability_security",
    "persistence", "unsystematic_risk", "tracking_error_a"
]


def format_cols(funcs: list, freq: str, prefix=None):
    """
    Args:
        funcs:
        freq:
        prefix:

    Returns:

    """

    # 根据全局变量_inds构建func_name的排序字典
    sorts_dict = dict(zip(_inds, range(len(_inds))))
    # sorts_dict = dict(enumerate(funcs))
    inds = sorted(funcs, key=lambda x: sorts_dict[x])
    cols = []
    if prefix is not None:
        cols.extend(prefix)
    for ind in inds:
        tmp = [
            "{interval}_{indicator}".format(interval=sf.Map.Private.mp_interval[interval],
                                            indicator=sf.Map.Private.mp_ind[freq][ind])
            for interval in calargs.intervals[ind][freq]
        ]
        cols.extend(tmp)
    return cols


def format_cols_private(funcs: list, freq: str, prefix=None):
    """
    Args:
        funcs:
        freq:
        prefix:

    Returns:

    """
    # sorts_dict = dict(zip(funcs, range(len(_inds))))
    # sorts_dict = dict(enumerate(funcs))
    inds = funcs.copy()
    cols = []
    if prefix is not None:
        cols.extend(prefix)
    for ind in inds:
        tmp = [
            "{interval}_{indicator}".format(interval=sf.Map.Private.mp_interval[interval],
                                            indicator=sf.Map.Private.mp_ind[freq][ind])
            for interval in calargs.intervals[ind][freq]
        ]
        cols.extend(tmp)
    return cols


def format_cols_mutual(funcs: list, freq: str, prefix=None):
    """
    Args:
        funcs:
        freq:
        prefix:

    Returns:

    """
    # sorts_dict = dict(zip(funcs, range(len(_inds))))
    # sorts_dict = dict(enumerate(funcs))
    inds = funcs.copy()
    cols = []
    if prefix is not None:
        cols.extend(prefix)
    for ind in inds:
        tmp = [
            "{interval}_{indicator}".format(interval=sf.Map.Mutual.mp_interval[interval],
                                            indicator=sf.Map.Mutual.mp_ind[freq][ind])
            for interval in calargs_mutual.intervals[ind][freq]
        ]
        cols.extend(tmp)
    return cols


def format_cols_org4r(funcs: list, freq: str, prefix=None):
    """
    Args:
        funcs:
        freq:
        prefix:

    Returns:

    """
    # sorts_dict = dict(zip(funcs, range(len(_inds))))
    # sorts_dict = dict(enumerate(funcs))
    inds = funcs.copy()
    cols = []
    if prefix is not None:
        cols.extend(prefix)
    for ind in inds:
        tmp = [
            "{interval}_{indicator}".format(interval=sf.Map.Org4R.mp_interval[interval],
                                            indicator=sf.Map.Org4R.mp_ind[freq][ind])
            for interval in calargs_org4r.intervals[ind][freq]
        ]
        cols.extend(tmp)
    return cols
