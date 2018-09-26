_intervals = (1, 3, 6, 12, 24, 36, 60, "w", "m", "q", "a", "whole")

_externals_d = (0, 0, 0, 22, 22, 22, 22, 0, 0, 0, 22, 0)
_internals_d = (22, 22, 22, 22, 22, 22, 22, 22, 22, 22, 22, 10000)

_externals_w = (0, 0, 0, 4, 4, 4, 4, 0, 0, 0, 4, 0)
_internals_w = (0, 0, 0, 5, 5, 5, 5, 0, 0, 0, 5, 1000)

_externals_m = (0, 0, 0, 1, 1, 1, 1, 0, 0, 0, 1, 0)
_internals_m = (0, 0, 0, 2, 2, 2, 2, 0, 0, 0, 2, 100)

_default_intervals = {
    "d": [1, 3, 6, 12, 24, 36, 60, "m", "q", "a", "whole"],
    "w": [1, 3, 6, 12, 24, 36, 60, "q", "a", "whole"],
    "m": [3, 6, 12, 24, 36, 60, "a", "whole"]
}
_default_intervals2 = {
    "d": [1, 3, 6, 12, 24, 36, 60, "m", "q", "a", "whole"],
    "w": [1, 3, 6, 12, 24, 36, 60, "q", "a", "whole"],
    "m": [6, 12, 24, 36, 60, "a", "whole"]
}


# internal/external search args
search = {
    "d": {
        interval: {"internal": internal, "external": external}
        for interval, internal, external in zip(_intervals, _internals_d, _externals_d)
    },
    "w": {
        interval: {"internal": internal, "external": external}
        for interval, internal, external in zip(_intervals, _internals_w, _externals_w)
        },
    "m": {
        interval: {"internal": internal, "external": external}
        for interval, internal, external in zip(_intervals, _internals_m, _externals_m)
        }
}

# annualize mode
mode = "accumulative"

intervals = {
    "accumulative_return": {
        "d": [1, 3, 6, 12, 24, 36, 60, "w", "m", "q", "a", "whole"],
        "w": [1, 3, 6, 12, 24, 36, 60, "m", "q", "a", "whole"],
        "m": [3, 6, 12, 24, 36, 60, "q", "a", "whole"]
    },
    "return_a": _default_intervals,
    "excess_return_a": _default_intervals,
    "odds": _default_intervals,
    "sharpe_a": _default_intervals,
    "calmar_a": _default_intervals,
    "sortino_a": _default_intervals,
    "info_a": _default_intervals,
    "jensen_a": _default_intervals,
    "treynor_a": _default_intervals,
    "stdev": _default_intervals,
    "stdev_a": _default_intervals,
    "downside_deviation_a": _default_intervals,
    "max_drawdown": _default_intervals,
    "beta": _default_intervals,
    "corr": _default_intervals,
    "con_rise_periods": _default_intervals,
    "con_fall_periods": _default_intervals,
    "ability_timing": _default_intervals,
    "ability_security": _default_intervals,
    "persistence": _default_intervals,
    "unsystematic_risk": _default_intervals,
    "tracking_error_a": _default_intervals,
    "VaR": _default_intervals,
    "p_earning_periods": _default_intervals,
    "n_earning_periods": _default_intervals,
    "max_return": _default_intervals,
    "min_return": _default_intervals,
    "mdd_repair_time": _default_intervals,
    "mdd_time": _default_intervals,
    "skewness": _default_intervals,
    "kurtosis": _default_intervals,
    "ERVaR": _default_intervals,
    # v2
    "ddr3_a": _default_intervals2,
    "pain_index": _default_intervals2,
    "CVaR": _default_intervals2,
    "average_drawdown": _default_intervals2,
    "upsidecap": _default_intervals2,
    "downsidecap": _default_intervals2,
    "pain_ratio": _default_intervals2,
    "ERCVaR": _default_intervals2,
    "return_Msqr": _default_intervals2,
    "adjusted_jensen_a": _default_intervals2,
    "assess_ratio": _default_intervals2,
    "sterling_a": _default_intervals2,
    "hurst": _default_intervals2,
    "timing_hm": _default_intervals2,
    "stock_hm": _default_intervals2,
    "upbeta_cl": _default_intervals2,
    "downbeta_cl": _default_intervals2,
    "burke_a": _default_intervals2,
    "kappa_a": _default_intervals2,
    "omega": _default_intervals2,
    "excess_pl": _default_intervals2,
    "beta_timing_camp": _default_intervals2,
    "corr_spearman": _default_intervals2,
}

# type_mapping
tm = {
    # 60401: "FI02",
    # 6010702: "FI03",
    # 6010101: "FI04",
    # 6010102: "FI05",
    # 6010103: "FI06",
    # 60105: "FI07",
    # 60102: "FI08",
    # 60106: "FI09",
    # 60104: "FI10",
    # 60103: "FI11",
    # 60108: "FI12",
    # 60107: "FI13"
}

bm = {
    # "FI01": 6,
    # "FI02": 5,
    # "FI03": 5,
    # "FI04": 5,
    # "FI05": 5,
    # "FI06": 5,
    # "FI07": 5,
    # "FI08": 5,
    # "FI09": 5,
    # "FI10": 5,
    # "FI11": 5,
    # "FI12": 5,
    # "FI13": 5,
    "hs300": 1,
    "csi500": 2,
    "sse50": 3,
    "cbi": 4,  # 6
    "nfi": 5,  # 7
}
