from collections import Iterable
import numpy as np
from scipy import stats
from scipy.optimize import leastsq
from sklearn.linear_model import LinearRegression
import pandas as pd

_err_string = None


# Derived Indicator
# 1-1 Accumulative Return
def accumulative_return(series, ignore_check=False, **kwargs):
    """
        Given the price series, calculate the Accumulative Return.

    Args:
        series: list
            Series sorted by time in descending order;
        kwargs:
            internal:
            external
    Returns:
        Accumulative Return
    """
    kw_used = ["internal", "external"]
    internal, external = [kwargs.get(x, 0) for x in kw_used]

    try:
        series, series_internal, series_external = _gen_alternative_series(series, internal, external)
        value_alternative = _fetch_notnone(series_internal, series_external)

        if ignore_check is False:
            _check_series_length(series, length_min=2)

        price_latest = series[0]
        price_last = series[-1]

        if price_latest is None:
            return _err_string
        if price_last is None:
            price_last = value_alternative

        return_accu = price_latest / price_last - 1

        return return_accu

    except Exception as e:
        return _err_string


# 1-2 Annualized Return
def return_a(series, period_num, flavor_a, series_type, ignore_check=False, **kwargs):
    """
        Given the return series, frequency, calculate the Annualized Return.

    Args:
        series: list
            Return series sorted by time in descending order;
        period_num: int
            Number of periods included in a year, optional {250, 52, 12}, default 12;
            e.g. if 52 is given, it's considered that there are 52 periods in a year, usually refers to "weekly";
        interest_type: str
            Interest type used, optional {"single", "compound"}, default "compound";
    Returns:
        Annualized Return
    """
    internal, external = [kwargs.get(x, 0) for x in ("internal", "external")]

    try:
        if series_type[0] == "r":
            length_min = 3
        elif series_type[0] == "p":
            length_min = 4

        if flavor_a == "mean":
            series = transform_series_type(series, input_type=series_type, output_type=["r"], fill_none=False)
            if ignore_check is False:
                series = _slice_dropna(series, all=True)
                _check_series_length(series, length_min=length_min + external)

        elif flavor_a == "accumulative":
            series = transform_series_type(series, input_type=series_type, output_type=["p"])
            if ignore_check is False:
                _check_series_length(series, length_min=length_min + external)
        return annualized_return(series, period_num=period_num, flavor=flavor_a, internal=internal, external=external)

    except Exception as e:
        return _err_string


# 1-3 Annualized Excess Return
def excess_return_a(series, series_bm, period_num, flavor_a, series_type, ignore_check=False, **kwargs):
    """

    Args:
        series:
        series_bm:
        period_num:
        flavor_a:
        series_type:
        **kwargs:

    Returns:

    """
    internal, external = [kwargs.get(x, 0) for x in ("internal", "external")]

    try:
        if flavor_a == "mean":
            _output_type = ["r", "r"]
            series, series_bm = transform_series_type(
                series, series_bm, input_type=series_type, output_type=_output_type, fill_none=False
            )

            if ignore_check is False:
                series, series_bm = _slice_dropna(series, series_bm, all=False)
                _check_series_length(series, length_min=3 + external)

        elif flavor_a == "accumulative":
            _output_type = ["p", "p"]
            series, series_bm = transform_series_type(
                series, series_bm, input_type=series_type, output_type=_output_type
            )
            if ignore_check is False:
                _check_series_length(series, series_bm, length_min=4 + external)
                length_min = min(len(series), len(series_bm))
                series = series[:length_min]
                series_bm = series_bm[:length_min]

        r_a = return_a(series, period_num, flavor_a=flavor_a, series_type=_output_type[0], internal=internal, external=external, ignore_check=True)
        r_bm_a = return_a(series_bm, period_num, flavor_a=flavor_a, series_type=_output_type[1], internal=internal, external=external, ignore_check=True)

        return r_a - r_bm_a

    except Exception as e:
        return _err_string


# 1-7 Max Range of Continuous Increase
def range_continuous_rise(return_series, **kwargs):
    try:
        return_series = _gen_alternative_series(return_series, 0, kwargs.get("external", 0))[0]
        _check_series_length(return_series, length_min=2)

        subs = []
        sub = []
        return_series = return_series[::-1]

        # find all continuous rise months
        for i in range(len(return_series)):
            if return_series[i] is not None and return_series[i] > 0:
                sub.append(i)

                if i == len(return_series) - 1:
                    subs.append(sub)
                    sub = []
            else:
                subs.append(sub)
                sub = []

        return_series = np.array(return_series)
        subs = [np.array(sub) for sub in subs if len(sub) >= 2]
        if len(subs) != 0:
            subs_r = [(return_series[sub] + 1).cumprod()[-1] for sub in subs]  # calculate increase range
            max_range = max(subs_r) - 1
            idx = [len(return_series) - x - 1 for x in
                   subs[subs_r.index(max_range + 1)]]  # get the index of these months
            idx = tuple([idx[0], idx[-1]])
            return (max_range, idx)
        else:
            return 0, _err_string

    except Exception as e:
        return _err_string, _err_string


# 1-8 Number of Positive-Return Periods
def periods_positive_return(return_series, date_series=None, **kwargs):
    u"""
    根据给定区间内的时间序列, 收益率序列, 计算区间非正收益月份数.

    *Args:
        dateSeq(list): 区间内包含的时间序列(日期由近及远);
        rrSeq(list): 区间内该基金产品的收益率序列(日期由近及远);

    *Returns:
        区间正收益月份数;
    """
    try:
        return_series = _gen_alternative_series(return_series, 0, kwargs.get("external", 0))[0]

        if date_series is None:
            return_series = _slice_dropna(return_series)
            _check_series_length(return_series, length_min=1)

            idx = (np.array(return_series) > 0)
            return sum(idx)
        else:
            return_series, date_series = _slice_dropna(return_series, date_series)
            _check_series_length(return_series, date_series, length_min=1)

            idx = (np.array(return_series) > 0)
            return sum(idx), np.array(date_series[:-1])[idx]

    except Exception as e:
        return _err_string


# 1-9 Odds to Benchmark
def odds(return_series, return_series_bm, **kwargs):
    u"""
    根据给定区间内的收益率序列, 市场指标的收益率序列, 披露时间频度, 利率类型, 计算区间基金对基准指数胜率.

    *Args:
        dateSeq(list): 区间内包含的时间序列(日期由近及远);
        rrSeq(list): 区间内该基金产品的收益率序列(日期由近及远);

    *Returns:
        区间基金对基准指数胜率;
    """
    try:
        return_series = _gen_alternative_series(return_series, 0, kwargs.get("external", 0))[0]
        return_series_bm = _gen_alternative_series(return_series_bm, 0, kwargs.get("external", 0))[0]

        return_series, return_series_bm = _slice_dropna(return_series, return_series_bm)
        _check_series_length(return_series, return_series_bm, length_min=1)

        return sum(np.array(return_series) > np.array(return_series_bm)) / len(return_series)

    except Exception as e:
        return _err_string


# 1-10 Min Return
def min_return(return_series, **kwargs):
    u"""
    根据给定区间内净值序列, 计算区间最低单月回报率.

    *Args:
        swanavSeq(list): 区间内的(复权累计)净值序列(日期由近及远, 包含上个区间末的最后一次净值);

    *Returns:
        区间最低单月回报率.
    """
    try:
        return_series = _gen_alternative_series(return_series, 0, kwargs.get("external", 0))[0]

        return_series = _slice_dropna(return_series)
        _check_series_length(return_series, length_min=2)

        r_min = min(return_series)
        idx = return_series.index(r_min)
        return r_min, idx

    except Exception as e:
        return _err_string, _err_string


# 1-11 Max Return
def max_return(return_series, **kwargs):
    u"""
    根据给定区间内净值序列, 计算区间最高单月回报率.

    *Args:
        swanavSeq(list): 区间内的(复权累计)净值序列(日期由近及远, 包含上个区间末的最后一次净值);

    *Returns:
        区间最高单月回报率.
    """
    try:
        return_series = _gen_alternative_series(return_series, 0, kwargs.get("external", 0))[0]

        return_series = _slice_dropna(return_series)
        _check_series_length(return_series, length_min=2)

        r_max = max(return_series)
        idx = return_series.index(r_max)
        return r_max, idx

    except Exception as e:
        return _err_string, _err_string


# 2-1 Standard Deviation
def standard_deviation(return_series, ignore_check=False, **kwargs):
    u"""
    根据给定区间内收益率序列, 计算区间标准差.

    *Args:
        rrSeq(list): 区间内该基金产品的收益率序列(日期由近及远);

    *Returns:
        区间标准差.
    """
    try:
        return_series = _gen_alternative_series(return_series, 0, kwargs.get("external", 0))[0]

        if ignore_check is False:
            return_series = _slice_dropna(return_series)
            _check_series_length(return_series, length_min=2)

        # return (sum([(rr - np.mean(value_series)) ** 2 for rr in value_series]) / (len(value_series) - 1)) ** .5
        return np.std(return_series, ddof=1)

    except Exception as e:
        return _err_string


# 2-2 Annualized Standard Deviation
def standard_deviation_a(return_series, period_num, ignore_check=False, **kwargs):
    u"""
    根据给定区间内收益率序列, 披露时间频度, 计算区间标准差.

    *Args:
        rrSeq(list): 区间内该基金产品的收益率序列(日期由近及远);
        period_num(int): 根据披露时间频度所得的一年内周期个数.包括52(周), 12(月), 250(日). 默认频度为52周;

    *Returns:
        区间年化标准差.
    """
    try:
        return_series = _gen_alternative_series(return_series, 0, kwargs.get("external", 0))[0]

        if ignore_check is False:
            return_series = _slice_dropna(return_series)
            _check_series_length(return_series, length_min=3)

        return standard_deviation(return_series) * (period_num ** .5)

    except Exception as e:
        return _err_string


# 2-3 Annualized Downside Deviation
def downside_deviation_a(return_series, return_series_f, period_num, ignore_check=False, **kwargs):
    u"""
    根据给定区间内收益率序列, 一年期无风险国债的收益率序列, 披露时间频度, 计算区间年化下行标准差.

    *Args:
        rrSeq(list): 区间内该基金产品的收益率序列(日期由近及远);
        rrSeq_bm(list): 区间内市场指标的收益率序列(日期由近及远);
        period_num(int): 根据披露时间频度所得的一年内周期个数.包括52(周), 12(月), 250(日). 默认频度为52周;
        kwargs:
            order:

    *Returns:
        区间年化下行标准差.
    """
    try:
        return_series = _gen_alternative_series(return_series, 0, kwargs.get("external", 0))[0]
        order = kwargs.get("order", 2)
        if ignore_check is False:
            return_series, return_series_f = _slice_dropna(return_series, return_series_f, all=True)
            _check_series_length(return_series, return_series_f, length_min=3 if order == 2 else 4)  # 二阶矩3样本, 三阶矩4样本;

        T = len(return_series)
        return_series, return_series_f = np.array(return_series), np.array(return_series_f)
        delta_series = return_series - return_series_f
        delta_series[delta_series > 0] = 0
        up = np.sum(abs(delta_series ** order))
        if up >= 0:
            return (period_num ** .5) * ((up / (T - 1)) ** (1 / order))
        else:   # 处理负数的N次方根会得到复数的问题, 本函数只求实根
            return -(period_num ** .5) * abs(((float(up) / (T - 1)) ** (1 / order)))

    except Exception as e:
        return _err_string


def drawdown(p_series, **kwargs):
    """

    Args:
        p_series: list
            price series
        **kwargs:
            internal: int
            external: int
    Returns:

    """
    try:
        p_series = _gen_alternative_series(p_series, 0, kwargs.get("external", 0))[0]
        p_series = p_series[::-1]  # 传入的日期序列由大到小, 计算前需要先反转
        p_series = _slice_dropna(p_series)

        maxs = np.maximum.accumulate(p_series)
        dd = (maxs - p_series) / p_series

        return dd
    except:
        return _err_string


def pain_ratio(price_series, return_series_f, period_num, flavor_a, **kwargs):
    """"""
    try:
        series_type = ("p", "r")

        # 纯调用, 因此样本个数检查放入子函数;
        pidx = pain_index(price_series, **kwargs)

        if pidx == 0:
            return _err_string

        er_a = excess_return_a(price_series, return_series_f, period_num=period_num, flavor_a=flavor_a, series_type=series_type, **kwargs)
        return er_a / pidx

    except Exception:
        return _err_string


def adjusted_jensen_a(return_series, return_series_bm, return_series_f, period_num, **kwargs):
    """"""
    try:

        # 子函数做样本个数检查;
        beta_ = beta(return_series, return_series_bm, return_series_f, **kwargs)
        jenses_a_ = jensen_a(return_series, return_series_bm, return_series_f, period_num, **kwargs)
        return jenses_a_ / beta_

    except Exception:
        return _err_string


def omega(return_series, return_series_bm, **kwargs):
    """"""

    try:
        return_series = _gen_alternative_series(return_series, 0, kwargs.get("external", 0))[0]
        return_series_bm = _gen_alternative_series(return_series_bm, 0, kwargs.get("external", 0))[0]

        return_series, return_series_bm = _slice_dropna(return_series, return_series_bm, all=True)
        _check_series_length(return_series, return_series_bm, length_min=4)

        return_series = np.array(return_series)
        return_series_bm = np.array(return_series_bm)

        er_divisor = return_series - return_series_bm
        idx_up = er_divisor < 0
        K = sum(~idx_up)
        er_divisor[idx_up] = 0

        er_dividend = return_series_bm - return_series
        idx_down = er_dividend < 0
        J = sum(~idx_down)
        er_dividend[idx_down] = 0

        return np.sum(er_divisor) / np.sum(er_dividend) * (J / K)

    except Exception:
        return _err_string


def excess_pl(return_series, return_series_bm, **kwargs):

    """"""

    try:
        return_series = _gen_alternative_series(return_series, 0, kwargs.get("external", 0))[0]
        return_series_bm = _gen_alternative_series(return_series_bm, 0, kwargs.get("external", 0))[0]

        return_series, return_series_bm = _slice_dropna(return_series, return_series_bm)
        _check_series_length(return_series, return_series_bm, length_min=4)

        return_series = np.array(return_series)
        return_series_bm = np.array(return_series_bm)

        er_divisor = return_series - return_series_bm
        idx_up = er_divisor < 0
        er_divisor[idx_up] = 0

        er_dividend = return_series_bm - return_series
        idx_down = er_dividend < 0
        er_dividend[idx_down] = 0

        return np.sum(er_divisor) / np.sum(er_dividend)

    except Exception:
        return _err_string


def hurst_holder(return_series, **kwargs):
    """"""
    try:
        return_series = _gen_alternative_series(return_series, 0, kwargs.get("external", 0))[0]

        return_series = _slice_dropna(return_series)
        _check_series_length(return_series, length_min=4)

        rs = np.array(return_series)
        T = len(rs)
        zs = rs - rs.mean()
        ys = zs[::-1].cumsum()

        zs_std = np.std(zs, ddof=1)
        if zs_std == 0:
            return _err_string

        hurst = np.log((max(ys) - min(ys)) / zs_std) / np.log(T)
        return hurst

    except Exception as e:
        return _err_string


def corr_spearman(return_series, return_series_bm, **kwargs):
    """"""
    try:
        return_series = _gen_alternative_series(return_series, 0, kwargs.get("external", 0))[0]
        return_series_bm = _gen_alternative_series(return_series_bm, 0, kwargs.get("external", 0))[0]

        return_series, return_series_bm = _slice_dropna(return_series, return_series_bm, all=True)
        _check_series_length(return_series, return_series_bm, length_min=4)

        corr_, p_value = stats.spearmanr(return_series, return_series_bm)
        return corr_, p_value
    except:
        return _err_string, _err_string


def beta_timing_CAMP(return_series, return_series_bm, return_series_f, **kwargs):
    """"""

    try:
        return_series = _gen_alternative_series(return_series, 0, kwargs.get("external", 0))[0]
        return_series_bm = _gen_alternative_series(return_series_bm, 0, kwargs.get("external", 0))[0]
        return_series_f = _gen_alternative_series(return_series_f, 0, kwargs.get("external", 0))[0]

        return_series, return_series_bm, return_series_f = _slice_dropna(return_series, return_series_bm, return_series_f, all=True)

        # 样本检查由子函数beta完成
        return_series = np.array(return_series)
        return_series_bm = np.array(return_series_bm)
        return_series_f = np.array(return_series_f)

        rs_bf = return_series_bm - return_series_f

        idx_pos = rs_bf >= 0
        beta_pos = beta(return_series[idx_pos], return_series_bm[idx_pos], return_series_f[idx_pos])
        beta_neg = beta(return_series[~idx_pos], return_series_bm[~idx_pos], return_series_f[~idx_pos])
        beta_timing = beta_pos / beta_neg
        return beta_timing

    except:
        return _err_string


def competency_HM(return_series, return_series_bm, return_series_f, **kwargs):
    """"""
    try:
        return_series = _gen_alternative_series(return_series, 0, kwargs.get("external", 0))[0]
        return_series_bm = _gen_alternative_series(return_series_bm, 0, kwargs.get("external", 0))[0]
        return_series_f = _gen_alternative_series(return_series_f, 0, kwargs.get("external", 0))[0]

        return_series, return_series_bm, return_series_f = _slice_dropna(return_series, return_series_bm, return_series_f, all=True)
        _check_series_length(return_series, return_series_bm, return_series_f, length_min=4)

        return_series = np.array(return_series)
        return_series_bm = np.array(return_series_bm)
        return_series_f = np.array(return_series_f)

        er = return_series - return_series_f
        er_bm = return_series_bm - return_series_f

        er_bm_pos = er_bm.copy()
        er_bm_pos[er_bm_pos < 0] = 0

        # pd.DataFrame([er_bm, er_bm_pos]).T[::-1].to_csv("c:/Users/Yu/Desktop/er.csv")

        lr = LinearRegression().fit(np.array([er_bm, er_bm_pos]).T, np.array([er]).T)
        beta1, beta2 = lr.coef_[0]
        alpha = lr.intercept_[0]

        # 选股, 择时
        return alpha, beta2
    except Exception as e:
        return _err_string, _err_string

def competency_CL(return_series, return_series_bm, return_series_f, **kwargs):
    """"""
    try:
        return_series = _gen_alternative_series(return_series, 0, kwargs.get("external", 0))[0]
        return_series_bm = _gen_alternative_series(return_series_bm, 0, kwargs.get("external", 0))[0]
        return_series_f = _gen_alternative_series(return_series_f, 0, kwargs.get("external", 0))[0]

        return_series, return_series_bm, return_series_f = _slice_dropna(return_series, return_series_bm, return_series_f, all=True)
        _check_series_length(return_series, return_series_bm, return_series_f, length_min=4)

        return_series = np.array(return_series)
        return_series_bm = np.array(return_series_bm)
        return_series_f = np.array(return_series_f)

        er = return_series - return_series_f
        er_bm = return_series_bm - return_series_f

        er_bm_pos = er_bm.copy()
        er_bm_neg = er_bm.copy()
        er_bm_pos[er_bm_pos < 0] = 0
        er_bm_neg[er_bm_neg > 0] = 0

        lr = LinearRegression().fit(np.array([er_bm_neg, er_bm_pos]).T, np.array([er]).T)
        beta1, beta2 = lr.coef_[0]
        alpha = lr.intercept_[0]

        # 选股, 择时
        return alpha, beta1, beta2

    except:
        return _err_string, _err_string, _err_string

# 2-4 Max Drawdown
def max_drawdown(p_series, time_series=None, **kwargs):
    try:
        # todo 回撤是先去空再计算的, 因此在计算形成期, 修复期的时候会少算价格为空的期数;
        p_series = _gen_alternative_series(p_series, 0, kwargs.get("external", 0))[0]
        p_series = p_series[::-1]
        p_series = _slice_dropna(p_series)
        _check_series_length(p_series, length_min=2)
        # 回撤谷值的时间点

        maxs = np.maximum.accumulate(p_series)
        up = maxs - p_series
        end = np.argmax(up / maxs)

        if end == 0:
            return 0, (None, None, None)

        # 回撤峰值的时间点
        # begin = np.argmax(swanav_series[:end])
        begin = end - 1 - np.argmax(p_series[end - 1::-1])

        # 回撤恢复的时间点
        recovery = None
        if end < len(p_series):
            for i in range(end + 1, len(p_series)):
                if p_series[i] >= p_series[begin]:
                    recovery = i
                    break
                recovery = None
        else:
            recovery = None
        mdd = abs(p_series[end] / p_series[begin] - 1)
        timenodes = (begin, end, recovery)

        if time_series is None:
            # return mdd, timenodes
            return mdd, [
                timenodes[i] - timenodes[i - 1]
                if timenodes[i] is not None and timenodes[i - 1] is not None else None
                for i in range(1, len(timenodes))
            ]
        else:
            time_series = _slice_dropna(time_series)
            time_series = time_series[::-1]
            assert len(time_series) == len(p_series)
            return mdd, [
                (time_series[timenodes[i]] - time_series[timenodes[i - 1]]) / 86400 if timenodes[i] is not None and
                                                                                       timenodes[
                                                                                           i - 1] is not None else None
                for i in range(1, len(timenodes))]

    except Exception as e:
        return _err_string, _err_string


# 2-6 Beta Coefficient
def beta(return_series, return_series_bm, return_series_f, **kwargs):
    """
    根据给定区间内收益率序列, 市场指标的收益率率序列, 一年期无风险国债的收益率序列, 计算区间贝塔系数.

    Args:
        return_series: list<numeric>
            间内该基金产品的收益率序列(日期由近及远);
        return_series_bm: list<numeric>
            区间内市场指标的收益率序列(日期由近及远);
        return_series_f: list<numeric>
            区间内一年期无风险国债的收益率序列(日期由近及远);
        **kwargs:
            external: int, default 0
            序列中需要剔除的样本点的个数

    Returns:
        区间贝塔系数.

    """
    try:
        return_series = _gen_alternative_series(return_series, 0, kwargs.get("external", 0))[0]
        return_series_bm = _gen_alternative_series(return_series_bm, 0, kwargs.get("external", 0))[0]
        return_series_f = _gen_alternative_series(return_series_f, 0, kwargs.get("external", 0))[0]

        return_series, return_series_bm, return_series_f = _slice_dropna(return_series, return_series_bm, return_series_f)
        _check_series_length(return_series, return_series_bm, return_series_f, length_min=3)

        return_series = np.array(return_series)
        return_series_bm = np.array(return_series_bm)
        return_series_f = np.array(return_series_f)
        T = len(return_series)
        delta_if = return_series - return_series_f
        delta_bf = return_series_bm - return_series_f
        return (T * sum(delta_if * delta_bf) - sum(delta_if) * sum(delta_bf)) / (T * sum(delta_bf ** 2) - sum(delta_bf) ** 2)

    except Exception as e:
        return _err_string


def alpha(return_series, return_series_bm, return_series_f, **kwargs):
    """"""

    try:
        return_series = _gen_alternative_series(return_series, 0, kwargs.get("external", 0))[0]
        return_series_bm = _gen_alternative_series(return_series_bm, 0, kwargs.get("external", 0))[0]
        return_series_f = _gen_alternative_series(return_series_f, 0, kwargs.get("external", 0))[0]

        return_series, return_series_bm, return_series_f = _slice_dropna(return_series, return_series_bm, return_series_f)
        _check_series_length(return_series, return_series_bm, return_series_f, length_min=3)

        return_series = np.array(return_series)
        return_series_bm = np.array(return_series_bm)
        return_series_f = np.array(return_series_f)
        return_series_if = return_series - return_series_f
        return_series_bmf = return_series_bm - return_series_f

        beta_ = beta(return_series, return_series_bm, return_series_f)
        alpha_ = np.mean(return_series_if) - beta_ * np.mean(return_series_bmf)
        return alpha_
    except Exception:
        return _err_string


def assess_ratio(return_series, return_series_bm, return_series_f, **kwargs):
    """"""
    try:
        return_series = _gen_alternative_series(return_series, 0, kwargs.get("external", 0))[0]
        return_series_bm = _gen_alternative_series(return_series_bm, 0, kwargs.get("external", 0))[0]
        return_series_f = _gen_alternative_series(return_series_f, 0, kwargs.get("external", 0))[0]

        return_series, return_series_bm, return_series_f = _slice_dropna(return_series, return_series_bm, return_series_f)
        _check_series_length(return_series, return_series_bm, return_series_f, length_min=4)

        alpha_ = alpha(return_series, return_series_bm, return_series_f)
        unsys_risk = unsystematic_risk(return_series, return_series_bm, return_series_f)
        return alpha_ / unsys_risk
    except Exception:
        return _err_string


def sterling_a(pseries, series_f, period_num, flavor_a, **kwargs):
    """"""

    try:
        series_type = ("p", "r")

        # 子函数作样本个数检查;
        er_a = excess_return_a(pseries, series_f, period_num, flavor_a, series_type, **kwargs)

        return_series = transform_series_type(
            pseries, input_type=series_type, output_type=["r"], fill_none=False
        )

        dd_avg = average_drawdown(return_series, **kwargs)
        return er_a / dd_avg

    except Exception:
        return _err_string


def burke_a(pseries, series_f, period_num, flavor_a, series_type, **kwargs):
    """"""

    try:
        # 子函数作样本个数检查;
        dd_series = np.array(drawdown(pseries, **kwargs))
        dividend = np.sqrt(np.sum(dd_series ** 2))

        if dividend == 0:
            return _err_string

        er_a = excess_return_a(pseries, series_f, period_num, flavor_a, series_type, **kwargs)
        return er_a / dividend

    except Exception:
        return _err_string


def kappa_a(series, series_f, period_num, flavor_a, series_type, **kwargs):
    """"""

    try:
        # 样本检查(>=4)由子函数完成;
        return_series, return_series_f = transform_series_type(
            series, series_f, input_type=series_type, output_type=["r", "r"], fill_none=False
        )
        er_a = excess_return_a(series, series_f, period_num, flavor_a, series_type, **kwargs)
        dd_o3 = downside_deviation_a(return_series, return_series_f, period_num, order=3, **kwargs)
        return er_a / dd_o3

    except Exception:
        return _err_string


# 2-7 Correlation Coefficient
def corr(return_series, return_series_bm, **kwargs):
    u"""
    根据给定区间内收益率序列, 市场指标的收益率率序列, 计算区间贝塔系数.

    *Args:
        rrSeq(list): 区间内该基金产品的收益率序列(日期由近及远);
        rrSeq_bm(list): 区间内市场指标的收益率序列(日期由近及远);

    *Returns:
        区间相关系数.
    """
    try:
        return_series = _gen_alternative_series(return_series, 0, kwargs.get("external", 0))[0]
        return_series_bm = _gen_alternative_series(return_series_bm, 0, kwargs.get("external", 0))[0]

        return_series, return_series_bm = _slice_dropna(return_series, return_series_bm)
        _check_series_length(return_series, return_series_bm, length_min=2)

        result = stats.pearsonr(return_series, return_series_bm)

        if isinstance(result[0], float) and np.isnan(result[0]) == False:
            return result
        else:
            return _err_string, _err_string

    except Exception as e:
        return _err_string, _err_string


# 2-8 Unsystematic Risk
def unsystematic_risk(return_series, return_series_bm, return_series_f, **kwargs):
    u"""
    根据给定区间内收益率序列, 市场指标的收益率率序列, 一年期无风险国债的收益率序列, 计算区间非系统风险.

    *Args:
        rrSeq(list): 区间内该基金产品的收益率序列(日期由近及远);
        rrSeq_bm(list): 区间内市场指标的收益率序列(日期由近及远);
        rrSeq_tbond(list): 区间内一年期无风险国债的收益率序列(日期由近及远);

    *Returns:
        区间非系统风险.
    """
    try:
        return_series = _gen_alternative_series(return_series, 0, kwargs.get("external", 0))[0]
        return_series_bm = _gen_alternative_series(return_series_bm, 0, kwargs.get("external", 0))[0]
        return_series_f = _gen_alternative_series(return_series_f, 0, kwargs.get("external", 0))[0]

        return_series, return_series_bm, return_series_f = _slice_dropna(return_series, return_series_bm, return_series_f)
        _check_series_length(return_series, return_series_bm, return_series_f, length_min=3)

        return_series = np.array(return_series)
        return_series_bm = np.array(return_series_bm)
        return_series_f = np.array(return_series_f)
        T = len(return_series)
        return_series_if = return_series - return_series_f
        return_series_bmf = return_series_bm - return_series_f

        beta_ = beta(return_series, return_series_bm, return_series_f)
        alpha_ = alpha(return_series, return_series_bm, return_series_f)

        return np.sqrt((sum(return_series_if ** 2) - alpha_ * sum(return_series_if) - beta_ * sum(
            return_series_if * return_series_bmf)) / (T - 2))

    except Exception as e:
        return _err_string


# 2-9 Annualized Tracking Error
def tracking_error_a(return_series, return_series_bm, period_num, **kwargs):
    u"""
    根据给定区间内收益率序列, 市场指标的收益率率序列, 披露时间频度, 计算区间年化跟踪误差.

    *Args:
        rrSeq(list): 区间内该基金产品的收益率序列(日期由近及远);
        rrSeq_bm(list): 区间内市场指标的收益率序列(日期由近及远);
        period_num(int): 根据披露时间频度所得的一年内周期个数.包括52(周), 12(月), 250(日). 默认频度为52周;

    *Returns:
        区间年化跟踪误差.
    """
    try:
        return_series = _gen_alternative_series(return_series, 0, kwargs.get("external", 0))[0]
        return_series_bm = _gen_alternative_series(return_series_bm, 0, kwargs.get("external", 0))[0]

        return_series, return_series_bm = _slice_dropna(return_series, return_series_bm)
        _check_series_length(return_series, return_series_bm, length_min=3)

        return_series = np.array(return_series)
        return_series_bm = np.array(return_series_bm)
        return np.std(return_series - return_series_bm, ddof=1) * period_num ** .5

    except Exception as e:
        return _err_string


# 2-10 Value at Risk
def value_at_risk(return_series, M=1000, alpha=.05, ignore_check=False, **kwargs):
    u"""
    根据给定区间内收益率序列, 随机抽样次数, 置信度, 计算区间风险价值.

    *Args:
        rrSeq(list): 区间内该基金产品的收益率序列(日期由近及远);
        M(int): 随机抽样次数. 默认抽样次数为1000次;
        alpha(float): 置信度, 默认值为0.05;

    *Returns:
        区间风险价值.
    """
    try:
        return_series = _gen_alternative_series(return_series, 0, kwargs.get("external", 0))[0]

        if ignore_check is False:
            return_series = _slice_dropna(return_series)
            _check_series_length(return_series, length_min=2)

        np.random.seed(527)  # set seed

        N = len(return_series)
        j = int((N - 1) * alpha + 1)
        g = ((N - 1) * alpha + 1) - j
        rd_index = np.random.randint(N, size=(M, N))  #
        return_series_sorted = np.sort(np.array(return_series)[rd_index])  #
        # VaR_alpha = np.apply_along_axis(lambda x: -((1 - g) * x[j - 1] + g * x[j]), axis=1, arr=return_series_sorted).sum() / M
        # VaR_alpha = sum([-((1 - g) * x[j - 1] + g * x[j]) for x in return_series_sorted]) / M
        VaR_alpha = sum(-((1 - g) * return_series_sorted[:, j - 1] + g * return_series_sorted[:, j])) / M

        return max(0, VaR_alpha)

    except Exception as e:
        return _err_string


def CVaR(return_series, M=1000, alpha=.05, ignore_check=False, **kwargs):
    try:
        return_series = _gen_alternative_series(return_series, 0, kwargs.get("external", 0))[0]

        if ignore_check is False:
            return_series = _slice_dropna(return_series)
            _check_series_length(return_series, length_min=2)

        np.random.seed(527)  # set seed

        N = len(return_series)
        j = int((N - 1) * alpha + 1)
        g = ((N - 1) * alpha + 1) - j
        rd_index = np.random.randint(N, size=(M, N))  #
        return_series_sorted = np.sort(np.array(return_series)[rd_index])  #
        VaR_series = -((1 - g) * return_series_sorted[:, j - 1] + g * return_series_sorted[:, j])

        VaR_series = pd.Series(VaR_series)
        df_rs = pd.DataFrame(return_series_sorted).T

        CVaR_series = df_rs[df_rs <= (-VaR_series)].mean()
        CVaR_ = CVaR_series.mean()

        return max(0, -CVaR_)

    except Exception as e:
        return _err_string


# 2-11 Skewness
def skewness(return_series, **kwargs):
    '''

    Args:
        return_series:

    Returns:

    '''
    try:
        return_series = _gen_alternative_series(return_series, 0, kwargs.get("external", 0))[0]

        return_series = _slice_dropna(return_series)
        _check_series_length(return_series, length_min=2)

        return_series_std = standard_deviation(return_series)
        if return_series_std != 0:
            return_series_mean = np.mean(return_series)
            return np.sum((return_series - return_series_mean) ** 3) / (
                (len(return_series) - 1) * return_series_std ** 3)
        else:
            return _err_string

    except Exception as e:
        return _err_string


# 2-12 Kurtosis
def kurtosis(return_series, **kwargs):
    u"""
    根据给定区间内收益率序列, 计算区间峰度.

    *Args:
        rrSeq(list): 区间内该基金产品的收益率序列(日期由近及远);

    *Returns:
        区间峰度.
    """
    try:
        return_series = _gen_alternative_series(return_series, 0, kwargs.get("external", 0))[0]

        return_series = _slice_dropna(return_series)
        _check_series_length(return_series, length_min=2)

        return_series_std = standard_deviation(return_series)
        return_series_mean = np.mean(return_series)
        if return_series_std != 0:
            return sum((return_series - return_series_mean) ** 4) / ((len(return_series) - 1) * return_series_std ** 4)
        else:
            return _err_string

    except Exception as e:
        return _err_string


# 2-13 Max Range of Continuous Decrease
def range_continuous_fall(return_series, **kwargs):
    u"""
    根据给定区间内收益率序列, 计算区间最长连续下跌幅度.

    *Args:
        rrSeq(list): 区间内该基金产品的收益率序列(日期由近及远);

    *Returns:
        区间最长连续下跌幅度.
    """
    try:
        return_series = _gen_alternative_series(return_series, 0, kwargs.get("external", 0))[0]

        _check_series_length(return_series, length_min=2)

        res = []
        subs = []
        sub = []
        max_months = 0
        return_series = return_series[::-1]

        # find all continuous rise months
        for i in range(len(return_series)):
            if return_series[i] is not None and return_series[i] < 0:
                sub.append(i)

                if i == len(return_series) - 1:
                    subs.append(sub)
                    sub = []
            else:
                subs.append(sub)
                sub = []

        return_series = np.array(return_series)
        subs = [np.array(sub) for sub in subs if len(sub) >= 2]
        if len(subs) != 0:
            subs_r = [(return_series[sub] + 1).cumprod()[-1] for sub in subs]  # calculate increase range
            max_range = max(subs_r) - 1
            idx = [len(return_series) - x - 1 for x in
                   subs[subs_r.index(max_range + 1)]]  # get the index of these months
            idx = tuple([idx[0], idx[-1]])
            return (max_range, idx)
        else:
            return 0, _err_string

    except Exception as e:
        return _err_string, _err_string


# 2-14 Number of Non-Positive-Return Periods
def periods_negative_return(return_series, date_series=None, **kwargs):
    u"""
    根据给定区间内的时间序列, 收益率序列, 计算区间非正收益月份数.

    *Args:
        dateSeq(list): 区间内包含的时间序列(日期由近及远);
        rrSeq(list): 区间内该基金产品的收益率序列(日期由近及远);

    *Returns:
        区间非正收益月份数;
    """
    try:
        return_series = _gen_alternative_series(return_series, 0, kwargs.get("external", 0))[0]

        if date_series is None:
            return_series = _slice_dropna(return_series)
            _check_series_length(return_series, length_min=1)

            idx = (np.array(return_series) < 0)
            return sum(idx)
        else:
            return_series, date_series = _slice_dropna(return_series, date_series)
            _check_series_length(return_series, date_series, length_min=1)

            idx = (np.array(return_series) < 0)
            return sum(idx), np.array(date_series[:-1])[idx]

    except Exception as e:
        return _err_string


# 2-17 Pain Index
def pain_index(p_series, **kwargs):
    try:
        p_series = _gen_alternative_series(p_series, 0, kwargs.get("external", 0))[0]
        p_series = _slice_dropna(p_series)
        _check_series_length(p_series, length_min=4)

        dd = drawdown(p_series)
        # dd = dd[dd > 0]
        return dd[1:].mean()  # 输入的参数是净值序列, 据此计算出的动态回撤第一期必为0, 从第一期开始计算均值

    except Exception as e:
        return _err_string


# 2-19 Average Drawdown
def average_drawdown(return_series, **kwargs):
    try:
        return_series = _gen_alternative_series(return_series, 0, kwargs.get("external", 0))[0]
        return_series = _slice_dropna(return_series)
        _check_series_length(return_series, length_min=4)
        # 回撤谷值的时间点

        return_series = np.array(return_series)
        return_series[return_series > 0] = 0
        return abs(return_series.mean())

    except Exception as e:
        return _err_string


# 3-1 Annualized Sharpe Ratio
def sharpe_a(series, series_f, period_num, flavor_a, series_type, ignore_check=False, **kwargs):
    u"""
    根据给定区间内收益率序列, 市场指标的收益率序列, 披露时间频度, 利率类型, 计算区间年化夏普比率.

    *Args:
        rrSeq(list): 区间内该基金产品的收益率序列(日期由近及远);
        rrSeq_tbond(list): 区间内一年期无风险国债的收益率序列(日期由近及远);
        period_num(int): 根据披露时间频度所得的一年内周期个数.包括52(周), 12(月), 250(日). 默认频度为52周;
        interest_type(str): 利率类型, 包括复利"compound"和单利"single".默认值为"compound";

    *Returns:
        区间年化夏普比率.
    """
    internal, external = [kwargs.get(x, 0) for x in ("internal", "external")]

    try:
        return_series, return_series_f = transform_series_type(
            series, series_f, input_type=series_type, output_type=["r", "r"], fill_none=False
        )

        # return_series_std_a = standard_deviation_a(value_series[:len(value_series) - external], period_num,
        #                                            ignore_check=True)

        return_series_std_a = standard_deviation_a(return_series[:len(return_series) - external], period_num, ignore_check=False)

        if flavor_a == "mean":
            _output_type = ["r", "r"]
            series, series_f = return_series, return_series_f

            if ignore_check is False:
                series, series_f = _slice_dropna(series, series_f, all=False)
                _check_series_length(series, length_min=3 + external)

        elif flavor_a == "accumulative":
            _output_type = ["p", "p"]
            series, series_f = transform_series_type(
                series, series_f, input_type=series_type, output_type=_output_type
            )
            if ignore_check is False:
                _check_series_length(series, series_f, length_min=4 + external)

                length_min = min(len(series), len(series_f))
                series = series[:length_min]
                series_f = series_f[:length_min]

        if return_series_std_a <= 0.0001:
            return _err_string

        return excess_return_a(series, series_f, period_num, flavor_a, series_type=_output_type, internal=internal,
                               external=external, ignore_check=True) / return_series_std_a

    except Exception as e:
        return _err_string


# 3-2 Annualized Calmar Ratio
def calmar_a(price_series, series_f, period_num, flavor_a, series_type, ignore_check=False, **kwargs):
    u"""
    根据给定区间内收益率序列, 一年期无风险国债的收益率序列, 披露时间频度, 利率类型, 计算区间年卡玛诺比率.

    *Args:
        swanav_series(list): 区间内该基金产品的净值序列(日期由近及远);
        return_series_f(list): 区间内一年期无风险国债的收益率序列(日期由近及远);
        period_num(int): 根据披露时间频度所得的一年内周期个数.包括52(周), 12(月), 250(日). 默认频度为52周;
        interest_type(str): 利率类型, 包括复利"compound"和单利"single".默认值为"compound";

    *Returns:
        区间年化卡玛比率.
    """
    internal, external = [kwargs.get(x, 0) for x in ("internal", "external")]

    try:
        mdd = max_drawdown(price_series[:len(price_series) - external])[0]
        if flavor_a == "mean":
            _output_type = ["r", "r"]
            price_series, series_f = transform_series_type(
                price_series, series_f, input_type=series_type, output_type=_output_type, fill_none=False
            )

            if ignore_check is False:
                price_series, series_f = _slice_dropna(price_series, series_f, all=False)
                _check_series_length(price_series, length_min=3 + external)

        elif flavor_a == "accumulative":
            _output_type = ["p", "p"]
            price_series, series_f = transform_series_type(
                price_series, series_f, input_type=series_type, output_type=_output_type
            )

            if ignore_check is False:
                _check_series_length(price_series, series_f, length_min=4 + external)
                length_min = min(len(price_series), len(series_f))
                price_series = price_series[:length_min]
                series_f = series_f[:length_min]

        er_a = excess_return_a(price_series, series_f, period_num, flavor_a, series_type=_output_type,
                               internal=internal, external=external, ignore_check=True)
        if mdd == 0:
            return _err_string

        return er_a / mdd

    except Exception as e:
        return _err_string


# 3-3 Annualized Sortino Ratio
def sortino_a(series, series_f, period_num, flavor_a, series_type, ignore_check=False, **kwargs):
    """
    根据给定区间内收益率序列, 一年期无风险国债的收益率序列, 披露时间频度, 利率类型, 计算区间年化索提诺比率.

    *Args:
        rrSeq(list): 区间内该基金产品的收益率序列(日期由近及远);
        rrSeq_tbond(list): 区间内一年期无风险国债的收益率序列(日期由近及远);
        period_num(int): 根据披露时间频度所得的一年内周期个数.包括52(周), 12(月), 250(日). 默认频度为52周;
        interest_type(str): 利率类型, 包括复利"compound"和单利"single".默认值为"compound";

    *Returns:
        区间年化索提诺比率.
    """
    internal, external = [kwargs.get(x, 0) for x in ("internal", "external")]
    try:

        return_series, return_series_f = transform_series_type(
            series, series_f, input_type=series_type, output_type=["r", "r"], fill_none=False
        )

        if flavor_a == "mean":
            _output_type = ["r", "r"]
            series = return_series
            series_f = return_series_f
            if ignore_check is False:
                series, series_f = _slice_dropna(return_series, return_series_f, all=False)
                _check_series_length(series, length_min=3 + external)

        elif flavor_a == "accumulative":
            _output_type = ["p", "p"]
            series, series_f = transform_series_type(
                series, series_f, input_type=series_type, output_type=_output_type
            )
            if ignore_check is False:
                _check_series_length(series, series_f, length_min=4 + external)
                length_min = min(len(series), len(series_f))
                series = series[:length_min]
                series_f = series_f[:length_min]

        er_a = excess_return_a(series, series_f, period_num, flavor_a=flavor_a, series_type=_output_type,
                               ignore_check=True, internal=internal, external=external)

        dd_a = downside_deviation_a(return_series[:len(return_series) - external],
                                    return_series_f[:len(return_series_f) - external], period_num, ignore_check=False)
        if dd_a == 0:
            return _err_string

        return er_a / dd_a

    except Exception as e:
        return _err_string


# 3-4 Annualized Treynor Ratio
def treynor_a(series, series_bm, series_f, period_num, flavor_a, series_type, ignore_check=False, **kwargs):
    u"""
    根据给定区间内收益率序列, 市场指标的收益率序列, 一年期无风险国债的收益率序列, 披露时间频度, 利率类型, 计算区间年化特雷诺比率.

    *Args:
        rrSeq(list): 区间内该基金产品的收益率序列(日期由近及远);
        rrSeq_bm(list): 区间内市场指标的收益率序列(日期由近及远);
        rrSeq_tbond(list): 区间内一年期无风险国债的收益率序列(日期由近及远);
        period_num(int): 根据披露时间频度所得的一年内周期个数.包括52(周), 12(月), 250(日). 默认频度为52周;
        interest_type(str): 利率类型, 包括复利"compound"和单利"single".默认值为"compound";

    *Returns:
        区间年化特雷诺比率.
    """
    try:
        internal, external = [kwargs.get(x, 0) for x in ("internal", "external")]

        return_series, return_series_bm, return_series_f = transform_series_type(
            series, series_bm, series_f, input_type=series_type, output_type=["r", "r", "r"], fill_none=False
        )

        if flavor_a == "mean":
            _output_type = ["r", "r"]
            series = return_series
            series_f = return_series_f
            if ignore_check is False:
                series, series_f = _slice_dropna(return_series, return_series_f, all=False)
                _check_series_length(series, length_min=3 + external)

        elif flavor_a == "accumulative":
            _output_type = ["p", "p"]
            series, series_f = transform_series_type(
                series, series_f, input_type=[series_type[0], series_type[2]], output_type=_output_type
            )
            if ignore_check is False:
                _check_series_length(series, series_f, length_min=4 + external)
                length_min = min(len(series), len(series_f))
                series = series[:length_min]
                series_f = series_f[:length_min]

        er_a = excess_return_a(series, series_f, period_num, flavor_a=flavor_a, series_type=_output_type,
                               internal=internal, external=external, ignore_check=True)
        beta_ = beta(return_series[:len(return_series) - external], return_series_bm[:len(return_series_bm) - external],
                     return_series_f[:len(return_series_f) - external])
        if beta_ == 0:
            return _err_string
        return er_a / beta_

    except Exception as e:
        return _err_string


# 3-5 Annualized Info Ratio
def info_a(series, series_bm, period_num, flavor_a, series_type, ignore_check=False, **kwargs):
    u"""
    根据给定区间内收益率序列, 市场指标的收益率序列, 披露时间频度, 利率类型, 计算区间年化信息比率.

    *Args:
        rrSeq(list): 区间内该基金产品的收益率序列(日期由近及远);
        rrSeq_bm(list): 区间内市场指标的收益率序列(日期由近及远);
        period_num(int): 根据披露时间频度所得的一年内周期个数.包括52(周), 12(月), 250(日). 默认频度为52周;
        interest_type(str): 利率类型, 包括复利"compound"和单利"single".默认值为"compound";

    *Returns:
        区间年化信息比率.
    """
    try:
        internal, external = [kwargs.get(x, 0) for x in ("internal", "external")]

        return_series, return_series_bm = transform_series_type(
            series, series_bm, input_type=series_type, output_type=["r", "r"], fill_none=False
        )

        if flavor_a == "mean":
            _output_type = ["r", "r"]
            series = return_series
            series_bm = return_series_bm
            if ignore_check is False:
                series, series_bm = _slice_dropna(series, series_bm, all=False)
                _check_series_length(series, length_min=3 + external)

        elif flavor_a == "accumulative":
            _output_type = ["p", "p"]
            series, series_bm = transform_series_type(
                series, series_bm, input_type=series_type, output_type=_output_type
            )
            if ignore_check is False:
                _check_series_length(series, series_bm, length_min=4 + external)
                length_min = min(len(series), len(series_bm))
                series = series[:length_min]
                series_bm = series_bm[:length_min]

        er_a = excess_return_a(series, series_bm, period_num, flavor_a=flavor_a, series_type=_output_type, internal=internal, external=external)
        if tracking_error_a == 0:
            return _err_string
        return er_a / tracking_error_a(return_series[:len(return_series) - external],
                                       return_series_bm[:len(return_series_bm) - external], period_num)

    except Exception as e:
        return _err_string


# 3-6 Annualized Jensen Ratio
def jensen_a(return_series, return_series_bm, return_series_f, period_num, **kwargs):
    u"""
    根据给定区间内收益率序列, 市场指标的收益率序列, 一年期无风险国债的收益率序列, 披露时间频度, 利率类型, 计算区间年化詹森指数.

    *Args:
        rrSeq(list): 区间内该基金产品的收益率序列(日期由近及远);
        rrSeq_bm(list): 区间内市场指标的收益率序列(日期由近及远);
        rrSeq_tbond(list): 区间内一年期无风险国债的收益率序列(日期由近及远);
        period_num(int): 根据披露时间频度所得的一年内周期个数.包括52(周), 12(月), 250(日). 默认频度为52周;
        interest_type(str): 利率类型, 包括复利"compound"和单利"single".默认值为"compound";

    *Returns:
        区间年化詹森指数.
    """
    try:
        return_series = _gen_alternative_series(return_series, 0, kwargs.get("external", 0))[0]
        return_series_bm = _gen_alternative_series(return_series_bm, 0, kwargs.get("external", 0))[0]
        return_series_f = _gen_alternative_series(return_series_f, 0, kwargs.get("external", 0))[0]

        return_series, return_series_bm, return_series_f = _slice_dropna(return_series, return_series_bm,
                                                                         return_series_f)
        _check_series_length(return_series, return_series_bm, return_series_f, length_min=3)

        return_series = np.array(return_series)
        return_series_bm = np.array(return_series_bm)
        return_series_f = np.array(return_series_f)

        delta_if = return_series - return_series_f
        delta_bf = return_series_bm - return_series_f

        beta_ = beta(return_series, return_series_bm, return_series_f)
        alpha = np.mean(delta_if) - beta_ * np.mean(delta_bf)

        return (1 + alpha) ** period_num - 1

    except Exception as e:
        return _err_string


# 3-7 区间风险价值调整比
def ERVaR(series, series_f, period_num, flavor_a, series_type, M=1000, alpha=.05, ignore_check=False, **kwargs):
    u"""
    根据给定区间内收益率序列, 一年期无风险国债的收益率序列, 披露时间频度, 利率类型, 指定随机抽样次数, 置信度, 计算区间风险价值调整比.

    *Args:
        rrSeq(list): 区间内该基金产品的收益率序列(日期由近及远);
        rrSeq_tbond(list): 区间内一年期无风险国债的收益率序列(日期由近及远);
        period_num(int): 根据披露时间频度所得的一年内周期个数.包括52(周), 12(月), 250(日). 默认频度为52周;
        interest_type(str): 利率类型, 包括复利"compound"和单利"single".默认值为"compound";
        M(int): 随机抽样次数. 默认抽样次数为1000次;
        alpha(float): 置信度, 默认值为0.05;

    *Returns:
        区间风险价值调整比.
    """
    try:
        internal, external = [kwargs.get(x, 0) for x in ("internal", "external")]

        return_series, return_series_f = transform_series_type(
            series, series_f, input_type=series_type, output_type=["r", "r"], fill_none=False
        )

        if flavor_a == "mean":
            _output_type = ["r", "r"]
            series = return_series
            series_f = return_series_f
            if ignore_check is False:
                series, series_f = _slice_dropna(return_series, return_series_f, all=False)
                _check_series_length(series, length_min=3 + external)

        elif flavor_a == "accumulative":
            _output_type = ["p", "p"]
            series, series_f = transform_series_type(
                series, series_f, input_type=series_type, output_type=_output_type
            )
            if ignore_check is False:
                _check_series_length(series, series_f, length_min=4 + external)
                length_min = min(len(series), len(series_f))
                series = series[:length_min]
                series_f = series_f[:length_min]

        er_a = excess_return_a(series, series_f, period_num, flavor_a, series_type=_output_type, ignore_check=True,
                               internal=internal, external=external)
        VaR_alpha = value_at_risk(return_series=return_series, M=M, alpha=alpha, external=external)

        return er_a / max(0.0001, VaR_alpha)

    except Exception as e:
        return _err_string


def ERCVaR(series, series_f, period_num, flavor_a, series_type, M=1000, alpha=.05, ignore_check=False, **kwargs):
        """"""
        try:
            internal, external = [kwargs.get(x, 0) for x in ("internal", "external")]

            return_series, return_series_f = transform_series_type(
                series, series_f, input_type=series_type, output_type=["r", "r"], fill_none=False
            )

            if flavor_a == "mean":
                _output_type = ["r", "r"]
                series = return_series
                series_f = return_series_f
                if ignore_check is False:
                    series, series_f = _slice_dropna(return_series, return_series_f, all=False)
                    _check_series_length(series, length_min=3 + external)

            elif flavor_a == "accumulative":
                _output_type = ["p", "p"]
                series, series_f = transform_series_type(
                    series, series_f, input_type=series_type, output_type=_output_type
                )
                if ignore_check is False:
                    _check_series_length(series, series_f, length_min=4 + external)
                    length_min = min(len(series), len(series_f))
                    series = series[:length_min]
                    series_f = series_f[:length_min]

            er_a = excess_return_a(series, series_f, period_num, flavor_a, series_type=_output_type, ignore_check=True,
                                   internal=internal, external=external)
            CVaR_alpha = CVaR(return_series, M, alpha, external=external)

            return er_a / max(1e-4, CVaR_alpha)

        except Exception as e:
            return _err_string


# 3-8  Annualized M-Square Return
def msq_return_a(pseries, pseries_bm, series_f, period_num, flavor_a, series_type, ignore_check=False, **kwargs):
    internal, external = [kwargs.get(x, 0) for x in ("internal", "external")]

    # 最小样本个数要求4, 若子函数通过年化收益, 则主函数也满足要求
    try:
        return_series, return_series_bm, return_series_f = transform_series_type(
            pseries, pseries_bm, series_f, input_type=series_type, output_type=["r", "r", "r"], fill_none=False
        )

        std_a = standard_deviation_a(return_series[:len(return_series) - external], period_num)
        std_bm_a = standard_deviation_a(return_series_bm[:len(return_series_bm) - external], period_num)

        if flavor_a == "mean":
            _output_type = ["r", "r"]
            series, series_f = return_series, return_series_f

            if ignore_check is False:
                series, series_f = _slice_dropna(series, series_f, all=False)
                _check_series_length(series, length_min=3 + external)

        elif flavor_a == "accumulative":
            _output_type = ["p", "p"]
            series, series_f = transform_series_type(
                pseries, series_f, input_type=[series_type[0], series_type[2]], output_type=_output_type
            )
            if ignore_check is False:
                _check_series_length(series, series_f, length_min=4 + external)
                # length_min = min(len(series), len(series_f))
                # series = series[:length_min]
                # series_f = series_f[:length_min]

        sp = sharpe_a(series, series_f, period_num, flavor_a, _output_type, **kwargs)
        r_a = return_a(series, period_num, flavor_a, _output_type[:1], **kwargs)

        return r_a + sp * (std_bm_a - std_a)

    except Exception as e:
        return _err_string


# 4-1 Ability of Market Timing
def competency_timing(return_series, return_series_bm, return_series_f, **kwargs):
    u"""
    根据给定区间内收益率序列, 市场指标的收益率序列, 一年期无风险国债的收益率序列, 计算区间选时能力.

    *Args:
        rrSeq(list): 区间内该基金产品的收益率序列(日期由近及远);
        rrSeq_bm(list): 区间内市场指标的收益率序列(日期由近及远);
        rrSeq_tbond(list): 区间内一年期无风险国债的收益率序列(日期由近及远);

    *Returns:
        区间选时能力.
    """
    try:
        return_series = _gen_alternative_series(return_series, 0, kwargs.get("external", 0))[0]
        return_series_bm = _gen_alternative_series(return_series_bm, 0, kwargs.get("external", 0))[0]
        return_series_f = _gen_alternative_series(return_series_f, 0, kwargs.get("external", 0))[0]

        return_series, return_series_bm, return_series_f = _slice_dropna(return_series, return_series_bm, return_series_f, all=True)
        _check_series_length(return_series, return_series_bm, return_series_f, length_min=4)

        y = np.array(return_series) - np.array(return_series_f)
        x = np.array(return_series_bm) - np.array(return_series_f)
        lsq = leastsq(residuals_competency, [1, 1, 1], args=(x, y))
        return lsq[0][2]

    except Exception as e:
        return _err_string


# 4-2 Ability of Securities Selection
def competency_stock(return_series, return_series_bm, return_series_f, **kwargs):
    u"""
    根据给定区间内收益率序列, 市场指标的收益率序列, 一年期无风险国债的收益率序列, 计算区间选股能力.

    *Args:
        rrSeq(list): 区间内该基金产品的收益率序列(日期由近及远);
        rrSeq_bm(list): 区间内市场指标的收益率序列(日期由近及远);
        rrSeq_tbond(list): 区间内一年期无风险国债的收益率序列(日期由近及远);

    *Returns:
        区间选股能力.
    """
    try:
        return_series = _gen_alternative_series(return_series, 0, kwargs.get("external", 0))[0]
        return_series_bm = _gen_alternative_series(return_series_bm, 0, kwargs.get("external", 0))[0]
        return_series_f = _gen_alternative_series(return_series_f, 0, kwargs.get("external", 0))[0]

        return_series, return_series_bm, return_series_f = _slice_dropna(return_series, return_series_bm, return_series_f, all=True)
        _check_series_length(return_series, return_series_bm, return_series_f, length_min=4)

        y = np.array(return_series) - np.array(return_series_f)
        x = np.array(return_series_bm) - np.array(return_series_f)
        lsq = leastsq(residuals_competency, [1, 1, 1], args=(x, y))
        return lsq[0][0]

    except Exception as e:
        return _err_string


# 4-3 Persistence of Excess Return
def persistence_er(return_series, return_series_bm, **kwargs):
    u"""
    根据给定区间内收益率序列, 市场指标的收益率序列, 计算区间超额收率可持续性.

    *Args:
        rrSeq(list): 区间内该基金产品的收益率序列(日期由近及远);
        rrSeq_bm(list): 区间内市场指标的收益率序列(日期由近及远);

    *Returns:
        区间超额收益率可持续性.
    """
    try:
        return_series = _gen_alternative_series(return_series, 0, kwargs.get("external", 0))[0]
        return_series_bm = _gen_alternative_series(return_series_bm, 0, kwargs.get("external", 0))[0]

        return_series, return_series_bm = _slice_dropna(return_series, return_series_bm)
        _check_series_length(return_series, return_series_bm, length_min=2)

        T = len(return_series)
        er_series = np.array(return_series) - np.array(return_series_bm)
        er_series = er_series[::-1]
        err_avg = np.mean(er_series)
        rho = sum([(er_series[i] - err_avg) * (er_series[i - 1] - err_avg) for i in range(1, T)]) / sum(
            [(er_series[i] - err_avg) ** 2 for i in range(T)])
        return rho

    except Exception as e:
        return _err_string


# 4-4 Max Periods of Continuous Rise
def periods_continuous_rise(return_series, **kwargs):
    try:
        return_series = _gen_alternative_series(return_series, 0, kwargs.get("external", 0))[0]

        _check_series_length(return_series, length_min=2)

        res = []
        sub = []
        max = 0

        # 连续上涨月数应大于等于2
        for i in range(len(return_series)):
            if return_series[i] is not None and return_series[i] > 0:
                sub.append(i)

                if i == len(return_series) - 1:
                    if len(sub) > max:
                        max = len(sub)
                        res = [max, [sub]]
                        sub = []
                    elif len(sub) == max and len(sub) != 0:
                        res[1].append(sub)
                        sub = []
                    else:
                        sub = []
            else:
                if len(sub) > max:
                    max = len(sub)
                    res = [max, [sub]]
                    sub = []
                elif len(sub) == max and len(sub) != 0:
                    res[1].append(sub)
                    sub = []
                else:
                    sub = []
        if max <= 1:
            return 0, _err_string
        else:
            return tuple(res)

    except Exception as e:
        return _err_string, _err_string


# 4-5 Max Periods of Continuous Fall
def periods_continuous_fall(return_series, **kwargs):
    """
    根据给定区间内收益率序列, 计算区间最长连续下跌月数.

    *Args:
        rrSeq(list): 区间内该基金产品的收益率序列(日期由近及远);

    *Returns:
        区间最长连续下跌月数.
    """
    try:
        return_series = _gen_alternative_series(return_series, 0, kwargs.get("external", 0))[0]

        _check_series_length(return_series, length_min=2)

        res = []
        sub = []
        max = 0

        # 连续上涨月数应大于等于2
        for i in range(len(return_series)):
            if return_series[i] is not None and return_series[i] < 0:
                sub.append(i)

                if i == len(return_series) - 1:
                    if len(sub) > max:
                        max = len(sub)
                        res = [max, [sub]]
                        sub = []
                    elif len(sub) == max and len(sub) != 0:
                        res[1].append(sub)
                        sub = []
                    else:
                        sub = []
            else:
                if len(sub) > max:
                    max = len(sub)
                    res = [max, [sub]]
                    sub = []
                elif len(sub) == max and len(sub) != 0:
                    res[1].append(sub)
                    sub = []
                else:
                    sub = []
        if max <= 1:
            return 0, _err_string
        else:
            return tuple(res)

    except Exception as e:
        return _err_string, _err_string


def upside_capture(return_series, return_series_bm, period_num, **kwargs):
    """"""

    try:
        return_series = _gen_alternative_series(return_series, 0, kwargs.get("external", 0))[0]
        return_series, return_series_bm = _slice_dropna(return_series, return_series_bm, all=True)
        _check_series_length(return_series, length_min=4)

        T = len(return_series)
        return_series = np.array(return_series)
        return_series_bm = np.array(return_series_bm)

        idx_upside = return_series_bm > 0
        divisor = (1 + return_series[idx_upside]).prod() ** (period_num / T) - 1
        dividend = (1 + return_series_bm[idx_upside]).prod() ** (period_num / T) - 1

        return divisor / dividend

    except Exception:
        return _err_string


def downside_capture(return_series, return_series_bm, period_num, **kwargs):
    """"""

    try:
        return_series = _gen_alternative_series(return_series, 0, kwargs.get("external", 0))[0]

        return_series, return_series_bm = _slice_dropna(return_series, return_series_bm, all=True)
        _check_series_length(return_series, length_min=4)

        T = len(return_series)
        return_series = np.array(return_series)
        return_series_bm = np.array(return_series_bm)

        idx_downside = return_series_bm < 0
        divisor = (1 + return_series[idx_downside]).prod() ** (period_num / T) - 1
        dividend = (1 + return_series_bm[idx_downside]).prod() ** (period_num / T) - 1

        return divisor / dividend

    except Exception:
        return _err_string


# helper function
def _slice_dropna(*series, all=True):
    """
        First, slice all the series to same length. Second, if `all` is True, index of all series which contains None
    will be dropped; Else, index of each series will be dropped individually.

    Args:
        *series:
        all:

    Returns:

    """
    length = len(series)
    length_min = min([len(serie) for serie in series])
    series = list(map(lambda x: x[:length_min], series))
    if all is True:
        series = np.array(series).T.tolist()
        series = list(filter(lambda x: None not in x, series))
        series = np.array(series).T.tolist()
    elif all is False:
        series = [list(filter(lambda x: x is not None, y)) for y in series]

    if length > 1:
        return series
    elif length == 1:
        return series[0]


def _check_series_length(*series, length_min):
    """
        检查输入样本的数据类型是否为Iterable及样本数量是否符合要求, 则抛出AssertionError.

    *Args:
        series(list): 序列形式的样本;

    *Kwargs:
        length_min(int): 最少需要的样本个数;
    """

    for serie in series:
        assert isinstance(serie, Iterable), "参数%s应为Iterable类型,而非%s类型." % ("series", type(serie))
        assert len(serie) >= length_min, "本公式计算所需的样本量应大于等于%s个,输入的样本量为%s个." % (
            str(length_min), str(len(serie)))


def transform_series_type(*series, input_type, output_type, fill_none="default"):
    """
        Transform series type to the `output_type`. If `input_type` = `output_type", no change will be done.
        *Instead of being dropped, when price series is transformed to return series, the None element will be filled
        by 1, or when return series is transformed to price series, the None element will be filled by 0, so as to keep
        the series length constant to the original series.

    Args:
        *series:
        input_type:
        output_type:
        fill_none: optional {"default", False, lambda<>}, default False;
        **kwargs:

    Returns:

    """
    result = []
    if fill_none is False:
        _apply = {
            "p": lambda x: x,
            "r": lambda x: x
        }

    elif fill_none == "default":
        _apply = {
            "p": lambda x: _fill_none(x, 1),
            "r": lambda x: _fill_none(x, 0)
        }

    for serie, type_i, type_o in zip(series, input_type, output_type):
        if type_i == type_o:
            result.append(serie)
            continue
        else:
            if type_i == "p":
                serie = _transform_series(_apply["p"](serie), type_o, reverse=False)

            elif type_i == "r":
                serie = _transform_series(_apply["r"](serie), type_o, reverse=True)
            result.append(serie)
    if len(result) > 1:
        return result
    elif len(result) == 1:
        return result[0]


def _check_series(*series, **kwargs):
    """
        Helper function to check whether the series meet the requirement of calculation.

    Args:
        *series: list<float>, or list<list>
            Series to be sliced and checked;

        **kwargs:
            all: boolean
                If True is passed, if all the indexes of all input series which appears None will be dropped, otherwise,
                only the index of series which contains the None value will be dropped. Default True;
            input_type: list<str>
                This argument is used to clarify the input series type. Optional value are "r" and "p";
            output_type: list<str>
                This argument is used to clarify the expected output series type. If the input_type of a series doesn't
                matches with the expected output_type, then 1) _gen_price_series will be used to a return series to
                generate a normalized price series; 2) _gen_return_series will be used to the price series so as to
                generate a return series;
            length_min: int
                The minimum sample number. If any of the series doesn't meet the requirement, then an assertion error
                will be raised;

    Returns:
        Processed series: list<float>, or list<list>
    """
    if len(series) > 1:
        series = transform_series_type(*series, input_type=kwargs.get("input_type"),
                                       output_type=kwargs.get("output_type"))
        series = _slice_dropna(*series, all=kwargs.get("all", True))
        _check_series_length(*series, length_min=kwargs.get("length_min"))
    elif len(series) == 1:
        series = transform_series_type(series, input_type=kwargs.get("input_type"),
                                       output_type=kwargs.get("output_type"), apply=kwargs.get("fill_none"))
        series = _slice_dropna(series, all=kwargs.get("all", True))
        _check_series_length(series, length_min=kwargs.get("length_min"))

    return series


def _fetch_notnone(*series):
    result = {}
    for item in series[::-1]:
        count = 0
        for element in item:
            if element is not None:
                result[count] = element
                break
            else:
                count += 1
    if len(result) == 0:
        return None
    else:
        return result.get(min(result.keys()))


def _fill_none(series, value):
    tmp = series[::]
    for i in range(len(series)):
        if series[i] is None:
            tmp[i] = value
    return tmp


def _gen_price_series(return_series):
    return_series = np.array(return_series)
    price_series = np.cumprod((1 + return_series))
    price_series = price_series[::-1].tolist()
    price_series.append(1.0)
    return price_series


def _gen_return_series(price_series, drop_none=False):
    """
    根据给定价格序列, 计算收益率序列.

    *Args:
        price_series(list): 价格序列(日期由近及远, 价格序列应包含上个区间末的最后一次披露的价格, 以计算本区间第一个收益率);

    *Returns:
        收益率序列(日期由近及远).
    """
    try:
        if drop_none:
            price_series = [x for x in price_series if x is not None]

        return [
            price_series[i] / price_series[i + 1] - 1
            if ((price_series[i] is not None) and (price_series[i + 1] is not None))
            else None for i in range(len(price_series) - 1)
        ]

    except Exception as e:
        return []


def _gen_alternative_series(series_original, internal=0, external=0):
    """
        用于将原有序列进行内外切分, 新序列长度 = 原序列长度 - 外序列长度
    Args:
        series_original:
        internal:
        external:

    Returns:
        series_new, series_internal, series_external

    """
    if internal == external == 0:
        return series_original, [], []
    length = len(series_original)
    series_new = series_original[:length - external]
    # series_internal = series_original[length - external - 1:(length - external - internal) - 1:-1]

    # 修正当internal >= length - external - 1时引起的负列表下标问题;
    outer = length - external - 1
    if internal <= outer:
        inner = outer - internal
    else:
        inner = 0
    series_internal = series_original[outer:inner:-1]
    series_external = series_original[length - external:]
    return series_new, series_internal, series_external


def _transform_series(series, tgt_type, reverse=False, **kwargs):
    if reverse:
        series = series[::-1]
    if tgt_type == "p":
        return _gen_price_series(series)
    elif tgt_type == "r":
        return _gen_return_series(series)


# Annualized Return
def annualized_return(series, period_num, flavor, ignore_check=False, **kwargs):
    """

    Args:
        series:
        period_num: 52, 12
        flavor: "mean", "accumulative"
        series_type: ["p"], or ["r"]
        **kwargs:
            internal:
            external:

    Returns:

    """
    kw_used = ("internal", "external")
    internal, external = [kwargs.get(x, 0) for x in kw_used]

    try:
        if flavor == "mean":  # use return series
            series = _gen_alternative_series(series, internal, external)[0]
            series = _slice_dropna(series)
            return (1 + np.mean(series)) ** period_num - 1

        elif flavor == "accumulative":  # use price_series

            return_accu = accumulative_return(series, internal=internal, external=external, ignore_check=ignore_check)
            return (1 + return_accu) ** (period_num / (len(series) - external - 1)) - 1

    except Exception as e:
        return _err_string


# 最小二乘估计_区间选时&选股能力
def func_competency(x, params):
    alpha, beta1, beta2 = params
    return alpha + beta1 * x + beta2 * (x ** 2)


def residuals_competency(params, xdata, ydata):
    return ydata - func_competency(xdata, params)


# import numpy as np
#
# np.random.randint(4, size=(10, 4))
