from collections import Iterable
import numpy as np
from scipy import stats
from scipy.optimize import leastsq

_err_string = None


def check_series(*series, **kwargs):
    """
    检查输入样本的数据类型是否为Iterable及样本数量是否符合要求, 则抛出AssertionError.

    *Args:
        series(list): 序列形式的样本;

    *Kwargs:
        length_min(int): 最少需要的样本个数;
    """

    for serie in series:
        assert isinstance(serie, Iterable), "参数%s应为数组类型,而非%s类型." % ("price_series", type(serie))
        assert len(serie) >= kwargs["length_min"], "本公式计算所需的样本量应大于等于%s个,输入的样本量为%s个." % (
            str(kwargs["length_min"]), str(len(serie)))


def slice_dropna(*series, all=True):
    length = len(series)
    length_min = min([len(serie) for serie in series])
    series = list(map(lambda x: x[:length_min], series))
    if all == True:
        series = np.array(series).T.tolist()
        series = list(filter(lambda x: None not in x, series))
        series = np.array(series).T.tolist()
    elif all == False:
        series = [list(filter(lambda x: x is not None, y)) for y in series]

    if length > 1:
        return series
    elif length == 1:
        return series[0]


# Derived Indicator
# 1-1 区间累计收益率/增长率
def accumulative_return(price_series, lookback=False):
    """
    根据给定区间内净值序列, 计算区间累计收益率.

    *Args:
        price_series(list): 价格序列(日期由近及远, 价格序列应包含上个区间末的最后一次披露的价格, 以计算本区间第一个收益率);

    *Returns:
        区间累计收益率.
    """
    try:
        check_series(price_series, length_min=2)

        price_latest = price_series[0]
        price_last = price_series[-1]
        accu_return = price_latest / price_last - 1 if (
            price_last is not None and price_last is not None) else _err_string
        if lookback is False:
            return accu_return
        else:
            pass  # unimplemented, should return a tuple like (accumulative_return, num_of_samples)

    except Exception as e:
        return _err_string


# 1-2 区间年化收益率
def return_a(return_series, period_num=52):
    """
    根据给定区间内收益率序列, 披露时间频度, 利率类型, 计算区间年化收益率.

    *Args:
        value_series(list): 区间内该基金产品的收益率序列(日期由近及远);
        period_num(int): 根据披露时间频度所得的一年内周期个数.包括52(周), 12(月), 250(日). 默认频度为52周;
        interest_type(str): 利率类型, 包括复利"compound"和单利"single".默认值为"compound";
               
    *Returns:
        区间年化收益率.
    """
    try:
        return_series = slice_dropna(return_series)
        check_series(return_series, length_min=3)

        return annualized_return(return_series, period_num)

    except Exception as e:
        return _err_string


# 1-3 区间超额年化收益率
def excess_return_a(return_series, return_series_bm, period_num=52):
    """
    根据给定区间内收益率序列, 一年期无风险国债的收益率序列, 披露时间频度, 利率类型, 计算区间超额年化收益率.

    *Args:
        value_series(list): 区间内该基金产品的收益率序列(日期由近及远);
        return_series_bm(list): 区间内市场指标的收益率序列(日期由近及远);
        period_num(int): 根据披露时间频度所得的一年内周期个数.包括52(周), 12(月), 250(日). 默认频度为52周;
        interest_type(str): 利率类型, 包括复利"compound"和单利"single".默认值为"compound";

    *Returns:
        区间超额年化收益率.
    """
    try:
        return_series, return_series_bm = slice_dropna(return_series, return_series_bm, all=False)
        check_series(return_series, return_series_bm, length_min=3)

        ir_a = annualized_return(return_series, period_num)
        ir_bm_a = annualized_return(return_series_bm, period_num)
        return ir_a - ir_bm_a

    except Exception as e:
        return _err_string


# 1-5 年度收益率

# 1-6 区间费前累计收益率

# 1-7 区间连续上涨的最大幅度
def range_continuous_rise(return_series):
    try:
        check_series(return_series, length_min=2)

        res = []
        subs = []
        sub = []
        max_months = 0
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


# 1-8 区间正收益月数
def periods_positive_return(return_series, date_series=None):
    u"""
    根据给定区间内的时间序列, 收益率序列, 计算区间非正收益月份数.

    *Args:
        dateSeq(list): 区间内包含的时间序列(日期由近及远);
        rrSeq(list): 区间内该基金产品的收益率序列(日期由近及远);
        
    *Returns:
        区间正收益月份数;
    """
    try:
        if date_series is None:
            return_series = slice_dropna(return_series)
            check_series(return_series, length_min=1)

            idx = (np.array(return_series) > 0)
            return sum(idx)
        else:
            return_series, date_series = slice_dropna(return_series, date_series)
            check_series(return_series, date_series, length_min=1)

            idx = (np.array(return_series) > 0)
            return sum(idx), np.array(date_series[:-1])[idx]

    except Exception as e:
        return _err_string


# 1-9 区间基金对基准指数胜率
def odds(return_series, return_series_bm):
    u"""
    根据给定区间内的收益率序列, 市场指标的收益率序列, 披露时间频度, 利率类型, 计算区间基金对基准指数胜率.

    *Args:
        dateSeq(list): 区间内包含的时间序列(日期由近及远);
        rrSeq(list): 区间内该基金产品的收益率序列(日期由近及远);
        
    *Returns:
        区间基金对基准指数胜率;
    """
    try:
        return_series, return_series_bm = slice_dropna(return_series, return_series_bm)
        check_series(return_series, return_series_bm, length_min=1)

        return sum(np.array(return_series) > np.array(return_series_bm)) / len(return_series)

    except Exception as e:
        return _err_string


# 1-10 区间最低单月回报
def min_return(return_series):
    u"""
    根据给定区间内净值序列, 计算区间最低单月回报率.

    *Args:
        swanavSeq(list): 区间内的(复权累计)净值序列(日期由近及远, 包含上个区间末的最后一次净值);
        
    *Returns:
        区间最低单月回报率.
    """
    try:
        return_series = slice_dropna(return_series)
        check_series(return_series, length_min=2)

        r_min = min(return_series)
        idx = return_series.index(r_min)
        return r_min, idx

    except Exception as e:
        return _err_string, _err_string


# 1-11 区间最高单月回报
def max_return(return_series):
    u"""
    根据给定区间内净值序列, 计算区间最高单月回报率.

    *Args:
        swanavSeq(list): 区间内的(复权累计)净值序列(日期由近及远, 包含上个区间末的最后一次净值);
        
    *Returns:
        区间最高单月回报率.
    """
    try:
        return_series = slice_dropna(return_series)
        check_series(return_series, length_min=2)

        r_max = max(return_series)
        idx = return_series.index(r_max)
        return r_max, idx

    except Exception as e:
        return _err_string, _err_string


# 2-1 区间标准差
def standard_deviation(return_series):
    u"""
    根据给定区间内收益率序列, 计算区间标准差.

    *Args:
        rrSeq(list): 区间内该基金产品的收益率序列(日期由近及远);

    *Returns:
        区间标准差.
    """
    try:
        return_series = slice_dropna(return_series)
        check_series(return_series, length_min=2)

        # return (sum([(rr - np.mean(value_series)) ** 2 for rr in value_series]) / (len(value_series) - 1)) ** .5
        return np.std(return_series, ddof=1)

    except Exception as e:
        return _err_string


# 2-2 区间年化标准差
def standard_deviation_a(return_series, period_num=52):
    u"""
    根据给定区间内收益率序列, 披露时间频度, 计算区间标准差.

    *Args:
        rrSeq(list): 区间内该基金产品的收益率序列(日期由近及远);
        period_num(int): 根据披露时间频度所得的一年内周期个数.包括52(周), 12(月), 250(日). 默认频度为52周;

    *Returns:
        区间年化标准差.
    """
    try:
        return_series = slice_dropna(return_series)
        check_series(return_series, length_min=3)

        return standard_deviation(return_series) * (period_num ** .5)

    except Exception as e:
        return _err_string


# 2-3 区间年化下行标准差
def downside_deviation_a(return_series, return_series_f, period_num=52):
    u"""
    根据给定区间内收益率序列, 一年期无风险国债的收益率序列, 披露时间频度, 计算区间年化下行标准差.

    *Args:
        rrSeq(list): 区间内该基金产品的收益率序列(日期由近及远);
        rrSeq_bm(list): 区间内市场指标的收益率序列(日期由近及远);
        period_num(int): 根据披露时间频度所得的一年内周期个数.包括52(周), 12(月), 250(日). 默认频度为52周;

    *Returns:
        区间年化下行标准差.
    """
    try:
        return_series, return_series_f = slice_dropna(return_series, return_series_f)
        check_series(return_series, return_series_f, length_min=3)

        # T = len(value_series)
        # return_series_f = return_series_f[:T]
        # print(T, len(return_series_f))
        # return period_num ** .5 * (sum([min(0, value_series[i] - return_series_f[i]) ** 2 for i in range(T)]) / (
        #     T - 1)) ** .5
        return_series, return_series_f = np.array(return_series), np.array(return_series_f)
        delta_series = return_series - return_series_f
        delta_series[delta_series > 0] = 0
        tmp = np.sum(delta_series ** 2)
        return (period_num * tmp / (len(delta_series) - 1)) ** .5

    except Exception as e:
        return _err_string


# 2-4 区间最大回撤
def max_drawdown(p_series, time_series=None):
    try:
        p_series = p_series[::-1]
        p_series = slice_dropna(p_series)
        check_series(p_series, length_min=2)
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
            return mdd, timenodes
            # return mdd, [timenodes[i] - timenodes[i - 1] if timenodes[i] is not None and timenodes[
            #                                                                                  i - 1] is not None else None
            #              for i in range(1, len(timenodes))]
        else:
            time_series = slice_dropna(time_series)
            time_series = time_series[::-1]
            assert len(time_series) == len(p_series)
            return mdd, [
                (time_series[timenodes[i]] - time_series[timenodes[i - 1]]) / 86400 if timenodes[i] is not None and
                                                                                       timenodes[
                                                                                           i - 1] is not None else None
                for i in range(1, len(timenodes))]

    except Exception as e:
        return _err_string, _err_string

# def max_retracement(self):
#     data = self.nav[self.nav.columns[0]].tolist()
#     mat = np.array([data] * len(data)).T / data - 1
#     mat = - np.tril(mat)
#     mdd = max([0, np.max(mat)])
#     if mdd == 0:
#         return 0, None, None
#     mdd_p, mdd_sp = np.argwhere(mat == mdd)[0]
#     mdd_date = self.nav.index[mdd_p]
#     formation_time = (mdd_date - self.nav.index[mdd_sp]).days
#     return mdd, mdd_date.strftime("%Y-%m-%d"), formation_time

# 2-6 区间贝塔系数
def beta(return_series, return_series_bm, return_series_f):
    u"""
    根据给定区间内收益率序列, 市场指标的收益率率序列, 一年期无风险国债的收益率序列, 计算区间贝塔系数.

    *Args:
        rrSeq(list): 区间内该基金产品的收益率序列(日期由近及远);
        rrSeq_bm(list): 区间内市场指标的收益率序列(日期由近及远);
        rrSeq_tbond(list): 区间内一年期无风险国债的收益率序列(日期由近及远);

    *Returns:
        区间贝塔系数.
    """
    try:
        return_series, return_series_bm, return_series_f = slice_dropna(return_series, return_series_bm,
                                                                        return_series_f)
        check_series(return_series, return_series_bm, return_series_f, length_min=3)

        return_series = np.array(return_series)
        return_series_bm = np.array(return_series_bm)
        return_series_f = np.array(return_series_f)
        T = len(return_series)
        delta_if = return_series - return_series_f
        delta_bf = return_series_bm - return_series_f
        return (T * sum(delta_if * delta_bf) - sum(delta_if) * sum(delta_bf)) / (
            T * sum(delta_bf ** 2) - sum(delta_bf) ** 2)

    except Exception as e:
        return _err_string


# 2-7 区间相关系数
def corr(return_series, return_series_bm):
    u"""
    根据给定区间内收益率序列, 市场指标的收益率率序列, 计算区间贝塔系数.

    *Args:
        rrSeq(list): 区间内该基金产品的收益率序列(日期由近及远);
        rrSeq_bm(list): 区间内市场指标的收益率序列(日期由近及远);

    *Returns:
        区间相关系数.
    """
    try:
        return_series, return_series_bm = slice_dropna(return_series, return_series_bm)
        check_series(return_series, return_series_bm, length_min=2)

        result = stats.pearsonr(return_series, return_series_bm)

        if isinstance(result[0], float) and np.isnan(result[0]) == False:
            return result
        else:
            return _err_string, _err_string

    except Exception as e:
        return _err_string, _err_string


def interval_corr2(rrSeq, rrSeq_bm, DOF=0):
    n = len(rrSeq)
    sum_x = sum(rrSeq)
    sum_y = sum(rrSeq_bm[:n])
    sum_xy = 0
    for i in range(n):
        sum_xy += rrSeq[i] * rrSeq_bm[i]

    sum_x2 = sum([rr ** 2 for rr in rrSeq])
    sum_y2 = sum([rr ** 2 for rr in rrSeq_bm[:n]])
    num = (n - DOF) * sum_xy - (sum_x * sum_y)
    den = np.sqrt(((n - DOF) * sum_x2 - sum_x ** 2) * ((n - DOF) * sum_y2 -
                                                       sum_y ** 2))
    return num / den


# 2-8 区间非系统风险
def unsystematic_risk(return_series, return_series_bm, return_series_f):
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
        return_series, return_series_bm, return_series_f = slice_dropna(return_series, return_series_bm,
                                                                        return_series_f)
        check_series(return_series, return_series_bm, return_series_f, length_min=3)

        return_series = np.array(return_series)
        return_series_bm = np.array(return_series_bm)
        return_series_f = np.array(return_series_f)
        T = len(return_series)
        return_series_if = return_series - return_series_f
        return_series_bmf = return_series_bm - return_series_f

        beta_ = beta(return_series, return_series_bm, return_series_f)
        alpha = np.mean(return_series_if) - beta_ * np.mean(return_series_bmf)
        return np.sqrt((sum(return_series_if ** 2) - alpha * sum(return_series_if) - beta_ * sum(
            return_series_if * return_series_bmf)) / (T - 2))

    except Exception as e:
        return _err_string


# 2-9 区间年化跟踪误差
def tracking_error_a(return_series, return_series_bm, period_num=52):
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
        return_series, return_series_bm = slice_dropna(return_series, return_series_bm)
        check_series(return_series, return_series_bm, length_min=3)

        return_series = np.array(return_series)
        return_series_bm = np.array(return_series_bm)
        return np.std(return_series - return_series_bm, ddof=1) * period_num ** .5

    except Exception as e:
        return _err_string


def value_at_risk(return_series, M=1000, alpha=.05):
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
        return_series = slice_dropna(return_series)
        check_series(return_series, length_min=2)

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


# 2-11 区间偏度
def skewness(return_series):
    '''

    Args:
        return_series:

    Returns:

    '''
    try:
        return_series = slice_dropna(return_series)
        check_series(return_series, length_min=2)

        return_series_std = standard_deviation(return_series)
        if return_series_std != 0:
            return_series_mean = np.mean(return_series)
            return np.sum((return_series - return_series_mean) ** 3) / (
                (len(return_series) - 1) * return_series_std ** 3)  # 修正效率问题
        else:
            return _err_string

    except Exception as e:
        return _err_string


# 2-12 区间峰度
def kurtosis(return_series):
    u"""
    根据给定区间内收益率序列, 计算区间峰度.

    *Args:
        rrSeq(list): 区间内该基金产品的收益率序列(日期由近及远);

    *Returns:
        区间峰度.
    """
    try:
        return_series = slice_dropna(return_series)
        check_series(return_series, length_min=2)

        return_series_std = standard_deviation(return_series)
        return_series_mean = np.mean(return_series)
        if return_series_std != 0:
            return sum((return_series - return_series_mean) ** 4) / ((len(return_series) - 1) * return_series_std ** 4)
        else:
            return _err_string

    except Exception as e:
        return _err_string


# 2-13 区间最长连续下跌幅度(区间最大跌幅)
def range_continuous_fall(return_series):
    u"""
    根据给定区间内收益率序列, 计算区间最长连续下跌幅度.

    *Args:
        rrSeq(list): 区间内该基金产品的收益率序列(日期由近及远);
               
    *Returns:
        区间最长连续下跌幅度.
    """
    try:
        check_series(return_series, length_min=2)

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


# 2-14 区间非正收益月数
def periods_npositive_return(return_series, date_series=None):
    u"""
    根据给定区间内的时间序列, 收益率序列, 计算区间非正收益月份数.

    *Args:
        dateSeq(list): 区间内包含的时间序列(日期由近及远);
        rrSeq(list): 区间内该基金产品的收益率序列(日期由近及远);
        
    *Returns:
        区间非正收益月份数;
    """
    try:
        if date_series is None:
            return_series = slice_dropna(return_series)
            check_series(return_series, length_min=1)

            idx = (np.array(return_series) <= 0)
            return sum(idx)
        else:
            return_series, date_series = slice_dropna(return_series, date_series)
            check_series(return_series, date_series, length_min=1)

            idx = (np.array(return_series) <= 0)
            return sum(idx), np.array(date_series[:-1])[idx]

    except Exception as e:
        return _err_string


# 3-1 区间年化夏普比率
def sharpe_a(return_series, return_series_f, period_num=52):
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
    try:
        return_series, return_series_f = slice_dropna(return_series, return_series_f)
        check_series(return_series, return_series_f, length_min=3)
        return_series_std_a = standard_deviation_a(return_series, period_num)
        if return_series_std_a == 0:
            return _err_string
        return excess_return_a(return_series, return_series_f, period_num) / return_series_std_a
        # return value_series, return_series_f
    except Exception as e:
        return _err_string


# 3-2 区间年化卡玛比率
def calmar_a(swanav_series, return_series_f, period_num=52):
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
    r_s = gen_return_series(swanav_series)
    try:
        check_series(r_s, return_series_f, length_min=3)

        er_a = excess_return_a(r_s, return_series_f, period_num)
        mdd = max_drawdown(swanav_series)[0]

        if mdd == 0:
            return _err_string

        return er_a / mdd

    except Exception as e:
        return _err_string


# 3-3 区间年化索提诺比率
def sortino_a(return_series, return_series_f, period_num=52):
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
    try:
        return_series, return_series_f = slice_dropna(return_series, return_series_f, all=False)
        check_series(return_series, return_series_f, length_min=3)

        er_a = excess_return_a(return_series, return_series_f, period_num)
        dd_a = downside_deviation_a(return_series, return_series_f, period_num)

        if dd_a == 0:
            return _err_string

        return er_a / dd_a

    except Exception as e:
        return _err_string


# 3-4 区间年化特雷诺比率
def treynor_a(return_series, return_series_bm, return_series_f, period_num=52):
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
        return_series, return_series_bm, return_series_f = slice_dropna(return_series, return_series_bm,
                                                                        return_series_f)
        check_series(return_series, return_series_bm, return_series_f, length_min=3)
        # irr_a = return_a(value_series, period_num, interest_type)
        # irr_bm_a = return_a(return_series_bm, period_num, interest_type)

        ierr = excess_return_a(return_series, return_series_f, period_num)
        beta_ = beta(return_series, return_series_bm, return_series_f)
        if beta_ == 0:
            return _err_string
        return ierr / beta_

    except Exception as e:
        return _err_string


# 3-5 区间年化信息比率
def info_a(return_series, return_series_bm, period_num=52):
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
        return_series, return_series_bm = slice_dropna(return_series, return_series_bm, all=True)
        check_series(return_series, return_series_bm, length_min=3)

        return_series = np.array(return_series)
        return_series_bm = np.array(return_series_bm)
        r_a = return_a(return_series, period_num)
        r_a_bm = annualized_return(return_series_bm, period_num)
        er_a = excess_return_a(return_series, return_series_bm, period_num)
        if tracking_error_a == 0:
            return _err_string
        return er_a / tracking_error_a(return_series, return_series_bm, period_num)

    except Exception as e:
        return _err_string


# 3-6 区间年化詹森指数
def jensen_a(return_series, return_series_bm, return_series_f, period_num=52):
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
        return_series, return_series_bm, return_series_f = slice_dropna(return_series, return_series_bm,
                                                                        return_series_f)
        check_series(return_series, return_series_bm, return_series_f, length_min=3)

        return_series = np.array(return_series)
        return_series_bm = np.array(return_series_bm)
        return_series_f = np.array(return_series_f)

        delta_if = return_series - return_series_f
        delta_bf = return_series_bm - return_series_f

        beta_ = beta(return_series, return_series_bm, return_series_f)
        alpha = [np.mean(delta_if) - beta_ * np.mean(delta_bf)]
        return annualized_return(alpha, period_num)

    except Exception as e:
        return _err_string


##3-7 区间风险价值调整比
def ERVaR(return_series, return_series_f, period_num=52, M=1000, alpha=.05):
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
        return_series, return_series_f = slice_dropna(return_series, return_series_f)
        check_series(return_series, return_series_f, length_min=3)

        VaR_alpha = value_at_risk(return_series, M, alpha)
        return excess_return_a(return_series, return_series_f, period_num) / max(0.0001, VaR_alpha)

    except Exception as e:
        return _err_string


# 4-1 区间选时能力
def competency_timing(return_series, return_series_bm, return_series_f):
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
        return_series, return_series_bm, return_series_f = slice_dropna(return_series, return_series_bm,
                                                                        return_series_f)
        check_series(return_series, return_series_bm, return_series_f, length_min=4)

        y = np.array(return_series) - np.array(return_series_f)
        x = np.array(return_series_bm) - np.array(return_series_f)
        lsq = leastsq(residuals_competency, [1, 1, 1], args=(x, y))
        return lsq[0][2]

    except Exception as e:
        return _err_string


# 4-2 区间选股能力
def competency_stock(return_series, return_series_bm, return_series_f):
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
        return_series, return_series_bm, return_series_f = slice_dropna(return_series, return_series_bm,
                                                                        return_series_f)
        check_series(return_series, return_series_bm, return_series_f, length_min=4)

        y = np.array(return_series) - np.array(return_series_f)
        x = np.array(return_series_bm) - np.array(return_series_f)
        lsq = leastsq(residuals_competency, [1, 1, 1], args=(x, y))
        return lsq[0][0]

    except Exception as e:
        return _err_string


# 4-3 区间超额收益率可持续性
def persistence_er(return_series, return_series_bm):
    u"""
    根据给定区间内收益率序列, 市场指标的收益率序列, 计算区间超额收率可持续性.

    *Args:
        rrSeq(list): 区间内该基金产品的收益率序列(日期由近及远);
        rrSeq_bm(list): 区间内市场指标的收益率序列(日期由近及远);
               
    *Returns:
        区间超额收益率可持续性.
    """
    try:
        return_series, return_series_bm = slice_dropna(return_series, return_series_bm)
        check_series(return_series, return_series_bm, length_min=2)

        T = len(return_series)
        er_series = np.array(return_series) - np.array(return_series_bm)
        er_series = er_series[::-1]
        err_avg = np.mean(er_series)
        rho = sum([(er_series[i] - err_avg) * (er_series[i - 1] - err_avg) for i in range(1, T)]) / sum(
            [(er_series[i] - err_avg) ** 2 for i in range(T)])
        return rho

    except Exception as e:
        return _err_string


# 4-4 区间最长连续上涨周期数
def periods_continuous_rise(return_series):
    try:
        check_series(return_series, length_min=2)

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


# 4-5 区间最长连续下跌周期数
def periods_continuous_fall(return_series):
    """
    根据给定区间内收益率序列, 计算区间最长连续下跌月数.

    *Args:
        rrSeq(list): 区间内该基金产品的收益率序列(日期由近及远);
               
    *Returns:
        区间最长连续下跌月数.
    """
    try:
        check_series(return_series, length_min=2)

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


# Intermediate Variables
# 基金收益率
def gen_return_series(price_series):
    """
    根据给定价格序列, 计算收益率序列.

    *Args:
        price_series(list): 价格序列(日期由近及远, 价格序列应包含上个区间末的最后一次披露的价格, 以计算本区间第一个收益率);
               
    *Returns:
        收益率序列(日期由近及远).
    """
    try:
        check_series(price_series, length_min=2)

        return [price_series[i] / price_series[i + 1] - 1 if (
            (price_series[i] is not None) and (price_series[i + 1] is not None)
        ) else None for i in
                range(len(price_series) - 1)]

    except Exception as e:
        return []


# 年化收益率 wind
def annualized_return(return_series, period_num=52, flavor="mean"):
    """
    根据给定收益率序列, 计算年化收益率.

    *Args:
        price_series(list): 价格序列(日期由近及远);
               
    *Returns:
        收益率序列(日期由近及远).
    """
    try:
        if flavor == "mean":
            return_series = slice_dropna(return_series)
            check_series(return_series, length_min=3)
            return (1 + np.mean(return_series)) ** period_num - 1
        elif flavor == "accumulative":
            pass

    except Exception as e:
        return _err_string


# 斜率
def genSlope(seq):
    return [(seq[i] - seq[i + 1]) / abs(seq[i + 1]) for i in range(len(seq) - 1)]


# 最小二乘估计_区间选时&选股能力
def func_competency(x, params):
    alpha, beta1, beta2 = params
    return alpha + beta1 * x + beta2 * (x ** 2)


def residuals_competency(params, xdata, ydata):
    return ydata - func_competency(xdata, params)

# import numpy as np
#
# np.random.seed(2)
# np.random.randint(4, size=(10, 4))
