import numpy as np
from utils.algorithm import fund_indicator_old as fi


# support function
def genScaleSeq(swanavListSeq, quotientListSeq):
    swanavListSeq = np.array(swanavListSeq)
    quotientListSeq = np.array(quotientListSeq)
    return swanavListSeq * quotientListSeq


def genTotalAssetScaleSeq(swanavListSeq, quotientListSeq):
    swanavListSeq = np.array(swanavListSeq)
    quotientListSeq = np.array(quotientListSeq)
    if swanavListSeq.ndim == 2:
        return np.sum(swanavListSeq * quotientListSeq, axis=1)
    elif swanavListSeq.ndim == 1:
        return swanavListSeq * quotientListSeq


def comprehensive_price(pls, sls=None):
    prices_series = np.array(pls)
    prices_series = np.array(list(map(lambda x: x[~np.isnan(x)], prices_series)))
    prices_series_weighted = np.array(list(map(np.mean, prices_series)))
    return prices_series_weighted


# def weighted_


# 区间综合
def comprehensive_return(prices_series, scaleListSeq=None, sequence="latest"):
    prices_series = np.array(prices_series)
    returns_series = prices_series[:-1] / prices_series[1:] - 1
    returns_series = np.array(list(map(lambda x: x[~np.isnan(x)], returns_series)))

    if scaleListSeq is None:
        return_series_weighted = np.array(list(map(np.mean, returns_series)))

    else:
        scaleListSeq = np.array(scaleListSeq)[1:]
        return_series_weighted = np.sum(returns_series * scaleListSeq, axis=scaleListSeq.ndim - 1) / np.sum(
            scaleListSeq, axis=scaleListSeq.ndim - 1)

    if sequence == "latest":
        return return_series_weighted[0].tolist()
    elif sequence == "all":
        return return_series_weighted.tolist()
    elif type(sequence) == int:
        return return_series_weighted[sequence].tolist()


# 1_1_1 区间累计收益率
def interval_return(prices_series, scales_series=None):
    prices_series = np.array(prices_series)
    i_r = prices_series[0] / prices_series[-1] - 1
    i_r = i_r[~np.isnan(i_r)]
    return i_r.mean()


# 1_1_2 区间年化收益率
def return_a(return_series_weighted, period_num=12, interest_type="compound"):
    return fi.annualized_return(return_series_weighted, period_num, interest_type)


# 1_1_3 区间超额年化收益率
def excess_return_a(return_series_weighted, return_series_bm, period_num=12):
    return fi.excess_return_a(return_series_weighted, return_series_bm)


# 1_2_1 区间年化夏普比率
def sharpe_a(return_series_weighted, return_series_f, period_num=12):
    return fi.sharpe_a(return_series_weighted, return_series_f, period_num)


# 1_2_2 年化卡玛比率
def calmar_a(prices_series_weighted, return_series_f, period_num=12, interest_type="compound"):  # still developing
    return fi.calmar_a(prices_series_weighted, return_series_f, period_num=12, interest_type="compound")


# 1_2_3 区间年化索提诺比率
def sortino_a(return_series_weighted, return_series_f, period_num=12, interest_type="compound"):
    return fi.sortino_a(return_series_weighted, return_series_f, period_num, interest_type)


# 1_2_4 区间年化特雷诺比率
def treynor_a(return_series_weighted, return_series_f, period_num=12, interest_type="compound"):
    return fi.treynor_a(return_series_weighted, return_series_f, period_num, interest_type)


# 1_2_5 区间年化信息比率
def info_a(return_series_weighted, return_series_bm, period_num=12, interest_type="compound"):
    return fi.info_a(return_series_weighted, return_series_bm, period_num, interest_type)


# 1_2_6 区间年化詹森指数
def jensen_a(return_series_weighted, return_series_bm, return_series_f, period_num=12, interest_type="compound"):
    return fi.jensen_a(return_series_weighted, return_series_bm, return_series_f, period_num, interest_type)


# 2_1_1 区间年化标准差
def standard_deviation_a(return_series_weighted, period_num=12):
    return fi.standard_deviation_a(return_series_weighted, period_num)


# 2_1_2 区间年化下行标准差
def downside_deviation_a(return_series_weighted, return_series_f, period_num=12):
    return fi.downside_deviation_a(return_series_weighted, return_series_f, period_num=12)


# 2_1_3 区间贝塔系数
def beta(return_series_weighted, return_series_bm, return_series_f):
    return fi.beta(return_series_weighted, return_series_bm, return_series_f)


# 2_1_4 区间内部策略相关系数
def corr(return_series_weighted, return_series_bm):
    return fi.corr(return_series_weighted, return_series_bm)


# 2_2_1 区间最大回撤
def max_drawdown(prices_series_weighted):
    return fi.max_drawdown(prices_series_weighted)


# 2_2_2 区间最大回撤的形成期
def interval_spanOfMaxRetracement(dateSeq, marketIndexSeq):  # still developing
    return fi.interval_spanOfMaxtracement(dateSeq, marketIndexSeq)


# 2_2_3 区间风险价值
def value_at_risk(return_series_weighted, M=100, alpha=0.05):
    return fi.value_at_risk(return_series_weighted, M, alpha)


# 2_2_4 区间结构化产品所占比重
def interval_averageScaleOfStructuralAsset(assetScaleSeq_s, totalAssetScaleSeq):
    u"""
    根据给定区间内的结构化产品规模序列, 所有产品的规模序列, 计算区间结构化产品平均比重.

    *Args:
        assetScaleSeq_s(list): 区间内结构化产品的总规模序列(日期由近及远);
        totalAssetScaleSeq(list): 区间内所有产品的总规模序列(日期由近及远);
                       
    *Returns:
        区间结构化产品平均比重.
    """
    return interval_averageAssetScale(assetScaleSeq_s) / interval_averageAssetScale(totalAssetScaleSeq)


# 3_1 区间择时能力
def competency_timing(return_series_weighted, return_series_bm, return_series_f):
    return fi.competency_timing(return_series_weighted, return_series_bm, return_series_f)


def competency_stock(return_series_weighted, return_series_bm, return_series_f):
    return fi.competency_stock(return_series_weighted, return_series_bm, return_series_f)


def odds(return_series_weighted, return_series_bm, period_num=12, interest_type="compound"):
    return fi.odds(return_series_weighted, return_series_bm, period_num, interest_type)


def sustainability_excess_return(return_series_weighted, return_series_bm):
    return fi.persistence_er(return_series_weighted, return_series_bm)


# 4_1_1 投顾管理运营时间

# 4_2_2 管理规模_区间平均资产规模
def interval_averageAssetScale(totalAssetScaleSeq):
    u"""
    根据给定区间内的产品规模序列, 计算区间平均资产规模.

    *Args:
        totalAssetScaleSeq(list): 区间内所有产品的总规模序列(日期由近及远);
                       
    *Returns:
        区间平均资产规模.
    """
    return np.mean(totalAssetScaleSeq)


# 4_2_3 管理规模_区间资产规模变动率
def interval_changeRateOfAssetScale(totalAssetScaleSeq):
    u"""
    根据给定区间内的产品规模序列, 计算区间资产规模变动率.

    *Args:
        totalAssetScaleSeq(list): 区间内所有产品的总规模序列(日期由近及远);
                       
    *Returns:
        区间资产规模变动率.
    """
    return totalAssetScaleSeq[0] / totalAssetScaleSeq[-1] - 1


# 4_3_4 运营收入_业绩浮动报酬
def interval_CRatio(scaleSeq, feeListSeq, yieldList):  # 提取日期(季度, 半年)对应的数据
    u"""
    根据给定区间内的产品规模序列, 计算区间业绩浮动报酬.

    *Args:
        assetScaleSeq(list): 区间内各产品的规模序列(日期由近及远);
        feeListSeq(list): 区间内各产品的管理费率序列(日期由近及远);
        yieldList(list): 区间内各产品的保障收益率;
                       
    *Returns:
        区间业绩浮动报酬.
    """
    scaleSeq = np.array(scaleSeq)
    feeListSeq = np.array(feeListSeq)
    yieldList = np.array(yieldList)

    delta = scaleSeq[:-1] - scaleSeq[1:]
    range = delta / scaleSeq[1:]
    isGreater = [range.T[i] > yieldList[i] for i in range(len(range.T))]
    reward = delta * feeListSeq
    reward_Seq = [reward.T[i][isGreater[i]] for i in range(len(isGreater))]

    return sum([sum(x) for x in reward_Seq])


# 4_3_5 运营收入_管理费收入
def interval_revenueFromManagementFee(assetScaleSeq, feeListSeq):
    u"""
    根据给定区间内各产品的规模序列, 各产品的管理费率, 计算区间管理费收入.

    *Args:
        totalAssetScaleSeq(list): 区间内各产品的总规模序列(日期由近及远);
        feeListSeq(list): 区间内各产品的管理费率序列(日期由近及远);
                       
    *Returns:
        区间管理费收入.
    """
    feeListSeq = np.array(feeListSeq)
    return np.sum(assetScaleSeq * feeListSeq)


# 4_4_6 公司稳定性_高管变动率
def interval_changeRateOfPerson(currentPeople, lastPeople):
    u"""
    根据给定当前高管人员, 上个区间末高管人员, 计算区间高管变动率.

    *Args:
        currentPeople(list): 当前高管人员;
        lastPeople(list): 上个区间末高管人员;
                       
    *Returns:
        区间高管变动率.
    """
    currentPeople = set(currentPeople)
    lastPeople = set(lastPeople)
    change = lastPeople.union(currentPeople) - lastPeople.intersection(currentPeople)
    return len(change) / len(lastPeople)


# 4_4_7 公司稳定性_产品基金经理变动率
# 公式同上

# 5 指数合成
def interval_marketIndex_PE(swanavListSeq, quotientListSeq):
    u"""
    根据给定区间内各产品的净值规模序列, 各产品的份额序列, 计算区间私募市场指数.

    *Args:
        swanavListSeq(list): 区间内各产品的净值序列(日期由近及远);
        quotientListSeq(list): 区间内各产品的份额序列(日期由近及远);
                       
    *Returns:
        区间内各期私募市场加权平均收益率;
        区间内各期私募市场指数;
    """
    swanavListSeq = np.array(swanavListSeq)
    scaleListSeq = genScaleSeq(swanavListSeq, quotientListSeq)
    rrListSeq = swanavListSeq[:-1] / swanavListSeq[1:] - 1
    rrSeq_weighted = np.sum(scaleListSeq[1:] * rrListSeq, axis=1) / np.sum(scaleListSeq[1:], axis=1) + 1

    return rrSeq_weighted, np.cumprod(rrSeq_weighted[::-1])[::-1] * 1000


def marketIndex_Current(swanavList_latest, scaleList_last, marketIndex_last):
    """

    """
    swanavList_latest = np.array(swanavList_latest)
    scaleList_last = np.array(scaleList_last)
    rrList = swanavList_latest[0] / swanavList_latest[-1] - 1
    rr_weighted = np.sum(rrList * scaleList_last, axis=scaleList_last.ndim - 1) / np.sum(scaleList_last,
                                                                                         axis=scaleList_last.ndim - 1)

    return (1 + rr_weighted) * marketIndex_last
