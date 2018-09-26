import numpy as np


def truncate_by_quarter(series) -> None:
    """
       对收益序列作四分位数截尾

    Args:
        series: pd.DataFrame{
            index: "statistic_date"<datetime.datetime>,
            column: "rs"<float>
        }

    Returns:
        None

    """

    q1, q3 = [series.quantile(q) for q in (0.25, 0.75)]
    lower_bound, upper_bound = q1 - 1.5 * (q3 - q1), q3 + 1.5 * (q3 - q1)
    series[series >= upper_bound] = upper_bound
    series[series <= lower_bound] = lower_bound


def trunc_quarter(a):

    #必须满足为list，且长度大于0
    if (type(a) == np.ndarray or type(a) == list) and len(a)>0:
        q1, q3 = np.nanpercentile(a, (25, 75))
        lb, ub = q1 - 1.5 * (q3 - q1), q3 + 1.5 * (q3 - q1)
        a[a >= ub] = ub
        a[a <= lb] = lb
    return a

