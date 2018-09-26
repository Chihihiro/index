import calendar as cld
import datetime as dt
from utils.timeutils.const import bday_chn
from dateutil.relativedelta import relativedelta
import pandas as pd
import time as t
import sys

_interval_args = {
    "m": True,
    "q": True,
    "a": True
}


# Standard Time Series

def timeseries_std(date_start, interval, periods_y=52, extend=0, **kwargs):
    """

    Args:
        date_start: datetime.date, datetime.datetime or float

        interval: int or str
            Interval type chosen, for str interval, optional {"w", "m", "q", "a"};
        periods_y: integer, optional {4, 12, 52}, default 52
            Frequency of period in a year, 12 refers to monthly, and 52 refers to weekly;
        extend: int, default 0
            The extra period number needed besides periods in interval(usually for calculation use);
        **kwargs:
            use_lastday: bool
            keep_firstday: bool
            weekday: bool
    Returns:

    """
    tmtp_s, tmstp_s = universal_time(date_start)
    date_end = date_infimum(interval, date_start)
    periods_num = periods_in_interval(date_start, date_end, periods_y)

    if periods_y == 52:
        if interval != "w":
            res = [tmstp_s - i * 604800 for i in range(
                periods_num + 1 + extend)]  # 604800 is the seconds of a week, because timestamp use second as unit
        else:
            first_weekday = (
                dt.datetime(tmtp_s.tm_year, tmtp_s.tm_mon, tmtp_s.tm_mday, 23, 59, 59, 999999) - dt.timedelta(
                    tmtp_s.tm_wday + 1)
            ).timestamp()
            tmp = [first_weekday - i * 604800 for i in range(1 + extend)]
            tmp.insert(0, tmstp_s)
            res = tmp
        return res

    elif periods_y == 12:
        dates_Ym = [
            (tmtp_s.tm_year - (i + 12 - tmtp_s.tm_mon) // 12, 12 - (i + 12 - tmtp_s.tm_mon) % 12)
            for i in range(periods_num + 1 + extend)
            ]
        month_range = [
            cld.monthrange(tmtp_s.tm_year - (i + 12 - tmtp_s.tm_mon) // 12, 12 - (i + 12 - tmtp_s.tm_mon) % 12)[1]
            for i in range(periods_num + 1 + extend)
            ]

        if kwargs.get("use_lastday", _interval_args.get(interval, False)):
            res = [dt.datetime(Y, m, d, 23, 59, 59, 999999).timestamp() for (Y, m), d in zip(dates_Ym, month_range)]
            if kwargs.get("keep_firstday", _interval_args.get(interval, False)):
                res[0] = tmstp_s
        else:
            res = [
                dt.datetime(Y, m, min(d, tmtp_s.tm_mday), tmtp_s.tm_hour, tmtp_s.tm_min, tmtp_s.tm_sec).timestamp()
                for (Y, m), d in zip(dates_Ym, month_range)
                ]

        return res

    elif periods_y == 4:
        quarters = tuple([10, 7, 4, 1])
        for quarter in quarters:
            if tmtp_s.tm_mon >= quarter:
                quarter_belong = quarter
                break

        if kwargs.get("use_lastday", False):
            first_day = dt.datetime(tmtp_s.tm_year, quarter_belong + 2,
                                    cld.monthrange(tmtp_s.tm_year, quarter_belong + 2)[1], 23, 59, 59, 999999)
            res = [(first_day - relativedelta(months=3 * i)).timestamp() for i in range(periods_num + 1)]
            if kwargs.get("keep_firstday", _interval_args.get(interval, False)):
                res[0] = tmstp_s
        else:
            first_day = dt.datetime(tmtp_s.tm_year, tmtp_s.tm_mon, tmtp_s.tm_mday)
            res = [(first_day - relativedelta(months=3 * i)).timestamp() for i in range(periods_num + 1)]
        return res

    elif periods_y == 250:
        use_weekday = kwargs.get("weekday", False)
        if use_weekday:
            res = [tmstp_s - 86400 * i for i in range(periods_num + 1 + extend)
                   if ((tmtp_s.tm_wday + 7) - (i % 7)) % 7 != 5 and ((tmtp_s.tm_wday + 7) - (i % 7)) % 7 != 6]
        else:
            res = [tmstp_s - 86400 * i for i in range(periods_num + 1 + extend)]
        return res


def timeseries_std_se(date_start, date_end, periods_y=52, extend=0, **kwargs):
    tmtp_s, tmstp_s = universal_time(date_start)
    tmtp_e, tmstp_e = universal_time(date_end)
    redundance = 4
    if periods_y in {52, 12, 4, 250}:
        periods = periods_in_interval(tmstp_s, tmstp_e, 12)
    # elif periods_y in {250}:
    #     periods = periods_in_interval(tmstp_s, tmstp_e, 12)

    if periods_y == 52:
        t_std_all = timeseries_std(tmstp_s, interval=periods, periods_y=52, extend=redundance)
    elif periods_y == 12:
        t_std_all = timeseries_std(tmstp_s, interval=periods, periods_y=12, extend=redundance, use_lastday=True,
                                   keep_firstday=True)
    elif periods_y == 4:
        t_std_all = timeseries_std(tmstp_s, interval=periods, periods_y=4, extend=redundance, use_lastday=True,
                                   keep_firstday=True)
    elif periods_y == 250:
        t_std_all = timeseries_std(tmstp_s, interval=periods, periods_y=250, extend=redundance + 31, **kwargs)

    t_std_all = t_std_all[:len([x for x in t_std_all if x >= tmstp_e]) + 1 + extend]

    return t_std_all


# Time Matching

# Support Function

def universal_time(tm):
    """
        Check the type of `time`, and return a timetuple(time.struct_time), timestamp(float).
    Notice that if the `time` is an instancce of int or float, then the timestamp will be
    equivalent to the `time`.

    Args:
        tm: datetime.datetime, datetime.date, int or float
            Time to transform;

    Returns:
        `time` in timetuple and timestamp format.
    """

    if isinstance(tm, dt.datetime):
        if sys.version_info.major == 3:
            return tm.timetuple(), tm.timestamp()
        elif sys.version_info.major == 2:
            return tm.timetuple(), t.mktime(tm.timetuple())


    elif isinstance(tm, dt.date):
        ttp = tm.timetuple()
        return ttp, t.mktime(ttp)

    elif isinstance(tm, (float, int)):
        ttp = dt.datetime.fromtimestamp(tm).timetuple()
        return ttp, tm


def periods_in_interval(date_start, date_end, periods_y=52):
    """
        Return periods included in the interval which begins from `date_start` and ends at `date_end`.

    Args:
        date_start: datetime.date or datetime.datetime
            The beginning date of the interval, usually considered as the calculate date;
        date_end: date or datetime
            The end date of the interval;
        periods_y: integer, optional {12, 52}, default 52
            Frequency of period in a year, 12 refers to monthly, and 52 refers to weekly;

    Returns:
        Number of periods included in the interval.
    """

    tmtp_s, tmstp_s = universal_time(date_start)
    tmtp_e, tmstp_e = universal_time(date_end)

    if periods_y == 52:
        delta_day = (tmstp_s - tmstp_e) / 86400
        n_integer = delta_day // 7
        n_decimal = delta_day % 7
        delta_weeks = int(n_integer + int(bool(n_decimal)))
        return delta_weeks

    elif periods_y == 12:
        delta_months = (tmtp_s.tm_year * 12 + tmtp_s.tm_mon) - (tmtp_e.tm_year * 12 + tmtp_e.tm_mon)
        return delta_months

    elif periods_y == 4:
        delta_month = (tmtp_s.tm_year * 12 + tmtp_s.tm_mon) - (tmtp_e.tm_year * 12 + tmtp_e.tm_mon)
        delta_quarter = delta_month // 3 + 1
        return delta_quarter

    elif periods_y == 250:
        delta_days = int((tmstp_s - tmstp_e) // 86400) + 1
        return delta_days


def date_infimum(interval, date_start=dt.datetime.today()):  # UNDER DEVELOPING
    """
    Generate the last moment of the interval from `date_start`, duration depends on the given `interval`

    Args:
        interval: int or str
            Interval type chosen, for str interval, optional {"w", "m", "q", "a"}
        date_start: datetime.date or datetime.datetime
            The beginning date of the interval, usually considered as the calculate date;

    Returns:

    """
    date_start = dt.datetime.fromtimestamp(universal_time(date_start)[1])
    if type(interval) is not str:
        total_month = date_start.year * 12 + date_start.month - interval
        y = total_month // 12
        m = total_month % 12
        if m == 0:
            m += 12
            y -= 1
        if cld.monthrange(y, m)[1] < date_start.day:
            date_inf = dt.datetime(y, m, cld.monthrange(y, m)[1], date_start.hour, date_start.minute, date_start.second,
                                   date_start.microsecond)
        else:
            date_inf = dt.datetime(y, m, date_start.day, date_start.hour, date_start.minute, date_start.second,
                                   date_start.microsecond)

    elif type(interval) is str:
        if interval == "d":
            date_inf = dt.datetime(date_start.year, date_start.month, date_start.day) - dt.timedelta(0, 0, 1)
        elif interval == "w":
            date_inf = dt.datetime(date_start.year, date_start.month, date_start.day) - dt.timedelta(
                date_start.weekday()) - dt.timedelta(0, 0, 1)
        elif interval == "m":
            date_inf = dt.datetime(date_start.year, date_start.month, 1) - dt.timedelta(0, 0, 1)
        elif interval == "q":
            s1 = [1, 2, 3]
            s2 = [4, 5, 6]
            s3 = [7, 8, 9]
            s4 = [10, 11, 12]
            if date_start.month in s1:
                date_inf = dt.datetime(date_start.year, s1[0], 1) - dt.timedelta(0, 0, 1)
            elif date_start.month in s2:
                date_inf = dt.datetime(date_start.year, s2[0], 1) - dt.timedelta(0, 0, 1)
            elif date_start.month in s3:
                date_inf = dt.datetime(date_start.year, s3[0], 1) - dt.timedelta(0, 0, 1)
            elif date_start.month in s4:
                date_inf = dt.datetime(date_start.year, s4[0], 1) - dt.timedelta(0, 0, 1)
        elif interval == "a":
            date_inf = dt.datetime(date_start.year, 1, 1) - dt.timedelta(0, 0, 1)
    return date_inf


def resample(df, freq, use_last=True, use_bday=True, **kwargs):
    """
        Dummy of pandas.DataFrame.resample method.
    Args:
        df_cp:
        freq:
        **kwargs:
            weekday
            keepfirst: bool, or datetime.datetime
    Returns:

    """

    keepfirst = kwargs.get("keepfirst", {"d": False, "w": False, "m": True, "q": True}.get(freq))
    df_cp = df.copy(False)

    if not isinstance(df_cp.index, (pd.DatetimeIndex, pd.TimedeltaIndex, pd.PeriodIndex)):
        df_cp.index = pd.DatetimeIndex(df_cp.index)

    if freq == "w":
        fdate = kwargs.get("fdate")
        weekdays_map = {0: "W-MON", 1: "W-TUE", 2: "W-WED", 3: "W-THU", 4: "W-FRI", 5: "W-SAT", 6: "W-SUN"}
        weekday = fdate.weekday() if kwargs.get("weekday") is None else kwargs.get("weekday")
        result = df_cp.resample(weekdays_map[weekday], closed="right", label="right")

    elif freq == "d":
        if use_bday:
            result = df_cp.resample(bday_chn, closed="left", label="left")
        else:
            result = df_cp.resample("D", closed="left", label="left")

    elif freq == "m":
        result = df_cp.resample(use_bday * "B" + "M", closed="right", label="right")

    elif freq == "q":
        result = df_cp.resample(use_bday * "B" + "Q", closed="right", label="right")

    if use_last:
        result = result.last()
        if type(keepfirst) is bool:
            if keepfirst:
                result.index = pd.DatetimeIndex([*result.index[:-1], df_cp.index[-1]])
        if type(keepfirst) is (dt.date, dt.datetime):
                result.index = pd.DatetimeIndex([*result.index[:-1], keepfirst])

    return result


def date_range(date_start, date_end, freq, reverse=False, ttype="date"):
    if freq == "w":
        weekday = date_start.weekday()
        weekdays_map = {0: "W-MON", 1: "W-TUE", 2: "W-WED", 3: "W-THU", 4: "W-FRI", 5: "W-SAT", 6: "W-SUN"}
        result = pd.date_range(date_end, date_start, freq=weekdays_map[weekday])

    elif freq == "d":
        result = pd.date_range(date_end, date_start, freq="B")

    elif freq == "m":
        result = pd.date_range(date_end, date_start, freq="BM")

    elif freq == "q":
        result = pd.date_range(date_end, date_start, freq="BQ")

    if reverse:
        result = result[::-1]

    if ttype == "date":
        result = [tmstmp.date() for tmstmp in result]

    return result


# To be DEPRECATED in the future
def tr(seq, ttype="datetime"):
    if ttype == "datetime":
        return [dt.datetime.fromtimestamp(x) if x is not None else None for x in seq]  # Only for debugging
    elif ttype == "date":
        return [dt.date.fromtimestamp(x) if x is not None else None for x in seq]
    elif ttype == "timestamp":
        return [t.mktime(x.timetuple()) if x is not None else None for x in seq]
    elif ttype == "str":
        return [x.strftime("%Y%m%d") for x in seq]


def outer_match4index_f7(ts_real, ts_std, drop_none=True):
    """
    (,]
    """
    idx_matched = {}
    ts_matched = []

    current_position = 0
    for i in range(len(ts_std)):
        is_missing_i = True
        for j in range(current_position, len(ts_real)):

            if is_missing_i and current_position < len(ts_real):
                if (ts_real[j] > ts_std[i] - 604800 and ts_real[j] <= ts_std[i]):
                    current_position = j
                    ts_matched.append(ts_real[j])
                    idx_matched[i] = j

                    is_missing_i = False
                    # print("matched: ", i, j)
                    break

                elif ts_real[j] <= ts_std[i] - 604800:
                    current_position = j

                    ts_matched.append(None)
                    idx_matched[i] = None
                    # print("not matched: ", i, j)
                    break

                else:
                    current_position = j
                    # print("still matching: ", i, j)
                    continue

        if i == len(ts_std) - 1:
            if drop_none:
                break
            else:
                num_std = len(ts_std)
                num_matched = len(ts_matched)
                num_unmatched = num_std - num_matched

                ts_matched.extend([None] * num_unmatched)
                for x in range(num_matched, num_std):
                    idx_matched[x] = None
                break

    return ts_matched, idx_matched


def outer_match4index_b7(ts_real, ts_std):
    """
    [,)
    """
    length_std = len(ts_std)
    tmp = [[x for x in ts_real if x <= ts_std[i] + 604800 and x > ts_std[i]] for i in range(length_std)]
    ts_matched = [min(x) if len(x) > 0 else None for x in tmp]
    idx_matched = [ts_real.index(x) if x is not None else None for x in ts_matched]
    idx_matched = dict(zip(range(length_std), idx_matched))
    return ts_matched, idx_matched


def outer_match4index_m(ts_real, ts_std, drop_none=True):
    idx_matched = {}
    ts_matched = []

    current_position = 0
    for i in range(len(ts_std) - 1):
        is_missing_i = True
        for j in range(current_position, len(ts_real)):

            if is_missing_i and current_position < len(ts_real):
                if (ts_real[j] > ts_std[i + 1] and ts_real[j] <= ts_std[i]):
                    current_position = j
                    ts_matched.append(ts_real[j])
                    idx_matched[i] = j

                    is_missing_i = False
                    break

                elif ts_real[j] <= ts_std[i + 1]:
                    current_position = j

                    # ts_matched.append(None)
                    # idx_matched[i] = None
                    break

                else:
                    current_position = j
                    continue

        if i == len(ts_std) - 2:
            if drop_none:
                break
            else:
                num_std = len(ts_std) - 1
                num_matched = len(ts_matched)
                num_unmatched = num_std - num_matched

                ts_matched.extend([None] * num_unmatched)
                for x in range(num_matched, num_std):
                    idx_matched[x] = None
                break

    return ts_matched, idx_matched


def outer_match4index_w(ts_real, ts_std, drop_none=True):
    idx_matched = {}
    ts_matched = []

    current_position = 0
    for i in range(len(ts_std) - 1):
        is_missing_i = True
        for j in range(current_position, len(ts_real)):

            if is_missing_i and current_position < len(ts_real):
                if (ts_real[j] >= ts_std[i + 1] and ts_real[j] < ts_std[i]):
                    current_position = j
                    ts_matched.append(ts_real[j])
                    idx_matched[i] = j

                    is_missing_i = False
                    break

                elif ts_real[j] < ts_std[i + 1]:
                    current_position = j

                    ts_matched.append(None)
                    idx_matched[i] = None
                    break

                else:
                    current_position = j
                    continue

        if i == len(ts_std) - 2:
            if drop_none:
                break
            else:
                num_std = len(ts_std) - 1
                num_matched = len(ts_matched)
                num_unmatched = num_std - num_matched

                ts_matched.extend([None] * num_unmatched)
                for x in range(num_matched, num_std):
                    idx_matched[x] = None
                break

    return ts_matched, idx_matched


def outer_match4indicator_w(ts_real, ts_std, drop_none=True):
    idx_matched = {}
    ts_matched = []

    current_position = 0
    for i in range(len(ts_std) - 1):
        is_missing_i = True
        for j in range(current_position, len(ts_real)):

            if is_missing_i and current_position < len(ts_real):
                if (ts_real[j] > ts_std[i + 1] and ts_real[j] <= ts_std[i]):
                    current_position = j
                    ts_matched.append(ts_real[j])
                    idx_matched[i] = j

                    is_missing_i = False
                    break

                elif ts_real[j] <= ts_std[i + 1]:
                    current_position = j

                    ts_matched.append(None)
                    idx_matched[i] = None
                    break

                else:
                    current_position = j
                    continue

        if i == len(ts_std) - 2:
            if drop_none:
                break
            else:
                num_std = len(ts_std) - 1
                num_matched = len(ts_matched)
                num_unmatched = num_std - num_matched

                ts_matched.extend([None] * num_unmatched)
                for x in range(num_matched, num_std):
                    idx_matched[x] = None
                break

    return ts_matched, idx_matched


def outer_match4indicator_m(ts_real, ts_std, drop_none=True):
    return outer_match4index_m(ts_real, ts_std, drop_none)


def date_of_weekday(date, weekday, shift=(0, 0)):
    """
    Get the date of the `weekday` in the specified week where the `date` locates.
    Default the week ranges from (Sunday of last week, Sunday of this week]

    Args:
        date: datetime.date
            A date to anchor a week;
        weekday: int
            The weekday to be search, 0 refers to Monday and 6 refers to Sunday;
        shift: tuple, default (0, 0)
            The first 0 refers to days to be shifted from Sunday, and the second 0 refers to weeks to be shifted based
            on the return date.
            e.g. If (2, 1) is given, then the week ranges from (Friday of last week, Friday of this week], and return
            date will be shifted to one week later;

    Returns:

    """

    wday = date.timetuple().tm_wday
    delta = weekday - wday if wday <= (6 - shift[0]) else weekday + 7 - wday
    date_weekday = date + dt.timedelta(delta) + dt.timedelta(7 * shift[1])
    return date_weekday
