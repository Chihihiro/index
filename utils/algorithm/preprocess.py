import calendar as cld
import datetime as dt
from dateutil.relativedelta import relativedelta
import functools
import pandas as pd
import multiprocessing
from multiprocessing.dummy import Pool as ThreadPool
from utils.database import config as cfg
from utils.database.sqlfactory import SQL as SQL_PRIVATE
from utils.algorithm import timeutils as tu
from utils.script import scriptutils as su
from utils.dev import meta

SQL_USED = SQL_PRIVATE

_engine_rd = cfg.load_engine()["2Gb"]
_extend = {
    "d": dict.fromkeys(["a", 12, 24, 36, 60], 30),
    "w": dict.fromkeys(["a", 12, 24, 36, 60], 4),
    "m": dict.fromkeys(["a", 12, 24, 36, 60], 1)
}

_shift = {
    "y1_treasury_rate": 1
}

_mkt = ["hs300", "csi500", "sse50", "cbi", "nfi", "y1_treasury_rate"]
_pe = ["FI{idx}".format(idx=(2 - len(str(idx))) * "0" + str(idx)) for idx in range(1, 14)]
_intervals = [1, 3, 6, 12, 24, 36, 60, "w", "m", "q", "a", "whole"]

_poolsize = {
    "process": multiprocessing.cpu_count(),
    "thread": 20
}

_periods_of_freq = {
    "d": 250,
    "w": 52,
    "m": 12
}

# _threadpool = ThreadPool(processes=_poolsize.get("thread"))


class Preprocess:
    def __init__(self, statistic_date, targets):
        pass


# fetching DB
def fetch_fids_used(**kwargs):
    """

    Args:
        **kwargs:
            statistic_date:
            freq:
    Returns:
        list
    """
    kw_used = ("statistic_date", "freq", "conn")
    statistic_date, freq, conn = meta.get_kwargs_used(kw_used, **kwargs)
    sql_ids_used = SQL_USED.ids_updated_sd(statistic_date, freq)
    ids_used = [x[0] for x in conn.execute(sql_ids_used)]
    return ids_used


def fetch_found_date(**kwargs):
    """

    Args:
        **kwargs:
            iids:
            id_type:
            use_tmstmp:
            conn:

    Returns:
        dict
    """
    kw_used = ("iids", "id_type", "conn")
    iids, id_type, conn = meta.get_kwargs_used(kw_used, **kwargs)
    sql_foundation_date = SQL_USED.foundation_date(ids=iids, id_type=id_type)
    d_foundation_date = pd.read_sql(sql_foundation_date, conn)

    if id_type == "fund":
        k1 = "fund_id"
    elif id_type == "org":
        k1 = "org_id"

    if kwargs.get("use_tmstmp") is True:
        d_foundation_date["t_min"] = d_foundation_date["t_min"].apply(lambda x: dt.datetime(x.year, x.month, x.day).timestamp())

    result = dict(
        zip(d_foundation_date[k1], d_foundation_date["t_min"])
    )

    return result


def fetch_firstnv_date(**kwargs):
    """

    Args:
        **kwargs:
            iids:
            use_tmstmp:
            conn:

    Returns:
        dict
    """
    kw_used = ("iids", "conn")
    iids, conn = meta.get_kwargs_used(kw_used, **kwargs)

    sql_firstnv_date = SQL_USED.firstnv_date(iids)
    d_firstnv_date = pd.read_sql(sql_firstnv_date, con=conn)

    if kwargs.get("use_tmstmp") is True:
        d_firstnv_date["t_min"] = d_firstnv_date["t_min"].apply(lambda x: dt.datetime(x.year, x.month, x.day).timestamp())

    return dict(zip(d_firstnv_date["fund_id"], d_firstnv_date["t_min"]))


def fetch_firstdate_used(**kwargs):
    """

    Args:
        **kwargs:
            iids:
            id_type:
            use_tmstmp:
            conn:

    Returns:
        dict
    """
    kw_used = ("iids", "id_type", "use_tmstmp", "check_date", "conn")
    iids, id_type, use_tmstmp, check_date, conn = meta.get_kwargs_used(kw_used, **kwargs)
    check_date = tu.universal_time(check_date)[1]
    error_list = []
    first_date = fetch_found_date(iids=iids, id_type=id_type, use_tmstmp=use_tmstmp, conn=conn)

    for iid, date in first_date.items():
        if date > check_date:
            error_list.append(iid)

    if len(error_list) > 0:
        print(
            "{num_error} of {num_total} funds ({err_funds}) foundation date may be wrong, using first nv date instead...".format(
                num_error=len(error_list),
                num_total=len(iids),
                err_funds=error_list
            )
        )

    # if len(first_date) != len(iids):
    if (len(first_date) - len(error_list)) != len(iids):    # 有错误的首个日期应该为: 1)首日期为空 2) 首个净值日期错误两种情况的并集;
        if id_type == "fund":
            print(
                "{num_missing} of {num_total} funds' foundation date missing, using first nv date instead...".format(
                    num_missing=len(set(iids) - first_date.keys()),
                    num_total=len(iids)
                )
            )
            firstnv_date = fetch_firstnv_date(iids=(set(iids) - first_date.keys() | set(error_list)),
                                              use_tmstmp=use_tmstmp, conn=conn)
            first_date.update(firstnv_date)

    return first_date


def fetch_fundtype(**kwargs):
    """

    Args:
        **kwargs:
            iids:
            dimension: {"strategy", "structure", "target", "issuance"}
            level: 1
            conn:
    Returns:

    """
    kw_used = ("iids", "dimension", "level", "conn")
    iids, dimension, level, conn = meta.get_kwargs_used(kw_used, **kwargs)
    sql_ftype = SQL_USED.fund_type(iids, dimension, level)
    if sql_ftype is not None:
        df = pd.read_sql(sql_ftype, con=conn)
        results = dict(zip(df["fund_id"], df["code"]))
        return results
    else:
        return {}


def fetch_fundname(**kwargs):
    kw_used = ("iids", "level", "conn")
    iids, level, conn = meta.get_kwargs_used(kw_used, **kwargs)
    sql_fname = SQL_USED.fund_name(iids, level=level)
    df = pd.read_sql(sql_fname, con=conn)
    results = dict(zip(df["fund_id"], df["fund_name"]))
    return results


def fetch_fundnv(**kwargs):
    """

    Args:
        **kwargs:
            iids:
            use_tmstmp:
            conn:

    Returns:
        pandas.DataFrame
    """
    kw_used = ("iids", "conn")
    iids, conn = meta.get_kwargs_used(kw_used, **kwargs)

    if "processes" in kwargs:
        _threadpool = ThreadPool(processes=_poolsize.get("thread"))
        tasks = [SQL_USED.nav(x) for x in _get_chunk(iids, kwargs.get("processes"))]
        my_func = functools.partial(pd.read_sql, con=conn)
        results = _threadpool.map(my_func, tasks)
        _threadpool.close()
        result = pd.DataFrame()
        for df in results:
            result = result.append(df)
    else:
        sql_fund_nv = SQL_USED.nav(iids)
        result = pd.read_sql(sql_fund_nv, con=conn)

    if kwargs.get("use_tmstmp") is True:
        # result["statistic_date"] = result["statistic_date"].apply(lambda x: pd.Timestamp(x).timestamp())
        result["statistic_date"] = result["statistic_date"].apply(lambda x: dt.datetime(x.year, x.month, x.day).timestamp())

    result.sort_values(by=["fund_id", "statistic_date"], ascending=[True, False], inplace=True)
    result.index = range(len(result))

    return result


def fetch_peindex(**kwargs):
    """

    Args:
        **kwargs:
            statistic_date:
            iids:
            freq:
            use_tmstmp:
            conn:

    Returns:
        pandas.DataFrame
    """
    kw_used = ("statistic_date", "iids", "freq", "conn")
    statistic_date, iids, freq, conn = meta.get_kwargs_used(kw_used, **kwargs)

    sql_pes = SQL_USED.pe_index(statistic_date, iids, freq)
    df_pes = pd.read_sql(sql_pes, conn)

    if kwargs.get("use_tmstmp") is True:
        df_pes["statistic_date"] = df_pes["statistic_date"].apply(lambda x: dt.datetime(x.year, x.month, x.day).timestamp())

    df_pes.sort_values(by=["index_id", "statistic_date"], ascending=[True, False], inplace=True)
    df_pes.index = range(len(df_pes))

    return df_pes


def fetch_marketindex(**kwargs):
    """

    Args:
        **kwargs:
            statistic_date:
            iids:
            use_tmstmp:
            conn:

    Returns:
        pandas.DataFrame
    """
    kw_used = ("statistic_date", "iids", "transform", "conn")
    statistic_date, iids, apply, conn = meta.get_kwargs_used(kw_used, **kwargs)

    if type(iids) is str:
        iids = [iids]

    sql_bms = SQL_USED.market_index(statistic_date, iids, whole=True)
    df_bms = pd.read_sql(sql_bms, conn)
    if "y1_treasury_rate" in iids:
        df_bms.loc[:, "y1_treasury_rate"] = df_bms["y1_treasury_rate"].fillna(method="backfill")

    if apply is not None:
        for col, func in apply.items():
            df_bms[col] = df_bms[col].apply(func)

    result = pd.DataFrame()
    for col in df_bms.columns[:-1]:
        tmp = df_bms.loc[:, [col, "statistic_date"]]
        tmp.loc[:, "index_id"] = col
        tmp.columns = ["index_value", "statistic_date", "index_id"]
        tmp = tmp.dropna()
        result = result.append(tmp)
    result = result[["index_id", "index_value", "statistic_date"]]

    if kwargs.get("use_tmstmp") is True:
        # result.loc[:, "statistic_date"] = result["statistic_date"].apply(lambda x: pd.Timestamp(x).timestamp())
        result.loc[:, "statistic_date"] = result["statistic_date"].apply(lambda x: dt.datetime(x.year, x.month, x.day).timestamp())

    result.sort_values(by=["index_id", "statistic_date"], ascending=[True, False], inplace=True)
    result.index = range(len(result))

    return result


def generate_tasks(update_time_l, update_time_r=None, freq=None, **kwargs):
    """
        Generate incremental calculation tasks of different frequency;
    Args:
        update_time_l:
        update_time_r:
        freq:
        **kwargs:
            processes: int
                thread pool size used when fetching dates data from database;
    Returns:
        tasks<dict>
        e.g.
        {
        date1: {fid1, fid2, ...}
        }
    """
    dates = dates_after_updateddate(update_time_l, update_time_r, freq, **kwargs)
    tasks = {}
    if freq in {"d", "w"}:
        for k, v in zip(dates["statistic_date"], dates["fund_id"]):
            tasks.setdefault(k, set()).add(v)
    elif freq == "m":
        dates["statistic_date"] = dates["statistic_date"].apply(lambda x: dt.date(x.year, x.month, cld.monthrange(x.year, x.month)[1]))
        dates = dates.drop_duplicates(["fund_id", "statistic_date"])
        for k, v in zip(dates["statistic_date"], dates["fund_id"]):
            tasks.setdefault(k, set()).add(v)
    # return dates
    return tasks


def dates_after_updateddate(update_time_l, update_time_r=None, data_freq="w", **kwargs):
    """
        Fetch statistic_date(s) of fund_nav after statistic_date on the updated_time
    Args:
        update_time_l:
        update_time_r:
        **kwargs:
            processes
            conn
    Returns:

    """
    processes = kwargs.get("processes", 5)
    conn = kwargs.get("conn", _engine_rd)
    _threadpool = ThreadPool(processes=processes)
    min_dates = mindate_after_updateddate(update_time_l, update_time_r, data_freq, conn=conn)
    if len(min_dates) < 20000:
        tasks = [SQL_USED.fetch_dates(x) for x in _get_chunk(min_dates, processes)]
        dfs = _threadpool.map(functools.partial(pd.read_sql, con=conn), tasks)
        _threadpool.close()
        df_merged = merge_dataframes(dfs)

    else:
        min_date_chunks = _get_chunk(min_dates, len(min_dates) // 10000 + 2)
        df_merged = pd.DataFrame()
        for min_date_chunk in min_date_chunks:
            tasks = [SQL_USED.fetch_dates(x) for x in _get_chunk(min_date_chunk, processes)]
            dfs = _threadpool.map(functools.partial(pd.read_sql, con=conn), tasks)
            _threadpool.close()
            tmp = merge_dataframes(dfs)
            df_merged = df_merged.append(tmp)

    return df_merged


def mindate_after_updateddate(update_time_l, update_time_r=None, data_freq="w", **kwargs):
    """

    Args:
        update_time_l:
        update_time_r:

    Returns:

    """
    conn = kwargs.get("conn", _engine_rd)
    sql_min_dates = SQL_USED.generate_min_date(update_time_l, update_time_r, data_freq)
    min_dates = pd.read_sql(sql_min_dates, conn).as_matrix()
    if len(min_dates) > 0:
        min_dates = dict(min_dates)
    else:
        raise ValueError("No updated data")
    return min_dates


def merge_dataframes(list_df):
    for i, df in enumerate(list_df):
        if i > 0:
            df_merged = df_merged.append(df)
        else:
            df_merged = df
        df_merged.index = range(len(df_merged))
    return df_merged


# preprocess function
def slice_data(dataframe, slice_by, cols_used):
    """

    Args:
        dataframe:
        slice_by:
        cols_used:

    Returns:
        list<pandas.DataFrame>
    """
    if dataframe is not None:
        df_tmp = dataframe.copy()

        bys = df_tmp[slice_by].drop_duplicates().tolist()
        df_tmp.index = df_tmp[slice_by]

        result = {}
        idx4slice = su.idx4slice(dataframe, slice_by)
        for col in cols_used:
            tmp = su.slice(df_tmp, idx4slice, col)
            result[col] = {by: value for by, value in zip(bys, tmp)}

        result = _reverse_dict(result)
    else:
        result = None

    return result


def gen_stdseries_longest(**kwargs):
    """

    Args:
        **kwargs:
            iids: collections.Iterable or dict{id: tm_series}
            date_s: datetime.date, or dict{id: <datetime.date>}
            date_e: datetime.date, or dict{id: <datetime.date>}
            freq: str

    Returns:

    """
    kw_used = ("iids", "key_tm", "date_s", "date_e", "freq")
    iids, key_tm, date_s, date_e, freq = meta.get_kwargs_used(kw_used, **kwargs)

    if isinstance(date_s, (dt.date, dt.datetime, float)):
        date_s = dict.fromkeys(iids, date_s)
    elif date_s is None:
        date_s = {iid: max(attributes[key_tm]) for iid, attributes in iids.items()}
    if isinstance(date_e, (dt.date, dt.datetime, float)):
        date_e = dict.fromkeys(iids, date_e)
    elif date_e is None:
        date_e = {iid: min(attributes[key_tm]) for iid, attributes in iids.items()}

    t_std_alls = {}
    for iid in iids:
        t_std_alls[iid] = tu.timeseries_std_se(date_s.get(iid), date_e.get(iid), periods_y=_periods_of_freq.get(freq), weekday=kwargs.get("weekday", False))

    return t_std_alls


def gen_stdseries(**kwargs):
    """
        Generate standard time series of various intervals for aligning.

    Args:
        **kwargs:

    Returns:

    """
    kw_used = ("date_s", "intervals", "freq", "extend")
    date_s, intervals, freq, extend = meta.get_kwargs_used(kw_used, **kwargs)

    if extend is None:
        tm_series_std = {
            interval: tu.timeseries_std(date_s, interval, _periods_of_freq[freq],
                                        # extend=1 * int(freq == "d" and interval != "m"),
                                        extend=1 + 1 * int(not(freq == "d" and interval == "m")),
                                        use_lastday=True,  # 处理日频, 本月以来区间多取一个点的问题
                                        keep_firstday=True,
                                        weekday=kwargs.get("weekday", False))
            for interval in intervals if interval != "whole"
        }
    else:
        tm_series_std = {
            interval: tu.timeseries_std(date_s, interval, _periods_of_freq[freq],
                                        extend=1 + extend.get(interval, 0) + 1 * int(freq == "d" and interval != "m"),   # 区间"今年以来", 以及大于等于一年的基金, 需要加长一个月的数据, 用于算法的区间外搜索;
                                        use_lastday=True,
                                        keep_firstday=True,
                                        weekday=kwargs.get("weekday", False))
            for interval in intervals if interval != "whole"
        }
    return tm_series_std


def match_by_std(obj, **kwargs):
    """
        Match objects with time series to standard time series, and apply the strategy to its other attributes.

    Args:
        obj: dict
            Dict like {id: {key1: Iterable, key2: Iterable, ...}};

        **kwargs:
            key_tm: str
                Key of the time series;
            key_used: Iterable<str>
                keys to match to standard time series;
            date_s: datetime.date, datetime.datetime, or float
                Statistic date(or the start date) of the standard time series;
            date_e: datetime.date, datetime.datetime, float, or dict
                Earliest date(or the end date) of the standard time series. If a dict is passed, then it should be
                {id: date}_like and its ids should be the same as the `obj` length;
            intervals: Iterable
                Interval of the standard time series to match, optional {1, 3, 6, 12, 24, 36, 60, "w", "m", "q", "a",
                "whole"};
            freq: str
                Frequency of the standard time series, optional {"w", "m"};
            extend: int, or dict
                Extra sample number of `interval` to use. If an int is parsed, then all intervals in `interval` will use
                this int as the extra sample number, else if an dict like {interval: extra_num} is parsed, then the
                specified interval will use the given extra number. Default None;
            shift: dict
                Dict like {id: shift_num} to specified ids which need to be shifted on its match case;
            apply: dict
                Dict like {id: func}
    Returns:
        dict like {id: {key_used:{interval: Iterable}}}
    """
    kw_used = ("key_tm", "key_used", "date_s", "date_e", "intervals", "freq", "shift", "extend", "apply")
    key_tm, key_used, date_s, date_e, intervals, freq, shift, extend, apply = meta.get_kwargs_used(kw_used, **kwargs)

    if isinstance(date_e, (dt.date, dt.datetime, float)):
        date_e = dict.fromkeys(obj.keys(), tu.universal_time(date_e)[1])
    elif date_e is None:
        date_e = {iid: tu.universal_time(min(attributes[key_tm]))[1] for iid, attributes in obj.items()}

    tm_series_std_alls = gen_stdseries_longest(iids=obj, key_tm=key_tm, date_s=date_s, date_e=date_e, freq=freq, weekday=kwargs.get("weekday", False))
    tm_series_std = gen_stdseries(date_s=date_s, freq=freq, intervals=intervals, extend=extend, weekday=kwargs.get("weekday", False))

    sample_nums = {interval: len(tm_serie_std) for interval, tm_serie_std in tm_series_std.items()}

    if freq == "w" or freq == "d":
        # matchs_whole = {
        #     iid: tu.outer_match4indicator_w(attributes[key_tm], tm_series_std_alls[iid], False)[1] for iid, attributes
        #     in
        #     obj.items()
        # }
        # matchs_w = {
        #     iid: tu.outer_match4indicator_w(attributes[key_tm], tm_series_std["w"])[1] for iid, attributes in
        #     obj.items()
        # }

        matchs_whole = {}
        for iid, attributes in obj.items():
            values = tu.outer_match4indicator_w(attributes[key_tm], tm_series_std_alls[iid], False)
            value = values[1]
            matchs_whole[iid] = value

        matchs_w = {}
        for iid, attributes in obj.items():
            values = tu.outer_match4indicator_w(attributes[key_tm], tm_series_std["w"])
            value = values[1]
            matchs_w[iid] = value

    elif freq == "m":

        matchs_whole = {}
        for iid, attributes in obj.items():
            values = tu.outer_match4indicator_m(attributes[key_tm], tm_series_std_alls[iid], False)
            value = values[1]
            matchs_whole[iid] = value

    result = dict.fromkeys(obj.keys(), {})
    if shift is None:
        shift = {}

    # match for each object
    date_s_dt = dt.date.fromtimestamp(tu.universal_time(date_s)[1])
    # intervals_regular = [interval for interval in intervals if interval not in ("w", "a", "whole")]   #
    intervals_regular = [interval for interval in intervals if interval not in ("w", "whole")]
    for iid, attributes in obj.items():
        shift_iid = shift.get(iid, 0)
        result[iid] = dict.fromkeys(key_used, {})
        date_e_iid = dt.date.fromtimestamp(date_e[iid])
        for key in result[iid].keys():
            freq_of_key = {}

            freq_of_key["whole"] = [attributes[key][idx] if idx is not None else None for idx in
                                    matchs_whole[iid].values()]

            if apply is not None and iid in apply:
                freq_of_key["whole"] = apply[iid](freq_of_key["whole"])

            if shift_iid > 0:
                freq_of_key["whole"] = freq_of_key["whole"][shift_iid:]

            # 根据每个基金产品的成立时间判断可以计算多长区间
            interval_used = _check_intervals(date_s_dt, date_e_iid, intervals)

            length_max = len(freq_of_key["whole"])
            for interval in intervals_regular:
                if interval_used[interval]:
                    sp_num = sample_nums[interval] - shift_iid - 1  #
                    # sp_num = sample_nums[interval] - 1
                    freq_of_key[interval] = freq_of_key["whole"][:sp_num]
                    if sp_num > length_max:
                        freq_of_key[interval].extend([None] * (sp_num - length_max))
                else:
                    freq_of_key[interval] = None

            # freq_of_key["a"] = [attributes[key][idx] if idx is not None else None for idx in matchs_a[iid].values()]  #

            if freq == "w" or freq == "d":
                # freq_of_key["w"] = [attributes[key][idx] if idx is not None else None for idx in
                #                     matchs_w[iid].values()]

                datas = []
                if iid in matchs_w.keys():
                    for idx in matchs_w[iid].values():
                        if idx is not None:
                            datas.append(attributes[key][idx])
                        else:
                            datas.append(None)
                freq_of_key["w"] = datas

            result[iid][key] = freq_of_key

    return result


def update_attributes(attribute, obj_dict, attribute_dict):
    for iid in obj_dict.keys():
        obj_dict[iid][attribute] = attribute_dict.get(iid)

    return obj_dict


# helper function
def _reverse_dict(dictionary):
    """

    Args:
        dictionary:

    Returns:

    """
    result = {}
    for k1 in dictionary.keys():
        for k2 in dictionary[k1].keys():
            tmp = dictionary[k1][k2]
            if k2 in result:
                pass
            else:
                result[k2] = {}
            result[k2][k1] = tmp

    return result


def _get_chunk(tasks, pool):
    """

    Args:
        tasks:
        chunks:

    Returns:

    """
    num = len(tasks)
    if num >= pool:
        chunk_size = num // (pool - 1)
    else:
        chunk_size = num

    if type(tasks) is list:
        chunks = [tasks[i: i + chunk_size] for i in range(0, num, chunk_size)]
    elif type(tasks) is dict:
        ls = sorted(tasks.items())
        chunks = [{k: v for k, v in ls[i: i + chunk_size]} for i in range(0, num, chunk_size)]
    # print("num of tasks: %s" % len(chunks))

    return chunks


def _fill_none(list_with_None, reverse=True):
    if reverse:
        tmp_list = list_with_None[::-1]
    else:
        tmp_list = list_with_None[:]
    for i in range(len(tmp_list) - 1):
        if tmp_list[i + 1] is None:
            tmp_list[i + 1] = tmp_list[i]
    return tmp_list[::-1]


def _check_intervals(date_s, date_e, intervals):
    delta = relativedelta(date_s, date_e)
    delta_months = delta.years * 12 + delta.months
    interval_to_used = {}
    for interval in intervals:
        if type(interval) is not str:
            interval_to_used.update(
                {
                    interval: delta_months >= interval
                }
            )
        else:
            interval_to_used[interval] = True
    return interval_to_used


# A higher level encapsulation of preprocessed data
class ProcessedData:
    """
        Use `funds`, `index` property
    """

    def __init__(self, statistic_date, fids, freq, mkt=_mkt, pe=_pe, **kwargs):
        self.fids = fids
        self.sd = statistic_date
        self.freq = freq
        self.mkt = mkt
        self.pe = pe
        self.index = {}
        self._id_type = kwargs.get("id_type", "fund")

        engine_rd = kwargs.get("conn", _engine_rd)
        engine_mkt = kwargs.get("conn_mkt", None)
        engine_pe = kwargs.get("conn_pe", None)

        if len(self.fids) > 0:
            if len(self.fids) >= 5:
                df_fund = fetch_fundnv(iids=self.fids, conn=engine_rd, processes=5, use_tmstmp=True)
            elif len(self.fids) < 5:
                df_fund = fetch_fundnv(iids=self.fids, conn=engine_rd, use_tmstmp=True)
            d_fdate = fetch_firstdate_used(iids=self.fids, conn=engine_rd, id_type=self._id_type, use_tmstmp=True, check_date=self.sd)
            d_type_s = fetch_fundtype(iids=self.fids, dimension="strategy", level=2, conn=engine_rd)
            d_fname = fetch_fundname(iids=self.fids, level=1, conn=engine_rd)
            d_fund = slice_data(df_fund, slice_by="fund_id", cols_used=["nav", "statistic_date"])
            self.funds = match_by_std(obj=d_fund, key_tm="statistic_date", key_used=["nav", "statistic_date"],
                                      date_s=self.sd, date_e=d_fdate, extend=_extend[self.freq],
                                      intervals=_intervals, freq=self.freq, weekday=kwargs.get("weekday", False))

            self.funds = update_attributes("id", self.funds, dict(zip(self.funds.keys(), self.funds.keys())))
            self.funds = update_attributes("type_s", self.funds, d_type_s)
            self.funds = update_attributes("name", self.funds, d_fname)

        if len(self.mkt) > 0:
            df_mkt = fetch_marketindex(statistic_date=self.sd, iids=self.mkt, conn=(engine_mkt or engine_rd),
                                       transform={"y1_treasury_rate": lambda x: su.annually2freq(x, freq)},
                                       use_tmstmp=True)
            d_mkt = slice_data(df_mkt, slice_by="index_id", cols_used=["index_value", "statistic_date"])
            self.index.update(match_by_std(obj=d_mkt, key_tm="statistic_date", key_used=["index_value", "statistic_date"],
                                           date_s=self.sd, intervals=_intervals, freq=self.freq,
                                           extend=_extend[self.freq],
                                           apply={"y1_treasury_rate": _fill_none}, shift=_shift, weekday=kwargs.get("weekday", False)))  # 一年期国债利率需要移动1期

        if len(self.pe) > 0:
            df_pe = fetch_peindex(statistic_date=statistic_date, iids=self.pe, conn=(engine_pe or engine_rd), freq=self.freq,
                                  use_tmstmp=True)
            d_pe = slice_data(df_pe, slice_by="index_id", cols_used=["index_value", "statistic_date"])
            self.index.update(
                match_by_std(obj=d_pe, key_tm="statistic_date", key_used=["index_value", "statistic_date"],
                             date_s=statistic_date, intervals=_intervals, extend=_extend[self.freq],
                             freq=self.freq, weekday=kwargs.get("weekday", False)))
