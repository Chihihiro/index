import sys
import pandas as pd
from WindPy import w as wind
wind.start()

_PREVIOUS_DAY = 0
_CODE_MAP = {
    "000300.SH": "hs300",
    "000905.SH": "csi500",
    "000016.SH": "sse50",
    "000002.SH": "ssia",
    "H11011.CSI": "cbi",
    "NH0100.NHF": "nfi",
    # "M1001940.NHF": "y1_treasury_rate",
    "M1001940": "y1_treasury_rate"
}

_CODE_MAP = {
    "000300.SH": "hs300",
    "000905.SH": "csi500",
    "000016.SH": "sse50",
    "000002.SH": "ssia",
    "H11011.CSI": "cbi",
    "NH0100.NHF": "nfi",
    # "M1001940.NHF": "y1_treasury_rate",
    "M1001940": "y1_treasury_rate"
}


def sql_cols(df, usage="sql"):
    cols = tuple(df.columns)
    if usage == "sql":
        cols_str = str(cols).replace("'", "`")
        if len(df.columns) == 1:
            cols_str = cols_str[:-2] + ")"  # to process dataframe with only one column
        return cols_str
    elif usage == "format":
        base = "'%%(%s)s'" % cols[0]
        for col in cols[1:]:
            base += ", '%%(%s)s'" % col
        return base
    elif usage == "values":
        base = "%s=VALUES(%s)" % (cols[0], cols[0])
        for col in cols[1:]:
            base += ", `%s`=VALUES(`%s`)" % (col, col)
        return base


def to_sql(tb_name, conn, dataframe, type="update", chunksize=2000):
    """
    Dummy of pandas.to_sql, support "REPLACE INTO ..." and "INSERT ... ON DUPLICATE KEY UPDATE (keys) VALUES (values)"
    SQL statement.

    Args:
        tb_name: str
            Table to insert get_data;
        conn:
            DBAPI Instance
        dataframe: pandas.DataFrame
            Dataframe instance
        type: str, optional {"update", "replace"}, default "update"
            Specified the way to update get_data. If "update", then `conn` will execute "INSERT ... ON DUPLICATE UPDATE ..."
            SQL statement, else if "replace" chosen, then "REPLACE ..." SQL statement will be executed;
        chunksize: int
            Size of records to be inserted each time;
        **kwargs:

    Returns:
        None
    """

    df = dataframe.copy()
    df = df.fillna("None")
    cols_str = sql_cols(df)
    for i in range(0, len(df), chunksize):
        # print("chunk-{no}, size-{size}".format(no=str(i/chunksize), size=chunksize))
        df_tmp = df[i: i + chunksize]
        if type == "replace":
            sql_base = "REPLACE INTO `{tb_name}` {cols}".format(
                tb_name=tb_name,
                cols=cols_str
            )
            sql_val = sql_cols(df_tmp, "format")
            vals = tuple([sql_val % x for x in df_tmp.to_dict("records")])
            sql_vals = "VALUES ({x})".format(x=vals[0])
            for i in range(1, len(vals)):
                sql_vals += ", ({x})".format(x=vals[i])
            sql_vals = sql_vals.replace("'None'", "NULL")

            sql_main = sql_base + sql_vals

        elif type == "update":
            sql_base = "INSERT INTO `{tb_name}` {cols}".format(
                tb_name=tb_name,
                cols=cols_str
            )
            sql_val = sql_cols(df_tmp, "format")
            vals = tuple([sql_val % x for x in df_tmp.to_dict("records")])
            sql_vals = "VALUES ({x})".format(x=vals[0])
            for i in range(1, len(vals)):
                sql_vals += ", ({x})".format(x=vals[i])
            sql_vals = sql_vals.replace("'None'", "NULL")

            sql_update = "ON DUPLICATE KEY UPDATE {0}".format(
                sql_cols(df_tmp, "values")
            )

            sql_main = sql_base + sql_vals + sql_update
        if sys.version_info.major == 2:
            sql_main = sql_main.replace("u`", "`")
        sql_main = sql_main.replace("%", "%%")
        conn.execute(sql_main)


class Index:
    def __init__(self, date_s, date_e):
        self._cached = {}
        self._params = {
            "date_s": date_s.strftime("%Y%m%d"),
            "date_e": date_e.strftime("%Y%m%d")
        }

    @property
    def params(self):
        return self._params


class MarketIndex(Index):
    def __init__(self, date_s, date_e):
        super().__init__(date_s, date_e)
        # self._fields = ["close", "lastradeday_s"]
        self._fields = ["close"]
        self._params.update(fields=",".join(self._fields))

    @property
    def csi300(self):
        k = "csi300"
        if k not in self._cached:
            self._cached[k] = wind.wsd("000300.SH", fields=self.params["fields"], beginTime=self.params["date_e"],
                                       endTime=self.params["date_s"], options="")
        return self._cached[k]

    @property
    def csi500(self):
        k = "csi500"
        if k not in self._cached:
            self._cached[k] = wind.wsd("000905.SH", fields=self.params["fields"], beginTime=self.params["date_e"],
                                       endTime=self.params["date_s"], options="")
        return self._cached[k]

    @property
    def sse50(self):
        k = "sse50"
        if k not in self._cached:
            self._cached[k] = wind.wsd("000016.SH", fields=self.params["fields"], beginTime=self.params["date_e"],
                                       endTime=self.params["date_s"], options="")
        return self._cached[k]

    @property
    def ssia(self):
        k = "ssia"
        if k not in self._cached:
            self._cached[k] = wind.wsd("000002.SH", fields=self.params["fields"], beginTime=self.params["date_e"],
                                       endTime=self.params["date_s"], options="")
        return self._cached[k]

    @property
    def cbi(self):
        k = "cbi"
        if k not in self._cached:
            self._cached[k] = wind.wsd("H11011.CSI", fields=self.params["fields"], beginTime=self.params["date_e"],
                                       endTime=self.params["date_s"], options="")
        return self._cached[k]

    @property
    def nfi(self):
        k = "nfi"
        if k not in self._cached:
            self._cached[k] = wind.wsd("NH0100.NHF", fields=self.params["fields"], beginTime=self.params["date_e"],
                                       endTime=self.params["date_s"], options="")
        return self._cached[k]


class TreasuryBond(Index):
    def __init__(self, date_s, date_e):
        super().__init__(date_s, date_e)

    @property
    def tbond_y1(self):
        k = "tbond_y1"
        if k not in self._cached:
            self._cached[k] = wind.edb("M1001940.NHF", beginTime=self.params["date_e"], endTime=self.params["date_s"],
                                       options="Fill=Previous")
        return self._cached[k]


class SWSIndex(Index):
    def __init__(self, date_s, date_e):
        super().__init__(date_s, date_e)
        self._fields = ["close"]
        self._params.update(fields=",".join(self._fields))

    tm = {
        "6102000000000000": ("210000", "采掘"),
        "6103000000000000": ("220000", "化工"),
        "6104000000000000": ("230000", "钢铁"),
        "6105000000000000": ("240000", "有色金属"),
        "6106010000000000": ("610000", "建筑材料"),
        "6106020000000000": ("620000", "建筑装饰"),
        "6107010000000000": ("630000", "电气设备"),
        "6107000000000000": ("640000", "机械设备"),
        "1000012579000000": ("650000", "国防军工"),
        "1000012588000000": ("280000", "汽车"),
        "6111000000000000": ("330000", "家用电器"),
        "6113000000000000": ("350000", "纺织服装"),
        "6114000000000000": ("360000", "轻工制造"),
        "6120000000000000": ("450000", "商业贸易"),
        "6101000000000000": ("110000", "农林牧渔"),
        "6112000000000000": ("340000", "食品饮料"),
        "6121000000000000": ("460000", "休闲服务"),
        "6115000000000000": ("370000", "医药生物"),
        "6116000000000000": ("410000", "公用事业"),
        "6117000000000000": ("420000", "交通运输"),
        "6118000000000000": ("430000", "房地产"),
        "6108000000000000": ("270000", "电子"),
        "1000012601000000": ("710000", "计算机"),
        "6122010000000000": ("720000", "传媒"),
        "1000012611000000": ("730000", "通信"),
        "1000012612000000": ("480000", "银行"),
        "1000012613000000": ("490000", "非银金融"),
        "6123000000000000": ("510000", "综合"),

    }

    def _fetchindex(self, index_id):
        k = ("_fetchindex", index_id)
        if k not in self._cached:
            self._cached[k] = wind.wsd(index_id, fields=self.params["fields"], beginTime=self.params["date_e"],
                                       endTime=self.params["date_s"], options="")
        return self._cached[k]


def construct_mi(date_s, date_e=None):

    mi = MarketIndex(date_s, date_e)
    result = None
    tb = TreasuryBond(date_s, date_e)
    wdata = [mi.csi300, mi.csi500, mi.sse50, mi.ssia, mi.cbi, mi.nfi, tb.tbond_y1]
    for idx, wd in enumerate(wdata):
        tmp = pd.DataFrame(wd.Data).T
        tmp.columns = [_CODE_MAP[wd.Codes[0]]]
        tmp["statistic_date"] = [x.date() for x in wd.Times]
        tmp = tmp[tmp.columns[::-1]]
        if idx == 0:
            result = tmp
        else:
            result = result.merge(tmp, how="outer", on="statistic_date")
    return result
