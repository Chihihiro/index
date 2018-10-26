"""
清洗 - (私募)基金_净值 (10m)
"""
import pandas as pd
from datetime import timedelta
from datetime import datetime
from utils.database import config as cfg, io
from utils.etlkit.core.base import Stream
from utils.etlkit.core import transform
from utils.etlkit.reader.mysqlreader import MysqlInput


ENGINE_RD = cfg.load_engine()["2Gb"]


def now_time(a=0):
    now = datetime.now()
    delta = timedelta(minutes=a)
    n_days = now + delta
    cc = n_days.strftime('%Y-%m-%d %H:%M:%S')
    return cc


def init_firstnv():
    sql = "SELECT t1.fund_id, t1.source_id, t1.nav FROM `fund_nv_data_source_copy2` t1 \
    JOIN (SELECT fund_id, source_id, MIN(statistic_date) msd FROM fund_nv_data_source_copy2 \
    WHERE is_used = 1 GROUP BY fund_id, source_id) t2 \
    ON t1.fund_id = t2.fund_id AND t1.source_id = t2.source_id AND t1.statistic_date = t2.msd \
    WHERE t1.nav > 50"
    tmp = pd.read_sql(sql, ENGINE_RD)
    keys = tuple(zip(tmp["fund_id"], tmp["source_id"]))
    vals = tmp["nav"].tolist()
    d = dict(zip(keys, vals))
    return d


def old_source():
    source_name = {'云通': '私募云通',
                   '三方': '第三方',
                   '券商': '托管机构',
                   '信托': '托管机构',
                   '投顾': '数据授权',
                   '资管': '数据授权',
                   }
    source_code = {'云通': 0,
                   '三方': 3,
                   '券商': 1,
                   '信托': 1,
                   '投顾': 4,
                   '资管': 4,
                   }
    sql = "SELECT source_id, `name` FROM config_private.`source_info`"
    tmp = pd.read_sql(sql, ENGINE_RD)
    tmp["source_code"] = tmp["name"].apply(lambda x: source_code.get(x))
    tmp["source_name"] = tmp["name"].apply(lambda x: source_name.get(x))
    keys = tmp["source_id"].tolist()
    vals1 = tmp["source_code"].tolist()
    vals2 = tmp["source_name"].tolist()
    d = dict(zip(keys, vals1))
    d2 = dict(zip(keys, vals2))
    return [d, d2]


class StreamsMain:

    end = now_time()
    start = now_time(-125)
    init_firstnv = init_firstnv()
    source_name = old_source()[1]
    source_code = old_source()[0]

    @classmethod
    def stream_source_cpoy2(cls):

        sql = "SELECT m.fund_id,ll.fund_name,ll.source_id,m.statistic_date,ll.nav,ll.added_nav FROM \
                (SELECT MAX(priority) mm, statistic_date, fund_id FROM \
                (SELECT fns.fund_id, fns.source_id,  \
                fns.statistic_date, fns.nav, fns.added_nav, sync.pk,sync.is_used,sync.priority \
                FROM base.fund_nv_data_source_copy2 fns \
                JOIN ( \
                SELECT fund_id, statistic_date \
                FROM base.fund_nv_data_source_copy2 WHERE update_time BETWEEN '{start}' AND '{end}' \
                GROUP BY fund_id, statistic_date \
                ) fupt ON fns.fund_id = fupt.fund_id AND fns.statistic_date = fupt.statistic_date \
                JOIN config_private.sync_source AS sync \
                ON sync.pk = fns.fund_id AND sync.source_id = fns.source_id \
                WHERE \
                fns.is_used = 1 AND sync.is_used = 1 AND fns.added_nav is not NULL \
                GROUP BY fns.fund_id,fns.statistic_date,priority) as tt \
                GROUP BY tt.fund_id,tt.statistic_date) as m \
                JOIN ( \
                SELECT fi.fund_id, fi.fund_name, fns.source_id, \
                fns.statistic_date, fns.nav, fns.added_nav, sync.pk,sync.is_used,sync.priority \
                FROM base.fund_info fi \
                JOIN base.fund_nv_data_source_copy2 fns ON fi.fund_id = fns.fund_id \
                JOIN ( \
                SELECT fund_id, statistic_date \
                FROM base.fund_nv_data_source_copy2 WHERE update_time BETWEEN '{start}' AND '{end}' \
                GROUP BY fund_id, statistic_date \
                ) fupt ON fns.fund_id = fupt.fund_id AND fns.statistic_date = fupt.statistic_date \
                JOIN config_private.sync_source AS sync \
                ON sync.pk = fns.fund_id AND sync.source_id = fns.source_id \
                WHERE fns.is_used = 1 AND sync.is_used = 1 and fns.added_nav is not NULL \
                GROUP BY fi.fund_id,fns.statistic_date,priority) AS ll \
                ON m.mm = ll.priority AND m.fund_id = ll.fund_id AND m.statistic_date = ll.statistic_date \
                ".format(start=cls.start, end=cls.end)

        inp = MysqlInput(ENGINE_RD, sql)

        dr = transform.Dropna(subset=['nav', 'added_nav'], axis=0)

        vm = transform.ValueMap({
            "100_mol": (lambda x, y: (str(x), str(y)), "fund_id", "source_id")
        })

        vm2 = transform.ValueMap({
            "100_g": (lambda x: cls.init_firstnv.get(x), "100_mol")
        })

        vm3 = transform.ValueMap({
            "nav": (lambda x, y: x/100 if type(y) is float and y > 70 else x, "nav", "100_g"),
            "added_nav": (lambda x, y: x / 100 if type(y) is float and y > 70 else x, "added_nav", "100_g")
        })

        sk = transform.MapSelectKeys({
            "fund_id": "fund_id",
            'source_id': 'source_id',
            'statistic_date': 'statistic_date',
            'nav': 'nav',
            'added_nav': 'added_nav',
            "fund_name": "fund_name"
        })
        s = Stream(inp, transform=[dr, vm, vm2, vm3, sk])
        return s


    @classmethod
    def confluence(cls):
        c = cls.stream_source_cpoy2().flow()
        d = c[0]
        e = d[(d.nav > 0) & (d.added_nav > 0)]
        return e

    @classmethod
    def to_old(cls):
        df = cls.confluence()
        df["source_code"] = df["source_id"].apply(lambda x: cls.source_code.get(x))
        df["source"] = df["source_id"].apply(lambda x: cls.source_name.get(x))
        return df

    @classmethod
    def write(cls):
        df = cls.confluence()
        df["source"] = df["source_id"].apply(lambda x: cls.source_name.get(x))
        print(df.head())
        io.to_sql("base.fund_nv_data_standard_copy3", ENGINE_RD, df,
                  type="update")
        df_old = cls.to_old()
        print(df_old.head())
        io.to_sql("base.fund_nv_data_standard", ENGINE_RD, df_old.drop(columns="source_id", axis=1),
                  type="update")


def main():
    """

    取当前时间2小时05分之前fund_nv_source更新的净值

    """
    StreamsMain.write()


if __name__ == "__main__":
    main()






