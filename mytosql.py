import os
import pandas as pd
import shutil
import sys
import re
from openpyxl import load_workbook
import time
# import pymysql
import pymssql


def sql_cols(df, usage="sql"):
    cols = tuple(df.columns)
    if usage == "sql":
        cols_str = str(cols).replace("'", "[")
        cols_str = cols_str.replace("[,", "],")
        cols_str = cols_str.replace("[)", "])")
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


def in_to_sql(DD, usr, tb_name, co, dataframe, type="update", chunksize=2000, debug=False):
    # tb_name = ".".join(["`" + x + "`" for x in tb_name.split(".")])
    conn = co.cursor()
    df = dataframe.copy(deep=False)
    df = df.fillna("None")
    df = df.applymap(lambda x: re.sub('([\'\"\\\])', '\\\\\g<1>', str(x)))
    cols_str = sql_cols(df)
    sqls = []
    for i in range(0, len(df), chunksize):
        # print("chunk-{no}, size-{size}".format(no=str(i/chunksize), size=chunksize))
        df_tmp = df[i: i + chunksize]

        # if type == "replace":
        #     sql_base = "REPLACE INTO [tb_name].[db].[cols]".format(
        #         tb_name=tb_name,
        #         db=usr,
        #         cols=cols_str
        #     )

        # elif type == "update":
        sql_base = "INSERT INTO [{BB}].[{db}].[{tb_name}]{cols}".format(
            BB=DD,
            db=usr,
            tb_name=tb_name,
            cols=cols_str
        )
        # sql_update = "ON DUPLICATE KEY UPDATE [0]".format(
        #     sql_cols(df_tmp, "values")
        # )

        # elif type == "ignore":
        #     sql_base = "INSERT IGNORE INTO [tb_name].[db].[cols]".format(
        #         tb_name=tb_name,
        #         db=usr,
        #         cols=cols_str
        #     )

        sql_val = sql_cols(df_tmp, "format")
        vals = tuple([sql_val % x for x in df_tmp.to_dict("records")])
        sql_vals = "VALUES ({x})".format(x=vals[0])
        for i in range(1, len(vals)):
            sql_vals += ", ({x})".format(x=vals[i])
        sql_vals = sql_vals.replace("'None'", "NULL")

        sql_main = sql_base + " " + sql_vals
        if type == "update":
            # sql_main += sql_update
            sql_main = sql_main
        if sys.version_info.major == 2:
            sql_main = sql_main.replace("u`", "`")
        if sys.version_info.major == 3:
            sql_main = sql_main.replace("%", "%%")

        if debug is False:
            try:
                print(sql_main)
                conn.execute(sql_main)
                co.commit()
            # except conn.err.InternalError as e:
            except BaseException as e:
                # print("ENCOUNTERING ERROR: {e}, RETRYING".format(e=e))
                print(e)
                print("注意有错误~~~~~~~~~~~有错误~~~~~~~~~~~")
                # time.sleep(10)
                # conn.execute(sql_main)
        else:
            sqls.append(sql_main)
    if debug:
        return sqls


def up_to_sql(DD, usr, tb_name, co, dataframe, zhu):
    conn = co.cursor()
    df = dataframe.copy(deep=False)
    df = df.fillna("None")
    df = df.applymap(lambda x: re.sub('([\'\"\\\])', '\\\\\g<1>', str(x)))
    cols_str = sql_cols(df)
    col = dataframe.columns.tolist()
    coo = ','.join(col)
    # sqls = []
    zzz = dataframe.columns.tolist()
    z1 = zhu[0]

    for nn in range(0, len(zhu)):
        zzz.remove(zhu[nn])

    for i in range(0, len(df)):
        # print("chunk-{no}, size-{size}".format(no=str(i/chunksize), size=chunksize))
        df_tmp = df[i: i + 1]
        sql1 = "if exists (select {cols} from [{BB}].[{db}].[{tb_name}] ".format(
            BB=DD,
            db=usr,
            tb_name=tb_name,
            cols=coo)

        v = df_tmp.values.tolist()[0]
        # sql2 = " where {a} = {b}".format(a=col[0], b="'" + v[0] + "'")
        # if len(col) > 1:
        #     ss = []
        #     for jj in range(1, len(col)):
        #         str2 = " and {a} = {b} ".format(a=col[jj], b="'" + v[jj] + "'")
        #         # s = str2.replace("'", "")
        #         ss.append(str2)
        #     sss = ''.join(ss)
        #     sql2 = sql2 + sss
        # else:
        #     pass
        # sql2 = sql2 + ")"

        sql3 = " update [{BB}].[{db}].[{tb_name}] set ".format(
            BB=DD,
            db=usr,
            tb_name=tb_name)

        sql4 = " where {z} = {v}".format(z=z1, v="'" + df[z1][i] + "'")
        # sql4 = "{z} = {v}".format(z=zzz[0], v="'" + df[zzz[0]][i] + "'")
        if len(zhu) > 1:
            ff = []
            for tt in range(1, len(zhu)):
                strf = " and {a} = {b} ".format(a=zhu[tt], b="'" + df[zhu[tt]][i] + "'")
                ff.append(strf)
            fff = ''.join(ff)
            sql4 = sql4 + fff
        else:
            pass

        print(df_tmp)
        sql5 = " {a} = {b}".format(a=zzz[0], b="'" + df[zzz[0]][i] + "'")
        if len(zzz) > 1:
            xx = []
            for i in range(1, len(zzz)):
                str3 = " and {a} = {b} ".format(a=zzz[i], b="'" + df_tmp[zzz[i]][0] + "'")
                xx.append(str3)
            xxx = ''.join(xx)
            sql5 = sql5 + xxx
        else:
            pass

        sql_all = sql1 + sql4 + ")" + sql3 + sql5 + sql4
        sql_base = " else INSERT [{BB}].[{db}].[{tb_name}]{cols}".format(
            BB=DD,
            db=usr,
            tb_name=tb_name,
            cols=cols_str
        )
        sql_val = sql_cols(df_tmp, "format")
        vals = tuple([sql_val % x for x in df_tmp.to_dict("records")])
        sql_vals = "VALUES ({x})".format(x=vals[0])
        for i in range(1, len(vals)):
            sql_vals += ", ({x})".format(x=vals[i])
        sql_main = sql_base + " " + sql_vals
        sql_main = sql_all + sql_main

        sql_main = sql_main.replace("'None'", "NULL")
        try:
            print(sql_main)
            conn.execute(sql_main)
            co.commit()

        except pymssql.IntegrityError as e:
            print(e)
            co.commit()
            print("注意有错误~~~~~~~~~~~有错误~~~~~~~~~~~")


import pandas as pd

engine = pymssql.connect(host="10.1.5.121", user="dmp", password="123456", database="JYPRIME")

sql = 'SELECT * FROM [dbo].[cmdCONVDEF1]  where XBM = 5'

dataframe = pd.read_sql(sql, engine)
import numpy as np
import pandas as pd
dataframe["XBM"] = np.random.randint(12, 2)

# to_sql("JYPRIME", "dbo", "cmdCONVDEF1", engine, dataframe, type="update")
up_to_sql("JYPRIME", "dbo", "cmdCONVDEF1", engine, dataframe, zhu=['XBM'])


import numpy as np
import pandas as pd

dd = []
for i in range(1, 4):
    data = np.random.randint(1, 20, 2)
    dd.append(data)
df2 = pd.DataFrame(dd, columns=['ID', 'org'])

up_to_sql("JYPRIME", "dbo", "mixZSSQB_BAK", engine, df2, zhu=['ID', 'INBBM'])


