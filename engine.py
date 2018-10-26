import pandas as pd
import pymssql
from sqlalchemy import create_engine


engine = pymssql.connect(host="10.1.5.121", user="dmp", password="123456", database="JYPRIME")
en = engine.cursor()
sql = 'SELECT * FROM [dbo].[cmdCONVDEF1]'

df = pd.read_sql(sql, engine)

df["XBM"] = 3

from mytosql import to_sql

df = to_sql("cmdCONVDEF1","dbo", engine, df, type="update")


# sql2 = "INSERT INTO [JYPRIME].[dbo].[cmdCONVDEF1] ([ZHLJM], [XBM], [XBZDXH], [SZJM], [WBYS], [WBYSJM], [XBZDM], [YBM], [YBZDM], [SCTJ]) VALUES ('1', '3', '1', '1', '1', '1', '1', '1', '1', '1')"
#
# en.execute(sql2)
# engine.commit()


sql3 = "INSERT INTO [JYPRIME].[dbo].[cmdCONVDEF1] ([ZHLJM], [XBM], [XBZDXH], [SZJM], [WBYS], [WBYSJM], [XBZDM], [YBM], [YBZDM], [SCTJ]) VALUES ('1', '3', '1', '1', '1', '1', '1', '2', '1', '1')," \
       "('1', '5', '1', '1', '1', '1', '1', '1', '1', '2')"

en.execute(sql3)
engine.commit()
