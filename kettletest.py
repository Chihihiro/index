"""
(公募)基金_信息,文本,费率,持有人

"""
import re
from utils.database import config as cfg, io
from utils.etlkit.core.base import Stream, Confluence
from utils.etlkit.core import transform
from utils.etlkit.reader.mysqlreader import MysqlInput
import time

import pymssql
import pandas as pd

engine1 = pymssql.connect(host="10.1.53.183", user="AppUser", password="AppUser123456", database="DCDB")
engine2 = pymssql.connect(host="10.1.5.121", user="dmp", password="123456", database="JYPRIME")

class StreamsMain:

    @classmethod
    def stream_1(cls):
        """
            清洗 d_org_info;

        """

        sql = " select top 10 ID,ZQDM,ZQJC,JYRQ,CJJG,CJL,CJJE,BUY,SELL,MTCC from dbo.webYXGCJTJXX"

        inp = pd.read_sql(sql, engine1)

        sk = transform.MapSelectKeys({
            "ID": "ID",
            'ZQDM': 'ZQDM',
            'ZQJC': 'ZQJC',
            'JYRQ': 'JYRQ',
            'CJJG': 'CJJG',
            'CJJE': 'CJJE',
            'SELL': 'SELL',
            'CJL': 'CJL',

        })
        s = Stream(inp, transform=[sk])
        return s

    @classmethod
    def stream_2(cls):
        """
            清洗 d_org_info;

        """

        sql = "select INBBM,IGSDM,GPDM from usrZQZB"

        inp = pd.read_sql(sql, engine2)

        sk = transform.MapSelectKeys({
            "INBBM": "INBBM",
            'IGSDM': 'IGSDM',
            'GPDM': 'ZQDM'
        })
        s = Stream(inp, transform=[sk])
        return s


    @classmethod
    def confluence(cls):
        a = StreamsMain.stream_1().flow()[0]
        b = StreamsMain.stream_2().flow()[0]
        result = pd.merge(a,b,how='left', on='ZQDM')
        return result

    @classmethod
    def stream_3(cls):
        """
            清洗 d_org_info;

        """

        vm = transform.ValueMap({
            'CJJE': lambda x: float(x) * 1000,
            'CJL': lambda x: float(x) * 1000,
            'GKBZ': 3,
            'XGRY': 1111,
            'XGRY2': 1111,
            'FBZT': 1,
            'FBSJ': time.strftime("%Y-%m-%d"),
            'XXFBSJ': '今天 '+ time.strftime("%H:%M:%S")
        })
        sk = transform.MapSelectKeys({
            "CJJE": "CJJE",
            'CJL': 'CJL',
            'GKBZ': 'GKBZ',
            'XGRY': 'XGRY',
            'XGRY2': 'XGRY2',
            'ID': 'ID',
            'ZQDM':'ZQDM',
            'ZQJC':'ZQJC',
            'JYRQ':'JYRQ',
            'INBBM':'INBBM',
            'FBZT':'FBZT',
            'FBSJ':'FBSJ',
            'XXFBSJ':'XXFBSJ'



        })
        s = Stream(cls.confluence(), transform=[vm,sk])
        return s



b = StreamsMain.stream_3().flow()[0]



import smtplib

