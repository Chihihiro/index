import sys
import re
import datetime
import pandas as pd
import os
from sqlalchemy import create_engine
import pymysql
import importlib
import time
importlib.reload(sys)
from pdfminer.pdfparser import PDFParser, PDFDocument
from pdfminer.pdfinterp import PDFResourceManager, PDFPageInterpreter
from pdfminer.converter import PDFPageAggregator
from pdfminer.layout import LTTextBoxHorizontal, LAParams
from pdfminer.pdfinterp import PDFTextExtractionNotAllowed
from multiprocessing.dummy import Pool as ThreadPool
from multiprocessing import Pool



engine = create_engine(
    "mysql+pymysql://{}:{}@{}:{}/{}".format('root', 'Chihiro123+', '10.3.2.25', 3306, 'base', ),
    connect_args={"charset": "utf8"}, echo=True, )


def strQ2B(ustring):
    """全角转半角"""
    rstring = ""
    for uchar in ustring:
        inside_code = ord(uchar)
        if inside_code == 12288:  # 全角空格直接转换
            inside_code = 32
        elif (inside_code >= 65281 and inside_code <= 65374):  # 全角字符（除空格）根据关系转化
            inside_code -= 65248

        rstring = rstring + chr(inside_code)
    return rstring


class clean_pdf():
    def __init__(self, path):
        self.path = path
        self.v = []

    def clean(self, x, zhenze):
        try:
            num = re.sub("\s|:|号码", "", re.search(zhenze, x).group(1))
        except BaseException:
            num = ''
        else:
            pass
        return num

    def clean_sltime(self, x, zhenze):
        try:
            num = re.sub("\n|:|：", "", re.search(zhenze, x).group(1))
        except BaseException:
            num = ''
        else:
            pass
        return num


    def retime(self,list, ss):
        ll = []
        for i in list:
            a = self.clean_sltime(ss, "{}".format(i))
            if type(a) is str and len(a) > 0:
                ll.append(a)
        if len(ll) >= 1:
            ll.sort(key=lambda i: len(i), reverse=True)
            print('采集合集:', ll)
            return ll[0]
        else:
            return None


    def strtime(self, times):
        ll = []
        if type(times) is list:

            for publish_Time in times:
                try:
                    array = time.strptime(publish_Time, u"%Y年%m月%d日")
                    publishTime = time.strftime("%Y-%m-%d", array)
                    ll.append(publishTime)
                except BaseException:
                    pass
            if len(ll) >= 1:
                return ll[0]
            else:
                return None
        elif type(times) is str:
            try:
                array = time.strptime(times, u"%Y年%m月%d日")
                publishTime = time.strftime("%Y-%m-%d", array)
                return publishTime
            except BaseException:
                return None
        else:
            return None


    def relist(self, list, ss):
        ll = []
        for i in list:
            a = self.clean(ss, "{}".format(i))
            if type(a) is str and len(a) >=2:
                ll.append(a)
        if len(ll) >= 1:
            print('采集合集:', ll)
            # ll.sort(key=lambda i: len(i), reverse=True)
            return ll[0]
        else:
            return None

    def zero(self, strs):
        s = 0
        for i in self.v:
            if i in strs:
                s += 1
        return s

    def havestr(self, lls, strs):
        have = []
        for i in lls:
            if i in strs:
                have.append(i)
        ll = list(set(have))
        if len(ll) > 0:
            return ll
        else:
            return None

    def get_list(self, p, dataframe, size=5):
        ps = []
        p1 = []
        for i in range(len(p)):
            if i == (len(p) - 1):
                p1.append(p[i])
                ps.append(p1)
            else:
                if (p[i + 1] - p[i]) < size:
                    p1.append(p[i])
                else:
                    p1.append(p[i])
                    new = p1.copy()
                    ps.append(new)
                    p1.clear()
            # ps.sort(key=lambda i: len(i), reverse=True)
            # lps = ps[:2]
            # return lps
        lls = []
        for ls in ps:
            ko = []
            for i in ls:
                a = dataframe.loc[i]["映射"]
                if type(a) is list:
                    for z in a:
                        ko.append(z)
            sum = list(set(ko))
            lls.append(sum)

        last = []
        for i in range(len(ps)):
            jj = lls[i]
            num = ps[i][0]
            # num2 = ps[i][-1]
            print(num)
            jj.append(num)
            # jj.append(num2)
            last.append(jj)
        last.sort(key=lambda i: len(i), reverse=True)
        lps = last[:2]
        return lps

    def readPDF(self, path):
        # 以二进制形式打开pdf文件
        all = ''
        now = datetime.datetime.now()
        with open(path, "rb") as f:
            # 创建一个pdf文档分析器
            parser = PDFParser(f)
            # 创建pdf文档
            pdfFile = PDFDocument()
            # 链接分析器与文档对象
            parser.set_document(pdfFile)
            pdfFile.set_parser(parser)
            # 提供初始化密码
            pdfFile.initialize()
            # 检测文档是否提供txt转换
        if not pdfFile.is_extractable:
            raise PDFTextExtractionNotAllowed
        else:
            # 解析数据
            # 数据管理
            manager = PDFResourceManager()
            # 创建一个PDF设备对象
            laparams = LAParams(line_overlap=0.5, char_margin=60.0, line_margin=0.5, word_margin=0.1, boxes_flow=0.5,
                                detect_vertical=False, all_texts=False, paragraph_indent=None,
                                heuristic_word_margin=False)
            device = PDFPageAggregator(manager, laparams=laparams)
            # 解释器对象
            interpreter = PDFPageInterpreter(manager, device)
            ll = []
            # 开始循环处理，每次处理一页
            for page in pdfFile.get_pages():
                interpreter.process_page(page)
                layout = device.get_result()

                for x in layout:
                    try:
                        if (isinstance(x, LTTextBoxHorizontal)):
                            # with open(toPath, "a") as f:
                                str = x.get_text()
                                ll.append(strQ2B(str))
                                # print(str)
                                all += str
                                # f.write(str)
                    except UnicodeEncodeError:
                        pass
                    else:
                        pass
            after = datetime.datetime.now()
            print('本次解析时间为：')
            print(after - now)
        return ll, all

    def now_time(self, a=0):
        now = datetime.datetime.now()
        delta = datetime.timedelta(days=a)
        n_days = now + delta
        print(n_days.strftime('%Y-%m-%d %H:%M:%S'))
        f = n_days.strftime('%Y%m%d%H%M%S')
        return f

    def flow(self):
        Z1, Z2 = self.readPDF(self.path)
        all = strQ2B(Z2)
        df = pd.DataFrame(Z1)

        dict1 = {'概况': '概况',
                 '公司基本情况': '公司基本情况',
                 '发行人简介': '发行人简介',
                 '发行人基本情况': '发行人基本情况',
                 '发行人基本信息': '发行人基本信息',
                 '中文名称': '中文名称',
                 '英文名称': '英文名称',
                 '注册资本': '注册资本',
                 '成立日期': 'IGSDM',
                 '公司设立日期': 'IGSDM',
                 '设立日期': 'BGSLRQ',
                 '整体变更日期 ':'BGSLRQ',
                 '法定代表人': 'FRDB',
                 '法人': 'FRDB',
                 '披露负责人': 'DSHMS',
                 '董事会秘书': 'DSHMS',
                 '联系电话': 'LXRDH',
                 '电    话 ': 'LXRDH',
                 '传    真': 'LXRCZ',
                 '传真号码': 'LXRCZ',
                 '电子邮箱': 'LXRDZYJ',
                 '电子信箱': 'LXRDZYJ',
                 '证券事务代表': 'ZQSWDB',
                 '股证事物代表': 'ZQSWDB',
                 '公司住所': 'GSZCDZ',
                 '公司注册地址': 'GSZCDZ',
                 '通信地址': 'GSBGDZ',
                 '办公地址': 'GSBGDZ',
                 '联系地址': 'GSBGDZ',
                 '邮政编码': 'GSZCDZYB',
                 '经营范围': '经营范围',
                 '网址': 'GSWZ'}

        self.v = list(dict1.keys())
        df = df.reset_index(level=0)
        df.columns = ["SUM", "A"]

        df["B"] = df["A"].apply(lambda x: self.zero(x) if type(x) is str else 0)
        df["C"] = (df["B"].shift(3) + df["B"].shift(2) + df["B"].shift(1) + df["B"].shift(-3) +
                        df["B"].shift(-2) + df["B"].shift(-1) + df["B"])
        df["映射"] = list(map(lambda x, y: self.havestr(self.v, x) if y >= 1 else None, df["A"], df["B"]))
        llmax = list(set(df["C"].tolist()))
        llmax = [int(i) for i in llmax if i >= 3]
        df['D'] = list(map(lambda x, y: x if y in llmax else None, df['SUM'], df['C']))
        p = list(set(df["D"].tolist()))
        p = [int(i) for i in p if i > 0]
        n = self.get_list(p, df)


        if len(n)>=2:
            n1 = n[0][-1]
            top1 = n1 - 5
            last1 = n1 + 15

            n2 = n[1][-1]
            top2 = n2 - 5
            last2 = n2 + 15

            df1 = df.iloc[int(top1):int(last1), [1]]
            df2 = df.iloc[int(top2):int(last2), [1]]

            t1 = df1["A"].tolist()
            ss1 = strQ2B(''.join(t1))

            t2 = df2["A"].tolist()
            ss2 = strQ2B(''.join(t2))
            ss3 = ss1 + ss2
        elif len(n) == 1:
            n1 = n[0][-1]
            top1 = n1 - 5
            last1 = n1 + 15
            df1 = df.iloc[int(top1):int(last1), [1]]
            t1 = df1["A"].tolist()
            ss3 = strQ2B(''.join(t1))
        else:
            ss3 = ''

        serch = {'GSCLRQ': ["成立时间(.+?)\\n", "成立日期(.+?)\\n", "公司设立日期(.+?)\\n"],
                 'BGSLRQ': ["股份[\u4e00-\u9fa5]+设立日期(.+?)\\n", "设立日期(.+?)\\n",
                            "股份[\u4e00-\u9fa5]+(.+?)日期(.+?)\\n", "整体变更日期(.+?)\\n"],
                 'SCZCDJDD': 'SCZCDJDD',
                 'FRDB': ["\\n法定代表人(.+?)\\n", "\\n法人代表(.+?)\\n", "法人(.+?)\\n"],
                 'DSHMS': ["\\n信息披露负责人(.+?)\\n", "\\n董事会秘书(.+?)\\n"],
                 'LXRDH': ["联系电话(.+?)\\n", "电话号码(.+?)\\n", '联系人电话(.+?)\\n',
                           '电\\s+话(.+?)\\n', '电话(.+?)\\n'],
                 'LXRCZ': ["传真号码(.+?)\n", "传\\s+真(.+?)\\n", "传真(.+?)\\n"],
                 'LXRDZYJ': ["电子信箱(.+?)\\n", "电子邮箱(.+?)\\n"],
                 'DMLXDH': 'DMLXDH',
                 'DMCZ': 'DMCZ',
                 'DMDZYJ': 'DMDZYJ',
                 'ZQSWDB': ["证券事务代表(.+?)\\n", "股证事务代表(.+?)\\n"],
                 'ZQSWDBDH': 'ZQSWDBDH',
                 'ZQSWDBCZ': 'ZQSWDBCZ',
                 'ZQSWDBDZYJ': 'ZQSWDBDZYJ',
                 'GSZCDZ': ["公司住所(.+?)\\n", "公司注册地址(.+?)\\n", "住\\s+所(.+?)\\n"],
                 'GSBGDZ': 'GSBGDZ',
                 'GSZCDZYB': ["邮政编码(.+?)\n", "邮\\s+编(.+?)\n"],
                 'GSWZ': ["网址(.+?)\\n", "互联网地址(.+?)\\n"]}


        GSCLRQ = self.strtime(self.relist(serch.get('GSCLRQ'), ss3))

        try:
            if GSCLRQ is None:
                c1 = self.relist(serch.get('GSCLRQ'), ss3)
                c2 = time.strptime(c1, u"%Y年%m月%d日")
                GSCLRQ = time.strftime("%Y-%m-%d", c2)
        except BaseException:
            pass

        BGSLRQ = self.strtime(self.relist(serch.get('BGSLRQ'), ss3))

        SCZCDJDD = None
        try:
            c1 = self.retime(serch.get('GSCLRQ'), ss3)
            if c1[0] == " ":
                c1 = "\n" + c1[1:]
            if c1[-1] == " ":
                c1 = c1[:-2] + "\n"

            # c2 = time.strptime(c1, u"%Y年%m月%d日")
            T = c1.replace("年", " 年 ")
            T = T.replace("月", " 月 ")
            T = T.replace("日", " 日")
            IGSDM_ZW = T
            # IGSDM_ZW = re.search("成立日期(.+?)\n", ss3).group(1)[1:-1]

            demo = [r"\n{}\,[\u4e00-\u9fa5]+在(.+?)完成设立登记".format(IGSDM_ZW),
                    r"\n{}\,[\u4e00-\u9fa5]+在(.+?)注册登记".format(IGSDM_ZW),
                    r"\n{}\,[\u4e00-\u9fa5]+在(.+?分局)".format(IGSDM_ZW),
                    r"\n{}\,[\u4e00-\u9fa5]+在(.+?局)".format(IGSDM_ZW),
                    r"\n{}\,[\u4e00-\u9fa5]+取得了(.+?局)".format(IGSDM_ZW),
                    r"\n{}\,[\u4e00-\u9fa5]+从(.+?局)".format(IGSDM_ZW)]
            SCZCDJDD = self.relist(demo, all)
            txts = re.findall("{a}\,[\u4e00-\u9fa5]+{b}".format(a=IGSDM_ZW, b=SCZCDJDD), all)
            df["定位"] = list(map(lambda x, y: x if type(y) is str and txts[0] in y else None, df["SUM"], df["A"]))

            din = list(set(df["定位"].tolist()))
            din = [int(i) for i in din if i >= 3]

            a1 = re.sub("\s|\n", "", df.loc[din[0]]['A'])
            a2 = re.sub("\s|\n", "", df.loc[din[0] + 1]['A'])
            a3 = re.sub("\s|\n", "", df.loc[din[0] + 2]['A'])
            a4 = re.sub("\s|\n", "", df.loc[din[0] + 3]['A'])
            a5 = re.sub("\s|\n", "", df.loc[din[0] + 4]['A'])
            aa = [a1, a2, a3, a4, a5]
            bb = [a1, a2, a3, a4, a5]
            kk = []
            for i in range(4):
                txt = aa[i]
                if "申报稿" in txt:
                    bb[i] = ""
                    bb[i - 1] = ""
                if "。" in txt:
                    kk.append(i)

            tts = "".join(bb[:kk[0] + 1])
            wen = tts.split('。')[0] + '。'

        except BaseException:
            wen = None


        GSJK = wen



        FRDB = self.relist(serch.get('FRDB'), ss3)
        DSHMS = self.relist(serch.get('DSHMS'), ss3)
        LXRDH = self.relist(serch.get('LXRDH'), ss3)
        LXRCZ = self.relist(serch.get('LXRCZ'), ss3)
        LXRDZYJ = self.relist(serch.get('LXRDZYJ'), ss3)

        """
        如果董事会秘书存在赋值给董秘
        """
        DMLXDH = None
        DMCZ = None
        DMDZYJ = None
        if type(DSHMS) is str and len(DSHMS) >= 2:
            DMLXDH = LXRDH
            DMCZ = LXRCZ
            DMDZYJ = LXRDZYJ

        ZQSWDB = self.relist(serch.get('ZQSWDB'), ss3)

        ZQSWDBDH = None
        ZQSWDBCZ = None
        ZQSWDBDZYJ = None
        if type(ZQSWDB) is str and len(ZQSWDB) >= 0:
            ZQSWDBDH = LXRDH
            ZQSWDBCZ = LXRCZ
            ZQSWDBDZYJ = LXRDZYJ

        GSZCDZ = self.relist(serch.get('GSZCDZ'), ss3)

        GSBGDZ = self.relist(serch.get('GSBGDZ'), ss3)

        GSLXDZ = GSBGDZ

        GSZCDZYB = self.relist(serch.get('GSZCDZYB'), ss3)

        GSBGDZYB = None
        GSLXDZYB = None
        if GSZCDZ == GSBGDZ:
            GSBGDZYB = GSZCDZ
            GSLXDZYB = GSZCDZYB
        GSWZ = self.relist(serch.get('GSWZ'), ss3)

        print('FRDB:', FRDB, '\n', 'DSHMS:', DSHMS, '\n', 'LXRDH:', LXRDH, '\n',
              'LXRCZ:', LXRCZ, '\n', 'LXRDZYJ:', LXRDZYJ, '\n', 'GSJK:', GSJK, '\n',
              'GSZCDZ:', GSZCDZ, '\n', 'GSBGDZ:', GSBGDZ, '\n', 'GSZCDZYB:', GSZCDZYB, '\n',
              'GSCLRQ:', GSCLRQ, '\n')

        IGSDM = re.search("to2\\\\(.+?)\.pdf", self.path).group(1)
        dff = {'IGSDM': IGSDM,
               'GSCLRQ': GSCLRQ,
               'BGSLRQ': BGSLRQ,
               'SCZCDJDD': SCZCDJDD,
               'FRDB': FRDB,
               'DSHMS': DSHMS,
               'LXRDH': LXRDH,
               'LXRCZ': LXRCZ,
               'LXRDZYJ': LXRDZYJ,
               'DMLXDH': DMLXDH,
               'DMCZ': DMCZ,
               'DMDZYJ': DMDZYJ,
               'ZQSWDB': ZQSWDB,
               'ZQSWDBDH': ZQSWDBDH,
               'ZQSWDBCZ': ZQSWDBCZ,
               'ZQSWDBDZYJ': ZQSWDBDZYJ,
               'GSZCDZ': GSZCDZ,
               'GSBGDZ': GSBGDZ,
               'GSLXDZ': GSLXDZ,
               'GSZCDZYB': GSZCDZYB,
               'GSBGDZYB': GSBGDZYB,
               'GSLXDZYB': GSLXDZYB,
               'GSWZ': GSWZ,
               'GSJK': GSJK}

        dffff = pd.DataFrame([dict(filter(lambda x: x[1] not in [None, ''], dff.items()))])
        if 'FRDB' in dffff.columns:
            dffff['FRDB'] = dffff['FRDB'].apply(lambda x: x if type(x) is str and 4 >= len(x) >= 2 else None)
        return dffff

    @classmethod
    def tool_to_db(cls, path):
        """path is list"""
        dff = clean_pdf(path[0]).flow()
        to_sql('test', engine, dff, type='update')

    @classmethod
    def to_db(cls, path):
        dff = clean_pdf(path).flow()
        to_sql('test', engine, dff, type='update')





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


def to_sql(tb_name, conn, dataframe, type="update", chunksize=2000, debug=False):
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
        type: str, optional {"update", "replace", "ignore"}, default "update"
            Specified the way to update get_data. If "update", then `conn` will execute "INSERT ... ON DUPLICATE UPDATE ..."
            SQL statement, else if "replace" chosen, then "REPLACE ..." SQL statement will be executed; else if "ignore" chosen,
            then "INSERT IGNORE ..." will be excuted;
        chunksize: int
            Size of records to be inserted each time;
        **kwargs:

    Returns:
        None
    """

    df = dataframe.copy(deep=False)
    df = df.fillna("None")
    df = df.applymap(lambda x: re.sub('([\'\"\\\])', '\\\\\g<1>', str(x)))
    cols_str = sql_cols(df)
    sqls = []
    for i in range(0, len(df), chunksize):
        # print("chunk-{no}, size-{size}".format(no=str(i/chunksize), size=chunksize))
        df_tmp = df[i: i + chunksize]

        if type == "replace":
            sql_base = "REPLACE INTO `{tb_name}` {cols}".format(
                tb_name=tb_name,
                cols=cols_str
            )

        elif type == "update":
            sql_base = "INSERT INTO `{tb_name}` {cols}".format(
                tb_name=tb_name,
                cols=cols_str
            )
            sql_update = "ON DUPLICATE KEY UPDATE {0}".format(
                sql_cols(df_tmp, "values")
            )

        elif type == "ignore":
            sql_base = "INSERT IGNORE INTO `{tb_name}` {cols}".format(
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
        if type == "update":
            sql_main += sql_update

        if sys.version_info.major == 2:
            sql_main = sql_main.replace("u`", "`")
        if sys.version_info.major == 3:
            sql_main = sql_main.replace("%", "%%")

        if debug is False:
            try:
                conn.execute(sql_main)
            except pymysql.err.InternalError as e:
                print("ENCOUNTERING ERROR: {e}, RETRYING".format(e=e))
                time.sleep(10)
                conn.execute(sql_main)
        else:
            sqls.append(sql_main)
    if debug:
        return sqls







# path = r'C:\Users\qinxd\Desktop\p23.pdf'
# clean_pdf.to_db(path)




if __name__ == '__main__':
    path = 'C:\\Users\\qinxd\\Desktop\\to2'
    files = []
    for i in os.listdir(path):
        pp = r"{path}\{i}".format(path=path, i=i)
        print(pp)
        files.append(pp)

    STEP = 1
    sliced = [files[i: i+STEP] for i in range(0, len(files), STEP)]
    print(sliced)
    pool = ThreadPool(2)
    p2 = Pool(2)
    result = pool.map(clean_pdf.tool_to_db, sliced)



# path = r'C:\Users\qinxd\Desktop\p23.pdf'
# toPath = r'C:\Users\qinxd\Desktop\p23.txt'
# a.to_csv(r'C:\Users\qinxd\Desktop\test14.csv', encoding='utf_8_sig')



