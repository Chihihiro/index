from xml.etree import ElementTree as ET
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
import jieba
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
            num = re.sub("\s|:|号码|\|", "", re.search(zhenze, x).group(1))
        except BaseException:
            num = ''
        else:
            pass
        return num

    def clean_sltime(self, x, zhenze):
        try:
            num = re.sub("\n|:|：|\|", "", re.search(zhenze, x).group(1))
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
                    tt = publish_Time.find('日')
                    publish_Time = publish_Time[0:tt + 1]
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
                tt = times.find('日')
                times = times[0:tt+1]
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
            print(a)
            if type(a) is str and len(a) >= 2:
                ll.append(a)
        if len(ll) >= 1:
            print('采集合集:', ll)
            # ll.sort(key=lambda i: len(i), reverse=True)
            return ll[0]
        else:
            return None

    def rell(self, list, ss):
        ll = []
        for i in list:
            a = self.clean(ss, "{}".format(i))
            print(a)
            if type(a) is str and len(a) >= 2:
                ll.append(a)
        if len(ll) >= 1:
            print('采集合集:', ll)
            # ll.sort(key=lambda i: len(i), reverse=True)
            return ll
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

    # def readPDF(self, path):
    #     # 以二进制形式打开pdf文件
    #     all = ''
    #     now = datetime.datetime.now()
    #     with open(path, "rb") as f:
    #         # 创建一个pdf文档分析器
    #         parser = PDFParser(f)
    #         # 创建pdf文档
    #         pdfFile = PDFDocument()
    #         # 链接分析器与文档对象
    #         parser.set_document(pdfFile)
    #         pdfFile.set_parser(parser)
    #         # 提供初始化密码
    #         pdfFile.initialize()
    #         # 检测文档是否提供txt转换
    #     if not pdfFile.is_extractable:
    #         raise PDFTextExtractionNotAllowed
    #     else:
    #         # 解析数据
    #         # 数据管理
    #         manager = PDFResourceManager()
    #         # 创建一个PDF设备对象
    #         laparams = LAParams(line_overlap=0.5, char_margin=60.0, line_margin=0.5, word_margin=0.1, boxes_flow=0.5,
    #                             detect_vertical=False, all_texts=False, paragraph_indent=None,
    #                             heuristic_word_margin=False)
    #         device = PDFPageAggregator(manager, laparams=laparams)
    #         # 解释器对象
    #         interpreter = PDFPageInterpreter(manager, device)
    #         ll = []
    #         # 开始循环处理，每次处理一页
    #         for page in pdfFile.get_pages():
    #             interpreter.process_page(page)
    #             layout = device.get_result()
    #
    #             for x in layout:
    #                 try:
    #                     if (isinstance(x, LTTextBoxHorizontal)):
    #                         # with open(toPath, "a") as f:
    #                             str = x.get_text()
    #                             ll.append(strQ2B(str))
    #                             # print(str)
    #                             all += str
    #                             # f.write(str)
    #                 except UnicodeEncodeError:
    #                     pass
    #                 else:
    #                     pass
    #         after = datetime.datetime.now()
    #         print('本次解析时间为：')
    #         print(after - now)
    #     return ll, all

    def read_xml(self, path):
        print(path)
        tree = ET.parse(path)
        root = tree.getroot()
        # root = ET.fromstring(country_data_as_string) #通过字符串导入,直接获取根
        childs = root.getchildren()

        books = []
        all = ""
        for child0 in childs:
            # book = {}
            for child00 in child0.getchildren():
                # print child00.tag #标签名，即name、date、price、description
                # print child00.text
                bb = child00.text
                books.append("\n" + re.sub("\|", "", strQ2B(bb)) + "\n")
                all += re.sub("\|", "", strQ2B(bb))

                for p in child00:
                    t = p.text
                    books.append("\n" + t + "\n")
                    all += "\n" + t + "\n"

        return books, all

    def now_time(self, a=0):
        now = datetime.datetime.now()
        delta = datetime.timedelta(days=a)
        n_days = now + delta
        print(n_days.strftime('%Y-%m-%d %H:%M:%S'))
        f = n_days.strftime('%Y%m%d%H%M%S')
        return f

    def flow(self):
        Z1, Z2 = self.read_xml(self.path)
        all = strQ2B(Z2)
        df = pd.DataFrame(Z1)

        must = ['概况',
                '公司基本情况',
                '发行人简介',
                '发行人基本情况',
                '发行人基本信息',
                '概览', '基本情况', '公司简介']

        nm = ['注册号', '营业执照']

        dict1 = {'概况': '概况',
                 '概览': '概览',
                 '公司基本情况': '公司基本情况',
                 '发行人简介': '发行人简介',
                 '公司简介': '公司简介',
                 '基本情况':  '基本情况',
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
                 '电话 ': 'LXRDH',
                 '传真': 'LXRCZ',
                 '传真号码': 'LXRCZ',
                 '电子邮箱': 'LXRDZYJ',
                 '电子信箱': 'LXRDZYJ',
                 '证券事务代表': 'ZQSWDB',
                 '股证事物代表': 'ZQSWDB',
                 '注册地址': '注册地址',
                 '公司住所': 'GSZCDZ',
                 '公司注册地址': 'GSZCDZ',
                 '通信地址': 'GSBGDZ',
                 '办公地址': 'GSBGDZ',
                 '联系地址': 'GSBGDZ',
                 '住所': '住所',
                 '邮政编码': 'GSZCDZYB',
                 '经营范围': '经营范围',
                 '网址': 'GSWZ'}

        self.v = list(dict1.keys())
        df = df.reset_index(level=0)
        df.columns = ["SUM", "A"]

        def zero2(strs):
            s = 0
            for i in nm:
                if i in strs:
                    s += 1
            return s

        df["B"] = df["A"].apply(lambda x: self.zero(x) if type(x) is str else 0)
        df["C"] = (df["B"].shift(3) + df["B"].shift(2) + df["B"].shift(1) + df["B"].shift(-3) +
                   df["B"].shift(-2) + df["B"].shift(-1) + df["B"])
        df["映射"] = list(map(lambda x, y: self.havestr(self.v, x) if y >= 1 else None, df["A"], df["B"]))

        llmax = list(set(df["C"].tolist()))
        llmax = [int(i) for i in llmax if i >= 3]
        df['D'] = list(map(lambda x, y: x if y in llmax else None, df['SUM'], df['C']))
        p = list(set(df["D"].tolist()))
        p = [int(i) for i in p if type(i) is float and i > 0]
        print(p)




        """取坐标公司基本情况"""
        size = 12
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
        llls = []
        for ls in ps:
            ko = []
            for i in ls:
                a = df.loc[i]["映射"]
                if type(a) is list:
                    for z in a:
                        ko.append(z)
            sum = list(set(ko))
            llls.append(sum)

        last = []
        for i in range(len(ps)):
            jj = llls[i]
            num = ps[i][0]
            # num2 = ps[i][-1]
            print(num)
            jj.append(num)
            # jj.append(num2)
            if len(jj) >= 4:
                last.append(jj)
        last.sort(key=lambda i: len(i), reverse=True)
        must_list = []
        for ll in last:
            for i in ll:
                if i in must:
                    must_list.append(ll)
                    break
        n = must_list[:2]



        # n = self.get_list(p, df)
        # print(n)

        if len(n) >= 2:
            n1 = n[0][-1]
            top1 = n1 - 5
            last1 = n1 + 25

            n2 = n[1][-1]
            top2 = n2 - 5
            last2 = n2 + 25

            df1 = df.iloc[int(top1):int(last1), [1]]
            df2 = df.iloc[int(top2):int(last2), [1]]

            t1 = df1["A"].tolist()
            ss1 = strQ2B(''.join(t1))
            if '...............' in ss1:
                ss1 = ''

            t2 = df2["A"].tolist()
            ss2 = strQ2B(''.join(t2))
            if '...............' in ss2:
                ss2 = ''
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

        serch = {'GSCLRQ': ["成立时间及股份公司设立时间(.+?)\\n", "成立时间(.+?)\\n", "成立日期(.+?)\\n", "公司设立日期(.+?)\\n"],
                 'BGSLRQ': ["成立时间及股份公司设立时间(.+?)\\n", "股份[\u4e00-\u9fa5]+日期(.+?)\\n", "设立日期(.+?)\\n",
                            "整体变更日期(.+?)\\n"],
                 'SCZCDJDD': 'SCZCDJDD',
                 'FRDB': ["\\n法定代表人(.+?)\\n", "\\n法人代表(.+?)\\n", "法人(.+?)\\n"],
                 'DSHMS': ["\\n信息披露负责人(.+?)\\n", "\\n董事会秘书(.+?)\\n"],
                 'LXRDH': ["联系电话(.+?)\\n", "电话号码(.+?)\\n", '联系人电话(.+?)\\n', '电话(.+?)\\n'],
                 'LXRCZ': ["传真号码(.+?)\\n", "传真(.+?)\\n"],
                 'LXRDZYJ': ["电子信箱(.+?)\\n", "电子邮箱(.+?)\\n"],
                 'DMLXDH': 'DMLXDH',
                 'DMCZ': 'DMCZ',
                 'DMDZYJ': 'DMDZYJ',
                 'ZQSWDB': ["证券事务代表(.+?)\\n", "股证事务代表(.+?)\\n"],
                 'ZQSWDBDH': 'ZQSWDBDH',
                 'ZQSWDBCZ': 'ZQSWDBCZ',
                 'ZQSWDBDZYJ': 'ZQSWDBDZYJ',
                 'GSZCDZ': ["公司住所(.+?)\\n", "公司注册地址(.+?)\\n", "住所(.+?)\\n", "办公地址(.+?)\\n"],
                 'GSBGDZ': 'GSBGDZ',
                 'GSZCDZYB': ["邮政编码(.+?)\\n", "邮编(.+?)\\n"],
                 'GSWZ': ["网址(.+?)\\n", "互联网地址(.+?)\\n"]}


        GSCLRQ = self.relist(serch.get('GSCLRQ'), ss3)
        if type(GSCLRQ) is str and '年' in GSCLRQ:
            GSCLRQ = self.strtime(GSCLRQ)

        if GSCLRQ is None:
            GSCLRQ = self.strtime(self.rell(serch.get('GSCLRQ'), ss3))


        try:
            if GSCLRQ is None:
                c1 = self.relist(serch.get('GSCLRQ'), ss3)
                c2 = time.strptime(c1, u"%Y年%m月%d日")
                GSCLRQ = time.strftime("%Y-%m-%d", c2)
        except BaseException:
            pass

        BGSLRQ = self.relist(serch.get('BGSLRQ'), ss3)
        if BGSLRQ is not None and '年' in BGSLRQ:
            BGSLRQ = self.strtime(BGSLRQ)

        if BGSLRQ is None:
            BGSLRQ = self.strtime(self.rell(serch.get('BGSLRQ'), ss3))

        SCZCDJDD = None







        try:
            ztime = []
            cltime = self.relist(serch.get('GSCLRQ'), ss3)
            if '日' in cltime:
                tt = cltime.find('日')
                cltime = cltime[0:tt + 1]

            ztime.append(cltime)

            if type(cltime) is str and '年' not in cltime:
                import datetime
                d4 = datetime.datetime.strptime(cltime, '%Y-%m-%d')
                d5 = d4.timetuple()
                V = datetime.datetime.strftime(d4, "%Y{}%m{}%d{}").format('年', '月', '日')
                Ver = str(d5.tm_year) + '{}' + str(d5.tm_mon) + '{}' + str(d5.tm_mday) + '{}'
                cltime = Ver.format('年', '月', '日')
                ztime.append(cltime)
                ztime.append(V)


            nm.append(cltime)

            df["X"] = df["A"].apply(lambda x: zero2(x) if type(x) is str else 0)
            df["Y"] = (df["X"].shift(3) + df["X"].shift(2) + df["X"].shift(1) + df["X"].shift(-3) +
                       df["X"].shift(-2) + df["X"].shift(-1) + df["X"])
            df["文映射"] = list(map(lambda x, y: self.havestr(nm, x) if y >= 1 else None, df["A"], df["X"]))
            wenmax = list(set(df["Y"].tolist()))
            wenmax = [int(i) for i in wenmax if i >= 2]
            df['Z'] = list(map(lambda x, y: x if y in wenmax else None, df['SUM'], df['Y']))
            np = list(set(df["Z"].tolist()))



            def get_wens(np, df, must, size=2):

                ps = []
                p1 = []
                for i in range(len(np)):
                    if i == (len(np) - 1):
                        p1.append(np[i])
                        ps.append(p1)
                    else:
                        if (np[i + 1] - np[i]) < size:
                            p1.append(np[i])
                        else:
                            p1.append(np[i])
                            new = p1.copy()
                            ps.append(new)
                            p1.clear()
                    # ps.sort(key=lambda i: len(i), reverse=True)
                    # lps = ps[:2]
                    # return lps
                llls = []
                for ls in ps:
                    ko = []
                    for i in ls:
                        a = df.loc[i]["文映射"]
                        if type(a) is list:
                            for z in a:
                                ko.append(z)
                    sum = list(set(ko))
                    llls.append(sum)

                last = []
                for i in range(len(ps)):
                    jj = llls[i]
                    num = ps[i][0]
                    # num2 = ps[i][-1]
                    print(num)
                    jj.append(num)
                    # jj.append(num2)
                    if len(jj) >= 2:
                        last.append(jj)
                last.sort(key=lambda i: len(i), reverse=True)
                must_list = []
                for ll in last:
                    for i in ll:
                        if i in must:
                            must_list.append(ll)
                            break
                return must_list[0]


            np = [int(i) for i in np if type(i) is float and i > 0]
            T = get_wens(np, df, ztime)
            dn = T[-1]
            dl = dn + 9
            df3 = df.iloc[int(dn):int(dl), [1]]
            ww = df3["A"].tolist()
            wens = ''.join(ww)
            wen = re.sub('\n|\|', "", wens)



            if '营业执照' in wen:
                x = re.search("{}(.+?)营业执照".format(cltime), wen).group(1)
                nextx = re.search("{}(.+?)。".format(x[-10:]), wen).group(1)
                # wen = cltime + x + '营业执照' + nextx + '。'
                wen = cltime + x + nextx + '。'
                seg_list = jieba.cut(x, cut_all=False)
                a = list(seg_list)
                jz = []
                j = []
                for i in range(len(a)):
                    if len(j) == 2:
                        break
                    if '局' in a[i]:
                        jz.append(a[i-1])
                        jz.append(a[i])
                        j.append(a[i])
                SCZCDJDD = ''.join(jz)
        except BaseException:
            wen = None


        # if wen is None:
        #     yl = [i.start() for i in re.finditer('营业执照', all)]
        #     chen = []
        #     for y in yl:
        #         # print(type(y))
        #         w = all[y - 200:y + 200]
        #         if cltime in w:
        #             print('you')


        GSJK = wen



        FRDB = self.relist(serch.get('FRDB'), ss3)
        DSHMS = self.relist(serch.get('DSHMS'), ss3)
        if type(DSHMS) is str and len(DSHMS) > 4:
            DSHMS = None
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
        if 'GSCLRQ' in dffff.columns:
            dffff['GSCLRQ'] = dffff['GSCLRQ'].apply(lambda x: x if type(x) is str and len(x) == 10 else None)
        if 'BGSLRQ' in dffff.columns:
            dffff['BGSLRQ'] = dffff['BGSLRQ'].apply(lambda x: x if type(x) is str and len(x) == 10 else None)

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




# if __name__ == '__main__':
#     path = 'C:\\Users\\qinxd\\Desktop\\to2'
#     files = []
#     for i in os.listdir(path):
#         pp = r"{path}\{i}".format(path=path, i=i)
#         print(pp)
#         files.append(pp)
#
#     STEP = 1
#     sliced = [files[i: i+STEP] for i in range(0, len(files), STEP)]
#     print(sliced)
#     pool = ThreadPool(2)
#     p2 = Pool(2)
#     result = pool.map(clean_pdf.tool_to_db, sliced)


if __name__ == '__main__':

    path = 'C:\\Users\\qinxd\\Desktop\\to2\\12018-05-04 湖北五方光电股份有限公司首次公开发行股票招股说明书（申报稿2018年4月27日报送）.pdf.xml'
    # path = 'C:\\Users\\qinxd\\Desktop\\to2\\12018-04-12 鞍山七彩化学股份有限公司创业板首次公开发行股票招股说明书（申报稿2018年3月30日报送）.pdf.xml'

    clean_pdf.to_db(path)






# df.to_csv(r'C:\Users\qinxd\Desktop\xml1111.csv', encoding='utf_8_sig')


