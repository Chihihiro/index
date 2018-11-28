import sys
import re
import importlib
import time

importlib.reload(sys)
from pdfminer.pdfparser import PDFParser, PDFDocument
from pdfminer.pdfinterp import PDFResourceManager, PDFPageInterpreter
from pdfminer.converter import PDFPageAggregator
from pdfminer.layout import LTTextBoxHorizontal, LAParams
from pdfminer.pdfinterp import PDFTextExtractionNotAllowed
import datetime
import pandas as pd


class clean_pdf():
    def __init__(self, path, topath):
        Z1, Z2 = self.readPDF(path, topath)
        self.all = self.strQ2B(Z1)
        self.df = pd.DataFrame(Z2)
        self.v = []


    def strQ2B(self, ustring):
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

    def clean(self, x, zhenze):
        try:
            num = re.sub("\s|:", "", re.search(zhenze, x).group(1))
        except AttributeError:
            num = ''
        else:
            pass
        return num

    def strtime(self, publish_Time):
        try:
            array = time.strptime(publish_Time, u"%Y年%m月%d日")
            publishTime = time.strftime("%Y-%m-%d", array)
            return publishTime
        except BaseException:
            return None

    def relist(self, list, ss):
        ll = []
        for i in list:
            a = self.clean(ss, "{}".format(i))
            if type(a) is str and len(a) > 0:
                ll.append(a)
        if len(ll) >= 1:
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

    def get_list(self, p, size=5):
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
                a = self.df.loc[i]["映射"]
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

    def readPDF(self, path, toPath):
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
                            with open(toPath, "a") as f:
                                str = x.get_text()
                                ll.append(self.strQ2B(str))
                                print(str)
                                all += str
                                f.write(str)
                    except UnicodeEncodeError:
                        pass
                    else:
                        pass
            after = datetime.datetime.now()
            print(after - now)
        return ll, all





    def flow(self):

        dict1 = {'概况': '概况',
                 '公司基本情况': '公司基本情况',
                 '发行人简介': '发行人简介',
                 '发行人基本情况': '发行人基本情况',
                 '发行人基本信息': '发行人基本信息',
                 '英文名称': '英文名称',
                 '成立日期': 'IGSDM',
                 '设立日期': 'BGSLRQ',
                 '法定代表人': 'FRDB',
                 '法人': 'FRDB',
                 '披露负责人': 'DSHMS',
                 '董事会秘书': 'DSHMS',
                 '联系电话': 'LXRDH',
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
                 '网址': 'GSWZ'}

        self.v = list(dict1.keys())
        self.df = self.df.reset_index(level=0)
        self.df.columns = ["SUM", "A"]

        self.df["B"] = self.df["A"].apply(lambda x: self.zero(x) if type(x) is str else 0)
        self.df["C"] = (self.df["B"].shift(3) + self.df["B"].shift(2) + self.df["B"].shift(1) + self.df["B"].shift(-3) +
                        self.df["B"].shift(-2) + self.df["B"].shift(-1) + self.df["B"])
        self.df["映射"] = list(map(lambda x, y: self.havestr(self.v, x) if y >= 1 else None, self.df["A"], self.df["B"]))
        llmax = list(set(self.df["C"].tolist()))
        llmax = [int(i) for i in llmax if i >= 3]
        self.df['D'] = list(map(lambda x, y: x if y in llmax else None, self.df['SUM'], self.df['C']))
        p = list(set(self.df["D"].tolist()))
        p = [int(i) for i in p if i > 0]
        n = self.get_list(p)

        return n





        # if len(n)>=2:
        #     n1 = n[0][-1]
        #     top1 = n1 - 5
        #     last1 = n1 + 15
        #
        #     n2 = n[1][-1]
        #     top2 = n2 - 5
        #     last2 = n2 + 15
        #
        #     df1 = df.iloc[int(top1):int(last1), [1]]
        #     df2 = df.iloc[int(top2):int(last2), [1]]
        #
        #     t1 = df1["A"].tolist()
        #     ss1 = self.strQ2B(''.join(t1))
        #
        #     t2 = df2["A"].tolist()
        #     ss2 = self.strQ2B(''.join(t2))
        #     ss3 = ss1 + ss2
        #
        # if len(n) == 1:
        #     n1 = n[0][-1]
        #     top1 = n1 - 5
        #     last1 = n1 + 15
        #     df1 = df.iloc[int(top1):int(last1), [1]]
        #
        #
        #
        #
        #



        # GSCLRQ = self.strtime(self.relist(["成立时间(.+?)\\n", "成立日期(.+?)\n"], ss3))
        #
        # if GSCLRQ is None:
        #     c1 = self.relist(["成立时间(.+?)\\n", "成立日期(.+?)\n"], ss3)[:10]
        #     c2 = time.strptime(c1, u"%Y年%m月%d日")
        #     # T = c1.replace("年", " 年 ")
        #     # T = T.replace("月", " 月 ")
        #     # T = T.replace("日", " 日")
        #     GSCLRQ = time.strftime("%Y-%m-%d", c2)
        #
        # BGSLRQ = self.strtime(self.clean(ss3, "股份[\u4e00-\u9fa5]+设立日期(.+?)\n"))
        #
        # SCZCDJDD = None
        # try:
        #     c1 = self.relist(["成立时间(.+?)\\n", "成立日期(.+?)\n"], ss3)[:10]
        #     # c2 = time.strptime(c1, u"%Y年%m月%d日")
        #     T = c1.replace("年", " 年 ")
        #     T = T.replace("月", " 月 ")
        #     T = T.replace("日", " 日")
        #     IGSDM_ZW = T
        #     # IGSDM_ZW = re.search("成立日期(.+?)\n", ss3).group(1)[1:-1]
        #
        #     demo = [r"\n{}\,[\u4e00-\u9fa5]+在(.+?)完成设立登记".format(IGSDM_ZW),
        #             r"\n{}\,[\u4e00-\u9fa5]+在(.+?)注册登记".format(IGSDM_ZW),
        #             r"\n{}\,[\u4e00-\u9fa5]+在(.+?分局)".format(IGSDM_ZW),
        #             r"\n{}\,[\u4e00-\u9fa5]+在(.+?局)".format(IGSDM_ZW),
        #             r"\n{}\,[\u4e00-\u9fa5]+取得了(.+?局)".format(IGSDM_ZW)]
        #     SCZCDJDD = self.relist(demo, all)
        #     txts = re.findall("{a}\,[\u4e00-\u9fa5]+{b}".format(a=IGSDM_ZW, b=SCZCDJDD), all)
        #     df["定位"] = list(map(lambda x, y: x if type(y) is str
        #                                           and txts[0] in y else None, df["SUM"], df["A"]))
        #
        #     din = list(set(df["定位"].tolist()))
        #     din = [int(i) for i in din if i >= 3]
        #
        #     a1 = re.sub("\s|\n", "", df.loc[din[0]]['A'])
        #     a2 = re.sub("\s|\n", "", df.loc[din[0] + 1]['A'])
        #     a3 = re.sub("\s|\n", "", df.loc[din[0] + 2]['A'])
        #     a4 = re.sub("\s|\n", "", df.loc[din[0] + 3]['A'])
        #     a5 = re.sub("\s|\n", "", df.loc[din[0] + 4]['A'])
        #     aa = [a1, a2, a3, a4, a5]
        #     bb = [a1, a2, a3, a4, a5]
        #     kk = []
        #     for i in range(4):
        #         txt = aa[i]
        #         if "申报稿" in txt:
        #             bb[i] = ""
        #             bb[i - 1] = ""
        #         if "。" in txt:
        #             kk.append(i)
        #
        #     tts = "".join(bb[:kk[0] + 1])
        #     wen = tts.split('。')[0] + '。'
        #
        # except BaseException:
        #     wen = None
        #
        # GSJK = wen
        #
        # FRDB = self.relist(["\\n法定代表人(.+?)\\n", "\\n法人代表(.+?)\\n", "法人(.+?)\\n"], ss3)
        # DSHMS = self.relist(["\\n信息披露负责人(.+?)\\n", "\\n董事会秘书(.+?)\\n"], ss3)
        # LXRDH = self.relist(["联系电话(.+?)\\n", "电话号码(.+?)\\n", '联系人电话(.+?)\\n'], ss3)
        # LXRCZ = self.clean(ss3, "传真号码(.+?)\n")
        # LXRDZYJ = self.relist(["电子信箱(.+?)\\n", "电子邮箱(.+?)\\n"], ss3)
        #
        # """
        # 如果董事会秘书存在赋值给董秘
        # """
        # DMLXDH = None
        # DMCZ = None
        # DMDZYJ = None
        # if type(DSHMS) is str and len(DSHMS) >= 2:
        #     DMLXDH = LXRDH
        #     DMCZ = LXRCZ
        #     DMDZYJ = LXRDZYJ
        #
        # ZQSWDB = self.relist(["证券事务代表(.+?)\\n", "股证事务代表(.+?)\\n"], ss3)
        #
        # ZQSWDBDH = None
        # ZQSWDBCZ = None
        # ZQSWDBDZYJ = None
        # if type(ZQSWDB) is str and len(ZQSWDB) >= 0:
        #     ZQSWDBDH = LXRDH
        #     ZQSWDBCZ = LXRCZ
        #     ZQSWDBDZYJ = LXRDZYJ
        #
        # GSZCDZ = self.relist(["公司住所(.+?)\\n", "公司注册地址(.+?)\\n", "公司住所(.+?)\\n"], ss3)
        #
        # GSBGDZ = self.relist(["通信地址(.+?)\\n", "办公地址(.+?)\\n", "联系地址(.+?)\\n"], ss3)
        #
        # GSLXDZ = GSBGDZ
        #
        # GSZCDZYB = self.clean(ss3, "邮政编码(.+?)\n")
        #
        # GSBGDZYB = None
        # GSLXDZYB = None
        # if GSZCDZ == GSBGDZ:
        #     GSBGDZYB = GSZCDZ
        #     GSLXDZYB = GSZCDZYB
        # GSWZ = self.relist(["网址(.+?)\\n", "互联网地址(.+?)\\n"], ss3)
        #
        # print('FRDB:', FRDB, '\n', 'DSHMS:', DSHMS, '\n', 'LXRDH:', LXRDH, '\n',
        #       'LXRCZ:', LXRCZ, '\n', 'LXRDZYJ:', LXRDZYJ, '\n', 'GSJK:', GSJK, '\n',
        #       'GSZCDZ:', GSZCDZ, '\n', 'GSBGDZ:', GSBGDZ, '\n', 'GSZCDZYB:', GSZCDZYB, '\n',
        #       'GSCLRQ:', GSCLRQ, '\n')
        #
        # IGSDM = "?????"
        # dff = {'IGSDM': IGSDM,
        #        'GSCLRQ': GSCLRQ,
        #        'BGSLRQ': BGSLRQ,
        #        'SCZCDJDD': SCZCDJDD,
        #        'FRDB': FRDB,
        #        'DSHMS': DSHMS,
        #        'LXRDH': LXRDH,
        #        'LXRCZ': LXRCZ,
        #        'LXRDZYJ': LXRDZYJ,
        #        'DMLXDH': DMLXDH,
        #        'DMCZ': DMCZ,
        #        'DMDZYJ': DMDZYJ,
        #        'ZQSWDB': ZQSWDB,
        #        'ZQSWDBDH': ZQSWDBDH,
        #        'ZQSWDBCZ': ZQSWDBCZ,
        #        'ZQSWDBDZYJ': ZQSWDBDZYJ,
        #        'GSZCDZ': GSZCDZ,
        #        'GSBGDZ': GSBGDZ,
        #        'GSLXDZ': GSLXDZ,
        #        'GSZCDZYB': GSZCDZYB,
        #        'GSBGDZYB': GSBGDZYB,
        #        'GSLXDZYB': GSLXDZYB,
        #        'GSWZ': GSWZ,
        #        'GSJK': GSJK}
        #
        # dffff = pd.DataFrame([dff])
        # return dffff


path = r'C:\Users\qinxd\Desktop\p23.pdf'
toPath = r'C:\Users\qinxd\Desktop\p23.txt'

test1 = clean_pdf(path=path,topath=toPath)
df = test1.flow()

test2 = test1.flow()




def now_time2(a=0):
    now = datetime.datetime.now()
    delta = datetime.timedelta(minutes=a)
    n_days = now + delta
    print(n_days.strftime('%Y-%m-%d %H:%M:%S'))
    f = n_days.strftime('%Y%m%d%H%M')
    return f


path = 'C:\\Users\\qinxd\\Desktop\\to2'

import os

files = []
for i in os.listdir(path):
    pp = path + '\\' + i
    files.append(pp)

now = '\\' + now_time2() + '.csv'
dataframe.to_csv(p + now, encoding='utf_8_sig')
dffff.to_csv(r'C:\Users\qinxd\Desktop\test14.csv', encoding='utf_8_sig')
