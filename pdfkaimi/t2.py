import sys
import re
import importlib
importlib.reload(sys)
from pdfminer.pdfparser import PDFParser, PDFDocument
from pdfminer.pdfinterp import PDFResourceManager, PDFPageInterpreter
from pdfminer.converter import PDFPageAggregator
from pdfminer.layout import LTTextBoxHorizontal, LAParams
from pdfminer.pdfinterp import PDFTextExtractionNotAllowed
import datetime
import pandas as pd


def readPDF(path, toPath):
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
        laparams = LAParams(line_overlap=0.5, char_margin=120.0, line_margin=0.5, word_margin=0.1,
            boxes_flow=0.5, detect_vertical=False, all_texts=False, paragraph_indent=None,
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
                    if(isinstance(x, LTTextBoxHorizontal)):
                        with open(toPath, "a") as f:
                            str = x.get_text()
                            ll.append(str)
                            print(str)
                            all+= str

                            f.write(str)
                except UnicodeEncodeError:
                    pass
                else:
                    pass
        after = datetime.datetime.now()
        print(after-now)
    return ll ,all






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



def zero(strs):
    s = 0
    for i in v:
        if i in strs:
            s += 1
    return s

def get_list(p, size=10):
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
    if len(ps) == 2:
        return ps
    else:
        ps.sort(key=lambda i: len(i), reverse=True)
        lps = ps[:2]
        return lps


path = r'C:\Users\qinxd\Desktop\P20POJIE.pdf'
toPath = r'C:\Users\qinxd\Desktop\P20POJIE.txt'
# path = r'C:\Users\qinxd\Desktop\005.pdf'
# toPath = r'C:\Users\qinxd\Desktop\005.txt'
ll, all = readPDF(path, toPath)
all = strQ2B(all)
df=pd.DataFrame(ll)

dict1 = {'概况': '概况',
         '公司基本情况': '公司基本情况',
         '发行人简介 ': '发行人简介 ',
         '发行人基本': '发行人基本',
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

v = list(dict1.keys())

df=df.reset_index(level=0)
df.columns = ["SUM", "A"]

df["B"] =df["A"].apply(lambda x: zero(x) if type(x) is str else 0)
df["C"] = (df["B"].shift(3)+df["B"].shift(2)+df["B"].shift(1)+df["B"].shift(-3)+df["B"].shift(-2)+
           df["B"].shift(-1)+df["B"])
llmax = list(set(df["C"].tolist()))
llmax = [i for i in llmax if i > 0]
m1 = max(llmax)
llmax.remove(m1)
m2 = max(llmax)

df['D'] = list(map(lambda x, y: x if y in [m1, m2] else None, df['SUM'], df['C']))

p = list(set(df["D"].tolist()))
p = [i for i in p if i > 0]
ju = 10

n = get_list(p)
n1 = n[0][0]
top1 = n1-8
last1 = n1+8

n2 = n[1][0]
top2 = n2-8
last2 = n2+8

df1 = df.iloc[int(top1):int(last1), [1]]
df2 = df.iloc[int(top2):int(last2), [1]]

t1 = df1["A"].tolist()
ss1 = strQ2B(''.join(t1))

t2 = df2["A"].tolist()
ss2 = strQ2B(''.join(t2))
ss3 = ss1+ss2






def clean(x,zhenze):
    try:
        num = re.sub("\s", "", re.search(zhenze, x).group(1))
    except AttributeError:
        num = ''
    else:
        pass
    return num

import time

def strtime(publish_Time):
    try:
        array = time.strptime(publish_Time, u"%Y年%m月%d日")
        publishTime = time.strftime("%Y-%m-%d", array)
        return publishTime
    except BaseException:
        return None

def relist(list, ss):
    ll = []
    for i in list:
        a = clean(ss, "{}".format(i))
        if type(a) is str:
            ll.append(a)
    if len(ll) >= 1:
        return ll[0]
    else:
        return None



GSCLRQ = strtime(clean(ss3, "成立日期:(.+?)\n"))
BGSLRQ = strtime(clean(ss3, "股份[\u4e00-\u9fa5]+设立日期:(.+?)\n"))

try:
    IGSDM_ZW = re.search("成立日期: (.+?)\n", ss3).group(1)[:-1]
    quan3 = re.search("{}(.+?)营业执照".format(IGSDM_ZW),all).group(1)
    quan3last = re.search("{}(.+?)\.".format(quan3),all).group(1)
    wen = IGSDM_ZW+quan3+"营业执照"+quan3last+'.'
except BaseException:
    wen = None


SCZCDJDD = None
if type(wen) is str:
    SCZCDJDD = relist(["在(.+?)注册登记", ",(.+?)向"], wen)

FRDB = relist(["法定代表人:(.+?)\\n", "法人代表:(.+?)\\n", "法人:(.+?)\\n"], ss3)
DSHMS =relist(["信息披露负责人:(.+?)\\n", "董事会秘书:(.+?)\\n"], ss3)
LXRDH = relist(["联系电话:(.+?)\\n","电话号码:(.+?)\\n", '联系人电话:(.+?)\\n'], ss3)
LXRCZ = clean(ss3, "传真号码:(.+?)\n")
LXRDZYJ = relist(["电子信箱:(.+?)\\n", "电子邮箱:(.+?)\\n"], ss3)
"""
如果董事会秘书存在赋值给董秘
"""
DMLXDH = None
DMCZ = None
DMDZYJ = None
if type(DSHMS) is str:
    DMLXDH = LXRDH
    DMCZ = LXRCZ
    DMDZYJ = LXRDZYJ

ZQSWDB = relist(["证券事务代表:(.+?)\\n", "股证事务代表:(.+?)\\n"], ss3)

ZQSWDBDH = None
ZQSWDBCZ= None
ZQSWDBDZYJ= None
if type(ZQSWDB) is str:
    ZQSWDBDH = LXRDH
    ZQSWDBCZ = LXRCZ
    ZQSWDBDZYJ = LXRDZYJ

GSZCDZ = relist(["公司住所:(.+?)\\n", "公司注册地址:(.+?)\\n", "公司住所:(.+?)\\n"], ss3)

GSBGDZ = relist(["通信地址:(.+?)\\n", "办公地址:(.+?)\\n", "联系地址:(.+?)\\n"], ss3)

GSLXDZ = GSBGDZ

GSZCDZYB = clean(ss3, "邮政编码:(.+?)\n")

GSBGDZYB = None
GSLXDZYB = None
if GSZCDZ == GSBGDZ:
    GSBGDZYB =GSZCDZ
    GSLXDZYB = GSZCDZYB

GSWZ = clean(ss3, "网址:(.+?)\n")

GSJK = wen
IGSDM = "?????"
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


dffff = pd.DataFrame([dff])





df.to_csv(r'C:\Users\qinxd\Desktop\test3.csv', encoding='utf_8_sig')