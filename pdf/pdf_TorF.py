import os
base_path = os.getcwd()
print(base_path)
import tabula
import pandas as pd
import re
import subprocess
import datetime


class mv_file:
    """
    type_file : 'all' 是全部, 可以指定各种格式pdf,csv,xlsx
    """

    def __init__(self, old_path, new_path, type_file='all'):
        self.old_path = old_path
        self.new_path = new_path
        self.files = []
        self.type_file = type_file

    def read_files(self):
        if self.type_file == 'all':
            self.files = os.listdir(self.old_path)
        else:
            files = os.listdir(self.old_path)
            self.files = [i for i in files if i[-len(self.type_file):] == self.type_file]
        return self.files

    def move_files(self):
        if not os.path.exists(self.new_path):
            os.makedirs(self.new_path)  # 创建路径
        files_list = self.read_files()
        for i in files_list:
            srcfile = self.old_path + '\\' + i
            dstfile = self.new_path + '\\' + i
            shutil.move(srcfile, dstfile)
            print("move %s -> %s" % (srcfile, dstfile))


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

def now_time(a=0):
    now = datetime.datetime.now()
    delta = datetime.timedelta(days=a)
    n_days = now + delta
    print(n_days.strftime('%Y-%m-%d %H:%M:%S'))
    f = n_days.strftime('%Y-%m-%d')
    return f



def one_page(path, pa):
    try:
        df1 = tabula.read_pdf(path, encoding='utf-8', pages=[pa], lattice=bool)
        if df1.index.values[0] == 0:
            pass
        else:
            cc = df1.columns[0]
            t = df1.reset_index(drop=False)['index']
            d = pd.DataFrame(t)

            d.columns = [cc]
            df1 = d

    except subprocess.CalledProcessError:
        print('超过页数')
        d = {"A": '增加/(減少)超过页数本月底结存'}
        df1 = pd.DataFrame([d])

    except BaseException as e:
        print(e, '无法解析')
        d = {"A": '增加/(減少)无法解析请手动查看本月底结存'}
        df1 = pd.DataFrame([d])
    return df1

def read_3_page(path, pa):

    df1 = one_page(path, pa)

    if type(df1) is pd.DataFrame:
        conum = len(df1.columns)
        if 10 > conum > 1:
            print('有两列数据')

            # path = 'C:/Users/qinxd/Desktop/00547-數字王國-月報表截至2018年9月30日止月份之股份發行人的證券變動月報表 (218KB, PDF).pdf'
            co1 = pd.DataFrame([df1.columns[0]], columns=['A'])
            # re.sub('\r0|\r| | |　｜　｜', "", s)
            li = [chr(i) for i in range(ord("A"), ord("Z") + 1)]
            df1.columns = [li[i] for i in range(len(df1.columns))]
            Adf = pd.DataFrame(df1['A'])
            dff = Adf.append(co1).reset_index(drop=True)
            return dff
        else:
            co1 = pd.DataFrame([df1.columns[0]], columns=['A'])
            df1.columns = ['A']
            dff = df1.append(co1).reset_index(drop=True)
            return dff
    else:
        print('为图片')
        d = {"A": '增加/(減少)解析为图片请查看本月底结存'}
        dff = pd.DataFrame([d])
        return dff


def clean_txt(x):
    try:
        num = re.sub(",|\(減少\)|\(\)", "", re.search('增加/(.+?)本月底.存', x).group(1))
    except AttributeError:
        num = ''
    else:
        pass
    return num

def only_num(x):
    try:
        # num = float(re.search('[0-9]*,?[0-9]', x).group())
        txt = re.findall('[0-9]*', re.sub(",", "", x))
        num = max([float(x) for x in txt if x != ''])
    except ValueError:
        num = None
    else:
        pass
    return num

def read_pdf_all(path):
    # df1 = read_3_page(path, 1)
    # df2 = read_3_page(path, 2)
    # df3 = read_3_page(path, 3)
    dfs = []
    for i in range(1, 4):
        try:
            t = read_3_page(path, i)
            dfs.append(t)
        except BaseException:
            d = {"A": '增加/(減少)无法解析请手动查看本月底结存'}
            t = pd.DataFrame([d])
            dfs.append(t)
        else:
            pass

    # df = df1.append([df2, df3]).reset_index(drop=True)
    df = dfs[0].append([dfs[1], dfs[2]]).reset_index(drop=True)
    dff = df.dropna(axis=0).reset_index(drop=True)
    dff['A'] = dff['A'].apply(lambda x: strQ2B(x) if type(x) is str else x)
    dff["B"] = dff['A'].apply(lambda x: x.split())
    dff['txt'] = dff['B'].apply(lambda x: "".join(x))
    dff['clean'] = dff['txt'].apply(lambda x: clean_txt(x))
    dff['包含的数字'] = dff['clean'].apply(lambda x: only_num(x))
    dff['特殊'] = dff['clean'].apply(lambda x: 11 if type(x) is str and '增加減少' in x else 0)
    y = "".join(dff['clean'].values.tolist())

    if y == '':
        y = '解析为空请查看一下'

    su = dff['包含的数字'].sum() + dff['特殊'].sum()
    if su > 10:
        a = '有数据请查看'
        return [a, y]
    else:
        a = '无'
        return [a, y]


def main():
    """

    :path: 读取文件夹的路径
    """
    # path = 'C:/Users/qinxd/Desktop/10-4/w/'
    # path = 'C:/Users/qinxd/Desktop/有问题的pdf'
    p = os.getcwd()
    pp = p + '\\pdfs\\'
    files = os.listdir(pp)
    pdfs = []
    for i in files:
        if i[-3:] == 'pdf':
            pdfs.append(i)
        else:
            pass

    result = []
    for pdf in pdfs:
        readp = pp + pdf
        print(pdf)
        w = read_pdf_all(readp)
        f = [pdf, w[0], w[1]]
        result.append(f)

    # p = os.getcwd()
    # pp = p + '\\pdfs\\'
    dataframe = pd.DataFrame(result, columns=['PDF名字', '解析结果', '内容和状况'])
    dataframe['解析不出的'] = dataframe['内容和状况'].apply(lambda x: '解析失败,或者有图片' if '解析' in x else 'pass')
    dataframe.to_csv(pp + '\\pdf解析.csv', encoding='utf_8_sig')


if __name__ == '__main__':
    PATH = os.getcwd()+'\\被解析过的pdfs\\'
    if not os.path.isdir(PATH):
        os.mkdir(PATH)
    main()

    floderaddress = os.getcwd()+'\\pdfs\\'
    file_all = os.listdir(floderaddress)  # 路径下所有文件名

    import shutil



a = mv_file(floderaddress, PATH, type_file='pdf')


# path = 'C:/Users/qinxd/Desktop/10-4/01686-新意網集團-月報表截至2018年9月30日止股份發行人的證券變動月報表 (234KB, PDF).pdf'
#
#
# import sys
# import os
# base_path = os.path.dirname(os.path.abspath(__file__))
# base_path = os.path.dirname(os.path.abspath(tabula.__file__))
# if getattr(sys, 'frozen', False):
#     base_path = sys._MEIPASS ## 表示在实际生产运行时让程序去此目录查找
# if not base_path in sys.path:
#     sys.path.append(base_path)
class Employee:
    '所有员工的基类'
    empCount = 0

    def __init__(self, name, salary):
        self.name = name
        self.salary = salary
        Employee.empCount += 1

    def displayCount(self):
        print("Total Employee %d" % Employee.empCount)

    def displayEmployee(self):
        print("Name : ", self.name, ", Salary: ", self.salary)



