import tabula
import pandas as pd
import re
from datetime import datetime, timedelta
import os


def now_time(a=0):
    now = datetime.now()
    delta = timedelta(minutes=a)
    n_days = now + delta
    cc = n_days.strftime('%Y-%m-%d %H:%M:%S')
    return cc


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


def get_w(t):
    ll = ['增加/(減少)', '本月增加/(減少)']
    w = []
    for i in ll:
        try:
            v = t.index(i)
            w.append(v)
        except ValueError:
            pass
        else:
            pass
    return w


def renum(x):
    try:
        num = float(re.sub(",", "", re.search('[0-9]*,?[0-9]', x).group()))
    except AttributeError:
        num = 0
    else:
        pass
    return num


def return_result(path):
    # df = tabula.read_pdf(path, encoding='utf-8', pages=[1, 2, 3], lattice=bool)
    try:
        df = read_one_pdf(path)
        print(df)
        df2 = pd.DataFrame(df.columns, columns=['A'])
        df.columns = ['A']
        dff = df.append(df2).reset_index(drop=True)
        dff['A'] = dff['A'].apply(lambda x: strQ2B(x) if type(x) is str else x)
        dff["B"] = dff['A'].apply(lambda x: x.split())
        dff["C"] = dff['B'].apply(lambda x: get_w(x))
        dff["D"] = list(map(lambda x, y: y[x[0] + 1] if len(x) > 0 else '无', dff['C'], dff['B']))
        dff["E"] = dff["D"].apply(lambda x: renum(x) if type(x) is str else 0)
        print(dff)
        su = dff["E"].sum()
        print(su)
        if su > 0:
            a = '有数据请查看'
            return a
        else:
            a = '无'
            return a
    except BaseException:
        print('解析失败返回错误')
        a = '解析失败请手动查看PDF'
        return a


def read_one_pdf(path):
    try:
        df = tabula.read_pdf(path, encoding='utf-8', pages=[1, 2, 3], lattice=bool)
    except BaseException:
        try:
            print('第一次解析失败')
            df = tabula.read_pdf(path, encoding='utf-8', pages=[1, 2], lattice=bool)
        except BaseException:
            try:
                print('第二次解析失败')
                df = tabula.read_pdf(path, encoding='utf-8', pages=[1, 2], lattice=bool)
            except BaseException:
                print('无法解析')
                d = {"增加/(減少)wu": "无"}
                df = pd.DataFrame([d])
            else:
                pass
        else:
            pass
    else:
        pass
    return df


def main():
    """

    :path: 读取文件夹的路径
    """
    path = 'C:/Users/qinxd/Desktop/pdfpd/10-5'
    files = os.listdir(path)
    pdfs = []
    for i in files:
        if '.pdf' in i:
            pdfs.append(i)
        else:
            pass

    result = []
    for pdf in pdfs:
        p = path + '/' + pdf
        w = return_result(p)
        f = [pdf, w]
        print(pdf)
        result.append(f)

    pp = os.getcwd()
    dataframe = pd.DataFrame(result, columns=['PDF名字', '解析结果'])
    dataframe.to_csv(pp + '/pdf解析.csv', encoding='utf_8_sig')


    p1 = '83199-南方五年國債－Ｒ-月報表月報表 - 2018年09月30日 (190KB, PDF).pdf'

if __name__ == '__main__':
    main()



