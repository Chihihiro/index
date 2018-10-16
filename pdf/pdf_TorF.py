import tabula
import pandas as pd
import re
import os
import subprocess


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
        if su > 100:
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
                df = tabula.read_pdf(path, encoding='utf-8', pages=[1], lattice=bool)
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


def one_page(path, pa):
    try:
        df1 = tabula.read_pdf(path, encoding='utf-8', pages=[pa], lattice=bool)
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
    y = "".join(dff['clean'].values.tolist())
    if y == '':
        y = '解析为空请查看一下'

    su = dff['包含的数字'].sum()
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
    # path = 'C:/Users/qinxd/Desktop/10-4'
    # path = 'C:/Users/qinxd/Desktop/有问题的pdf'
    p = os.getcwd()
    pp = p + '\\pdfs\\'
    files = os.listdir(pp)
    pdfs = []
    for i in files:
        if '.pdf' in i:
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

    p = os.getcwd()
    pp = p + '\\pdfs\\'
    dataframe = pd.DataFrame(result, columns=['PDF名字', '解析结果', '内容和状况'])
    dataframe['解析不出的'] = dataframe['内容和状况'].apply(lambda x: '解析失败,或者有图片' if '解析' in x else 'pass')
    dataframe.to_csv(pp + '\\pdf解析.csv', encoding='utf_8_sig')


if __name__ == '__main__':
    PATH = os.getcwd()+'\\pdfs\\'
    if not os.path.isdir(PATH):
        os.mkdir(PATH)
    main()




