


# df = tabula.read_pdf("C:/Users/qinxd/Desktop/00803-昌興國際-月報表截至二零一八年九月三十日止之股份發行人的證券變動月報表 (485KB, PDF).pdf", encoding='utf-8', pages=[1,2,3])
# li = [chr(i) for i in range(ord("A"), ord("Z")+1)]
# df.columns = [li[i] for i in range(len(df.columns))]
# sai = df[(df.A == '增加/(減少)')|(df.A == '本月增加/（減少）')|(df.A == '本月增加／（減少）')]


# def read_one_pdf(path):
#     try:
#         df = tabula.read_pdf(path, encoding='utf-8', pages=[1, 2, 3])
#     except pd.errors.ParserError:
#         try:
#             print('第一次解析失败')
#             df = tabula.read_pdf(path, encoding='utf-8', pages=[1, 2])
#         except pd.errors.ParserError:
#             try:
#                 print('第二次解析失败')
#                 df = tabula.read_pdf(path, encoding='utf-8', pages=[1])
#             except BaseException:
#                 print('无法解析')
#                 df = []
#             else:
#                 pass
#         else:
#             pass
#     else:
#         pass
#     return df


li = [chr(i) for i in range(ord("A"), ord("Z")+1)]
df.columns = [li[i] for i in range(len(df.columns))]
sai = df[(df.A == '增加/(減少)')|(df.A == '本月增加/（減少）')|(df.A == '本月增加／（減少）')|(df.A == '增加／（減少）')]
a = sai.iloc[:, 1:]
b = a.dropna(axis=1, how="all")
c = b.dropna(axis=0, how="all")
col = c.columns
for i in col:
    c[i] = c[i].apply(lambda x: float(re.search('[0-9]*,?[0-9]', x).group()) if type(x) is str else 0)

d = c.sum(axis=1)
e = d.sum()
print(e)