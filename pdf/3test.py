import tabula

df = tabula.read_pdf('C:\\Users\\qinxd\\Desktop\\资产证券化业务备案情况（2017年一季度）.pdf',output_format='dataframe',
             encoding='utf-8',multiple_tables=True)
print(df)

for indexs in df.index:
    # 遍历打印企业名称
    print(df.loc[indexs].values[1].strip())

import pdfplumber
pdf = pdfplumber.open('C:\\Users\\qinxd\\Desktop\\资产证券化业务备案情况（2017年一季度）.pdf')
# 这里只读取了第一页，我的文档第一页是有表格的，
# 自己相应的改表格的页码就行了，示例代码
p0 = pdf.pages()
table = p0.extract_table()
import pandas as pd
df = pd.DataFrame(table[1:], columns=table[0])
