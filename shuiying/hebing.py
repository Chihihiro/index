"""
合并成一张PDF
"""


import codecs
import os
import PyPDF2 as PyPDF2

# 建立一个装pdf文件的数组
files = list()

# 遍历该目录下的所有文件
for filename in os.listdir("E:\pycharms\index\shuiying\jpgs"):

    # 如果是以.pdf结尾的文件，则追加到数组中
    if filename.endswith(".pdf"):
        files.append(filename)

# 以数字进行排序（数组中的排列顺序默认是以ASCII码排序，当以数字进行排序时会不成功）
# newfiles = sorted(files, key=lambda d: int(d.split(".pdf")[0]))

# 进入该目录
os.chdir("E:\pycharms\index\shuiying\jpgs")

# 生成一个空白的PDF文件
pdfwriter = PyPDF2.PdfFileWriter()
for item in files:

    ##以只读方式依次打开pdf文件
    pdfreader = PyPDF2.PdfFileReader(open(item, "rb"))
    for page in range(pdfreader.numPages):
        # #将打开的pdf文件内容一页一页的复制到新建的空白pdf里
        pdfwriter.addPage(pdfreader.getPage(page))

# 生成all.pdf文件
with codecs.open("all.pdf", "wb") as f:

    # 将复制的内容全部写入all.pdf文件中
    pdfwriter.write(f)