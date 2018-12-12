# -*- encoding: utf-8 -*-
import os
from win32com import client


def doc2pdf(doc_name, pdf_name):
    """
    :word文件转pdf
    :param doc_name word文件名称
    :param pdf_name 转换后pdf文件名称
    """
    try:
        word = client.DispatchEx("Word.Application")
        if os.path.exists(pdf_name):
            os.remove(pdf_name)
        worddoc = word.Documents.Open(doc_name, ReadOnly=1)
        worddoc.SaveAs(pdf_name, FileFormat=17)
        worddoc.Close()
        return pdf_name
    except:
        return 1


if __name__ == '__main__':
    doc_name = "C:\\Users\\qinxd\\Desktop\\0238.doc"
    # input = "C:\\Users\\qinxd\\Desktop\\66.docx"
    ftp_name = r"C:\Users\qinxd\Desktop\02308.pdf"
    # doc_name = "f:/test.doc"
    # ftp_name = "f:/test.pdf"
    doc2pdf(doc_name, ftp_name)

import win32com.client


def check_exsit(process_name):
    WMI = win32com.client.GetObject('winmgmts:')
    processCodeCov = WMI.ExecQuery('select * from Win32_Process where Name="%s"' % process_name)
    if len(processCodeCov) > 0:
        return 1
    else:
        return 0


if __name__ == '__main__':
    check_exsit('java.exe')

import time
times = 20

nb = 0
for i, e in enumerate(range(times)):
    print(i)
    nn = check_exsit('java.exe')
    time.sleep(5)
    nb += 5
    if nb == 10+times:
        print('超过时间直接解析')
        break
    if nn < 1:
        print('解析完成')
        break

