# -*- encoding:utf-8 -*-
"""
    author:lgh
    简单的doc转pdf，html，pdf转doc脚本
    依赖库pdfminer3k,pip install pdfminer3k即可
"""

from win32com.client import Dispatch, constants

from pdfminer.pdfparser import PDFParser, PDFDocument
from pdfminer.pdfinterp import PDFResourceManager, PDFPageInterpreter
from pdfminer.layout import LAParams, LTTextBoxHorizontal
from pdfminer.converter import PDFPageAggregator
from pdfminer.pdfinterp import PDFTextExtractionNotAllowed

def doc2pdf(input, output):
    w = Dispatch('Word.Application')
    try:
        # 打开文件
        doc = w.Documents.Open(input, ReadOnly=1)
        # 转换文件
        doc.ExportAsFixedFormat(output, constants.wdExportFormatPDF,
                                Item=constants.wdExportDocumentWithMarkup, CreateBookmarks = constants.wdExportCreateHeadingBookmarks)
        return True
    except Exception as e:
        print(e)
        return False
    finally:
        w.Quit(constants.wdDoNotSaveChanges)

def doc2html(input, output):
    w = Dispatch('Word.Application')
    try:
        doc = w.Documents.Open(input, ReadOnly=1)
        doc.SaveAs(output, 8)
        return True
    except Exception as e:
        print(e)
        return False
    finally:
        w.Quit(constants.wdDoNotSaveChanges)

def pdf2doc(input, output):
    try:
        with open(input, 'rb') as f:
            parser = PDFParser(f)
            doc = PDFDocument()
            parser.set_document(doc)
            doc.set_parser(parser)
            # 设置初始化密码
            doc.initialize()
            if not doc.is_extractable:
                raise PDFTextExtractionNotAllowed
            else:
                rsrcmgr = PDFResourceManager()
                laparams = LAParams()
                device = PDFPageAggregator(rsrcmgr, laparams=laparams)
                interpreter = PDFPageInterpreter(rsrcmgr, device)
                for page in doc.get_pages():
                    interpreter.process_page(page)
                    layout = device.get_result()
                    for x in layout:
                        if isinstance(x, LTTextBoxHorizontal):
                            with open(output, 'a', encoding='utf-8') as f1:
                                results = x.get_text()
                                f1.write(results+'\n')
        return True
    except Exception as e:
        print(e)
        return False


def main():
    # rc = doc2pdf(input, output)
    # rc = doc2html(input, output)
    input = r'F:\save_data\流畅的Python.pdf'
    output = r'F:\save_data\test.doc'
    rc = pdf2doc(input, output)
    if rc:
        print('转换成功')
    else:
        print('转换失败')

if __name__ == '__main__':
    main()