import os

def pdftoXml(filename):
    pdfpath = 'E:/xml/' + filename + '.pdf'
    resultpath = 'E:/xml/' + filename + '.xml'
    os.system('E:/xml/poppler-0.45/bin/pdftohtml.exe %s -i -xml %s' % (pdfpath, resultpath))

pdftoXml('ZG')



