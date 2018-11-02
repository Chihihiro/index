"""
pdf 转成JPG
"""

pdf_file = 'E:\\pycharms\\index\\shuiying\\03898-JUNE23-GTJAHK-中文版.pdf'
from wand.image import Image
def convert_pdf_to_jpg(file_name, pic_file=None, resolution=500):
    # 转换函数，默认分辨率120
    with Image(filename=file_name, resolution=resolution) as img:
        print('pages = ', len(img.sequence))

        with img.convert('jpeg') as converted:
            # 指定图片位置
            if pic_file != None:
                converted.save(filename=pic_file)
            else:
                # 同目录同文件名
                converted.save(filename= '%s.jpg' % (file_name[:file_name.rindex('.')]))

convert_pdf_to_jpg(pdf_file)

from reportlab.pdfgen import canvas
import io
# from reportlab.lib.pagesizes import A4, landscape
# def convert_to_pdf(im_name,pdf_name):
#     (w, h) = landscape(A4)
#     with open(pdf_name, 'wb') as f:
#         c = canvas.Canvas(f, pagesize=A4)
#         c.drawImage(im_name, 0, 0)
#         c.showPage()
#         c.save()


"""
jpg 转pdf
"""

import os
import sys
from reportlab.lib.pagesizes import portrait
from reportlab.pdfgen import canvas
from PIL import Image
def imgtopdf(input_paths, outputpath):
    (maxw, maxh) = Image.open(input_paths).size
    c = canvas.Canvas(outputpath, pagesize=portrait((maxw, maxh)))
    c.drawImage(input_paths, 0, 0, maxw, maxh)
    c.showPage()
    c.save()
#
# imgtopdf("E:\\pycharms\\index\\shuiying\\jpgs\\00051-2.jpg", "cc.pdf")
pp = "E:\\pycharms\\index\\shuiying\\jpgs\\"
for filename in os.listdir(pp):
    imgtopdf(pp+filename, pp+filename[:-3]+'pdf')

