#导入cv模块
# import cv2
# #读取图像，支持 bmp、jpg、png、tiff 等常用格式
# img = cv2.imread("E:\pycharms\index\shuiying\866828890928632067.jpg")
# #创建窗口并显示图像
# cv2.namedWindow("Image")
# cv2.imshow("Image",img)
# cv2.waitKey(0)
# #释放窗口
# cv2.destroyAllWindows()

from pdf2image import convert_from_path
import tempfile

def main(filename, outputDir):
    print('filename=', filename)
    print('outputDir=', outputDir)
    with tempfile.TemporaryDirectory() as path:
        images = convert_from_path(filename)
        for index, img in enumerate(images):
            img.save('%s/page_%s.png' % (outputDir, index))
if __name__ == "__main__":
    main('E:/pycharms/index/shuiying/00051.pdf', 'E:/pycharms/index/shuiying/')

import tempfile
from pdf2image import convert_from_path, convert_from_bytes

# 以下三种方式都可以读取文件，第三种最好
import fitz
import glob


def rightinput(desc):
    flag = True
    while (flag):
        instr = input(desc)
        try:
            intnum = eval(instr)
            if type(intnum) == int:
                flag = False
        except:
            print('请输入正整数！')
            pass
    return intnum

pdffile = glob.glob("*.pdf")[0]
doc = fitz.open(pdffile)

flag = rightinput("输入：1：全部页面；2：选择页面\t")
if flag == 1:
    strat = 0
    totaling = doc.pageCount

else:
    strat = rightinput('输入起始页面：') - 1
    totaling = rightinput('输入结束页面：')

for pg in range(strat, totaling):
    page = doc[pg]
    zoom = int(100)
    rotate = int(0)
    trans = fitz.Matrix(zoom / 100.0, zoom / 100.0).preRotate(rotate)
    pm = page.getPixmap(matrix=trans, alpha=False)
    pm.writePNG('pdf2png/%s.png' % str(pg + 1))

