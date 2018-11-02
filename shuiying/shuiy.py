__author__ = 'River'
# -*- coding: utf-8 -*-
import cv2, os, shutil, datetime, re, time
from threading import Thread
from hashlib import md5

PICHASH = {}


def md5_file(name):
    try:
        m = md5()
        a_file = open(name, 'rb')
        m.update(a_file.read())
        a_file.close()
        return m.hexdigest()
    except:
        return None


def nowater(dir, newdir, dirlist):
    jpgname = "E:\\pycharms\\index\\shuiying\\jpgs\\001.jpg"
    newdir = "C:\\Users\\qinxd\\Desktop\\"

    img = cv2.imread(jpgname)
    x, y, z = img.shape
    for i in range(x):
        for j in range(y):
            varP = img[i, j]
            if sum(varP) > 250 and sum(varP) < 765:  # 大于250，小于765（sum比白色的小）
                img[i, j] = [255, 255, 255]
    cv2.imwrite("C:\\Users\\qinxd\\Desktop\\new01.jpg", img)



if __name__ == '__main__':
    dir = "E:\\pycharms\\index\\shuiying\\jpgs"
    newdir = "C:\\Users\\qinxd\\Desktop\\"
    list0 = []

    Thread(target=nowater, args=(dir, newdir, list0)).start()  # 这里只有


    list0 = []
    for dirpath, dirnames, filenames in os.walk(dir):
        for filename in filenames:
            cc = dirpath + '\\'+filename
            list0.append(cc)

import cv2
import numpy as np
import os
path ="E:\\pycharms\\index\\shuiying\\jpgs\\001.jpg"
# os.path.exists(path)
img = cv2.imread(path)
hight, width, depth = img.shape[0:3]

# 图片二值化处理，把[240, 240, 240]~[255, 255, 255]以外的颜色变成0
thresh = cv2.inRange(img, np.array([240, 240, 240]), np.array([255, 255, 255]))

# 创建形状和尺寸的结构元素
kernel = np.ones((3, 3), np.uint8)

# 扩张待修复区域
hi_mask = cv2.dilate(thresh, kernel, iterations=1)
specular = cv2.inpaint(img, hi_mask, 5, flags=cv2.INPAINT_TELEA)

cv2.namedWindow("Image", 0)
cv2.resizeWindow("Image", int(width / 2), int(hight / 2))
cv2.imshow("Image", img)

cv2.namedWindow("newImage", 0)
cv2.resizeWindow("newImage", int(width / 2), int(hight / 2))
cv2.imshow("newImage", specular)
cv2.waitKey(0)
cv2.destroyAllWindows()

