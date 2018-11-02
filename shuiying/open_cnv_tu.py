
import cv2
import traceback
filename = 'E:\\pycharms\\index\\shuiying\\1541063102(1).png'
try:
    # 读取图片
    img = cv2.imread(filename)
    # 获取图片大小
    x, y, z = img.shape
    for i in range(x):
        for j in range(y):
            varP = img[i, j]
            if sum(varP) == 687:
                img[i, j] = [255, 255, 255]
    cv2.imwrite('zmister_qushuiyin.jpg', img)
except Exception as e:
    print(traceback.print_exc())



# coding=utf-8
# 图片修复

import cv2
import numpy as np

path = r"E:/pycharms/index/shuiying/jpgs/001.jpg"
import os
if os.path.isfile(path):
    print('zai')
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




