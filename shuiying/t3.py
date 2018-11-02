# coding:UTF-8
import cv2
import numpy as np


class Detect:
    def __init__(self, path):
        # 原始图像信息
        self.ori_img = cv2.imread(path)
        self.gray = cv2.cvtColor(self.ori_img, cv2.COLOR_BGR2GRAY)
        self.hsv = cv2.cvtColor(self.ori_img, cv2.COLOR_BGR2HSV)
        # 获得原始图像行列
        rows, cols = self.ori_img.shape[:2]
        # 工作图像
        self.work_img = cv2.resize(self.ori_img, (cols / 4, rows / 4))
        self.work_gray = cv2.resize(self.gray, (cols / 4, rows / 4))
        self.work_hsv = cv2.resize(self.hsv, (cols / 4, rows / 4))

    # 颜色区域提取
    def color_area(self):
        # 提取红色区域(暂定框的颜色为红色)
        low_red = np.array([156, 43, 46])
        high_red = np.array([180, 255, 255])
        mask = cv2.inRange(self.work_hsv, low_red, high_red)
        red = cv2.bitwise_and(self.work_hsv, self.work_hsv, mask=mask)
        return red

path = 'C:\Users\qinxd\Desktop\\read1.png'