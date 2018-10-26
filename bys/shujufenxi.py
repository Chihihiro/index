import jieba
from matplotlib import pyplot as plt
from wordcloud import WordCloud
from scipy.misc import imread



def mk_png(path, p):
    t = open(path, mode='r', encoding='gbk').read()
    cut = jieba.cut(t)  # 分词
    string = ' '.join(cut)
    print(len(string))
    mk = imread(p)
    stopword = ['？']  # 设置停止词，也就是你不想显示的词，这里这个词是我前期处理没处理好，你可以删掉他看看他的作用
    wc = WordCloud(
        font_path='E:\\pycharms\\index\\aaa.ttf',
        background_color='white',
        width=600,
        height=800,
        mask=mk,
        max_font_size=40,
        max_words=20,
    )
    wc.generate_from_text(string)  # 绘制图片
    plt.imshow(wc)
    plt.axis('off')
    plt.figure()
    # plt.show()  # 显示图片
    wc.to_file('C:/Users/qinxd/Desktop/new.png')  # 保存图片



path = 'C:/Users/qinxd/Desktop/nibao.txt'
p = 'C:/Users/qinxd/Desktop/1359776-20180520173211640-474294784.jpg'


mk_png(path, p)
print('输出完成')
import time
time.sleep(4)


