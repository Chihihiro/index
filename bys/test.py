import re
from matplotlib import pyplot as plt
from wordcloud import WordCloud
from scipy.misc import imread

x = '666666阿斌要飞天'

y = re.sub('666666', '', x)
print(y)
import time
time.sleep(5)