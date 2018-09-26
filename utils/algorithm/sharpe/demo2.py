import datetime as dt

import numpy as np
import pandas as pd

from utils.algorithm.sharpe import factors_sharpe as fs

# 私募实时计算
#### 参数1 ####
start, end = dt.date(2017, 10, 1), dt.date(2018, 4, 30)
freq = "w"
# 需要使用的因子ID, 及其所需映射的维度;
# e.g.1 股票类型因子
DEFAULT_1 = {
    '000852.CSI': 'stock',
    '000918.CSI': 'stock',
    '000919.CSI': 'stock',
    'H30351.CSI': 'stock',
    'H30352.CSI': 'stock',
    'R001.CM': 'cash'
}

# e.g.2 申万28因子
DEFAULT_2 = {
    '801010.SI': '农林牧渔',
    '801020.SI': '采掘',
    '801030.SI': '化工',
    '801040.SI': '钢铁',
    '801050.SI': '有色金属',
    '801080.SI': '电子元器件',
    '801110.SI': '家用电器',
    '801120.SI': '食品饮料',
    '801130.SI': '纺织服装',
    '801140.SI': '轻工制造',
    '801150.SI': '医药生物',
    '801160.SI': '公用事业',
    '801170.SI': '交通运输',
    '801180.SI': '房地产',
    '801200.SI': '商业贸易',
    '801210.SI': '餐饮旅游',
    '801230.SI': '综合',
    '801710.SI': '建筑材料',
    '801720.SI': '建筑装饰',
    '801730.SI': '电气设备',
    '801740.SI': '国防军工',
    '801750.SI': '计算机',
    '801760.SI': '传媒',
    '801770.SI': '通信',
    '801780.SI': '银行',
    '801790.SI': '非银金融',
    '801880.SI': '汽车',
    '801890.SI': '机械设备',
    'R001.CM': 'cash'
}

# e.g.3 传入任意组合及因子分组
factor_map3 = {
    '000918.CSI': 'stock',
    'R001.CM': 'cash',
    '801770.SI': '通信',
}
factor_ids = factor_map3


# 构造数据类
# 需要实现`return_series`接口
class MutualFund:
    @property
    def return_series(self):
        # construct your own return_series
        dr = pd.date_range(start, end, freq="B")
        test = pd.Series(np.random.random((len(dr),)), dr)
        return test


# 自定义约束, 需要实现`initialize`方法, 更新_cons和_bnds属性;
class CustomCons(fs.BaseConstraints):
    def initialize(self):
        # e.g. 根据业务需求, 设置约束条件
        self._cons.append({"type": "eq", "fun": lambda x: sum(x) - 1})  # 各因子优化权重之和为1
        self._bnds = [(0, 1)] * len(factor_ids)  # 各因子权重在0, 1之间

        self.initialized = True


# 夏普归因, 默认无约束条件, 如果需要设置约束条件, 需要实现constraints属性, 返回BaseConstraints
class MutSharpeFactor(fs.SharpeFactor):
    @property
    def constraints(self):
        cons = CustomCons()  # 初始化约束类
        return cons


def main():
    fund = MutualFund()  # 初始化投资组合
    factors = fs.Factors(factor_ids, start, end, "d")  # 初始化因子序列

    res_proxy = MutSharpeFactor(fund, factors)  # 归因;

    # 返回结果
    res_proxy.factors.mtx_ids  # 所有因子ID(与矩阵形式对应)
    res_proxy.factors.factor_map  # 所有因子对应分类
    res_proxy.solver.x  # 所有因子权重
    res_proxy.p_value  # 所有因子P值
    res_proxy.rsquare  # 拟合优度
    res_proxy.ret_contri_ratio  # 收益贡献率
    res_proxy.rsk_contri_ratio  # 风险贡献率


if __name__ == "__main__":
    main()
