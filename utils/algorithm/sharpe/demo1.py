import datetime as dt

from utils.algorithm.sharpe.factors_sharpe_pri import PriSharpeFactor

# 私募实时计算
#### 参数1 ####
fund_id = "JR000001"
start, end = dt.date(2017, 10, 1), dt.date(2018, 4, 30)
freq = "w"
factor_type = "style"  # 夏普风格类型


def main():
    # 接口调用
    res_proxy = PriSharpeFactor(fund_id, start, end, freq, factor_type)

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
