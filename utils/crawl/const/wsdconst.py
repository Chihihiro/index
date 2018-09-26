
class SWS:
    names = {
        "110000": "农林牧渔",
        "210000": "采掘",
        "220000": "化工",
        "230000": "钢铁",
        "240000": "有色金属",
        "270000": "电子",
        "280000": "汽车",
        "330000": "家用电器",
        "340000": "食品饮料",
        "350000": "纺织服装",
        "360000": "轻工制造",
        "370000": "医药生物",
        "410000": "公用事业",
        "420000": "交通运输",
        "430000": "房地产",
        "450000": "商业贸易",
        "460000": "休闲服务",
        "480000": "银行",
        "490000": "非银金融",
        "510000": "综合",
        "610000": "建筑材料",
        "620000": "建筑装饰",
        "630000": "电气设备",
        "640000": "机械设备",
        "650000": "国防军工",
        "710000": "计算机",
        "720000": "传媒",
        "730000": "通信",
    }

    type = {
        "210000": "6102000000000000",
        "220000": "6103000000000000",
        "230000": "6104000000000000",
        "240000": "6105000000000000",
        "610000": "6106010000000000",
        "620000": "6106020000000000",
        "630000": "6107010000000000",
        "640000": "6107000000000000",
        "650000": "1000012579000000",
        "280000": "1000012588000000",
        "330000": "6111000000000000",
        "350000": "6113000000000000",
        "360000": "6114000000000000",
        "450000": "6120000000000000",
        "110000": "6101000000000000",
        "340000": "6112000000000000",
        "460000": "6121000000000000",
        "370000": "6115000000000000",
        "410000": "6116000000000000",
        "420000": "6117000000000000",
        "430000": "6118000000000000",
        "270000": "6108000000000000",
        "710000": "1000012601000000",
        "720000": "6122010000000000",
        "730000": "1000012611000000",
        "480000": "1000012612000000",
        "490000": "1000012613000000",
        "510000": "6123000000000000",
    }

    price = {
        "110000": "801010.SI",
        "210000": "801020.SI",
        "220000": "801030.SI",
        "230000": "801040.SI",
        "240000": "801050.SI",
        "270000": "801080.SI",
        "280000": "801880.SI",
        "330000": "801110.SI",
        "340000": "801120.SI",
        "350000": "801130.SI",
        "360000": "801140.SI",
        "370000": "801150.SI",
        "410000": "801160.SI",
        "420000": "801170.SI",
        "430000": "801180.SI",
        "450000": "801200.SI",
        "460000": "801210.SI",
        "480000": "801780.SI",
        "490000": "801790.SI",
        "510000": "801230.SI",
        "610000": "801710.SI",
        "620000": "801720.SI",
        "630000": "801730.SI",
        "640000": "801890.SI",
        "650000": "801740.SI",
        "710000": "801750.SI",
        "720000": "801760.SI",
        "730000": "801770.SI",
    }


class Common:
    class Base:
        class Stock:
            class Info:
                sec_name = ("sec_name", "name")

    class StockCapital:
        class Quantity:
            free_float_shares = ("free_float_shares", "free_float_shares")

    class MarketQuot:
        class CloseQuot:
            lastradeday_s = ("lastradeday_s", "last_trading_day")
            close = ("close", "close")
            trade_status = ("trade_status", "status")
            amt = ("amt", "trans_amount")

    class Analysis:
        class StockValuation:
            class Common:
                mkt_cap_ard = ("mkt_cap_ard", "market_price")  # 总市值2

                pe_ttm = ("pe_ttm", "pe_ttm")  # TTM
                val_pe_deducted_ttm = ("val_pe_deducted_ttm", "pe_deducted_ttm")  # TTM 扣除非经常性损益
                pe_lyr = ("pe_lyr", "pe_lyr")  # LYR

                pb_lf = ("pb_lf", "pb_lf")
                pb_mrq = ("pb_mrq", "pb_mrq")

            class Corporate:
                mkt_cap_float = ("mkt_cap_float", "circulated_price")  # 流通市值

            class Unrepeatable:
                pb = ("pb", "pb")

        class Tech:
            class Indicator:
                atr = ("ATR", "atr")

        class Risk:
            class StandardParam:
                beta_24m = ("beta_24m", "beta_24m")

    class Finance:
        class Analysis:
            class TTM:
                roe_ttm = ("roe_ttm", "roe_ttm")
                roe_ttm2 = ("roe_ttm2", "roe_ttm2")

            class GrowthAbility:
                class Yoy:
                    yoyeps_basic = ("yoyeps_basic", "yoyeps_basic")

            class DebtAbility:
                longdebttodebt = ("longdebttodebt", "longdebttodebt")

        class ReportCN:
            class AccountStatement:
                class Asset:
                    tot_assets = ("tot_assets", "tot_assets")

                class Liability:
                    tot_liab = ("tot_liab", "tot_liab")

                class Equity:
                    cap_stk = ("cap_stk", "cap_stk")

    class Predict:
        class WindConsist:
            west_netprofit_CAGR = ("west_netprofit_CAGR", "west_netprofit_CAGR")


class CommonQuery:
    S_BASEINFO = dict([
        Common.Base.Stock.Info.sec_name,
    ])

    D_STOCKPRICE = dict([
        Common.MarketQuot.CloseQuot.lastradeday_s,
        Common.MarketQuot.CloseQuot.close,
        # Common.MarketQuot.CloseQuot.trade_status,
    ])

    D_STOCKTURNOVER = dict([
        Common.MarketQuot.CloseQuot.amt
    ])


    # fama因子采集
    D_STOCKVAL = dict([
        Common.Analysis.StockValuation.Common.mkt_cap_ard,

        Common.Analysis.StockValuation.Common.pe_ttm,
        Common.Analysis.StockValuation.Common.val_pe_deducted_ttm,
        Common.Analysis.StockValuation.Common.pe_lyr,

        Common.Analysis.StockValuation.Corporate.mkt_cap_float,

        Common.Analysis.StockValuation.Unrepeatable.pb,
        Common.Analysis.StockValuation.Common.pb_lf,
        # Common.Analysis.StockValuation.Common.pb_mrq,
        Common.Analysis.Risk.StandardParam.beta_24m,
    ])

    D_STOCK_CAPITAL = dict([
        Common.StockCapital.Quantity.free_float_shares  # unit=1
    ])

    D_STOCK_TECH = dict([
        Common.Analysis.Tech.Indicator.atr  # ATR_N=14;ATR_IO=1
    ])

    S_STOCK_OTHERS = dict([
        Common.Finance.Analysis.TTM.roe_ttm2,  # unit=1;rptType=1;Period=Q;Days=Alldays
        Common.Finance.Analysis.GrowthAbility.Yoy.yoyeps_basic,  # unit=1;rptType=1;Period=Q;Days=Alldays
        Common.Finance.ReportCN.AccountStatement.Asset.tot_assets,  # unit=1;rptType=1;Period=Q;Days=Alldays
        Common.Finance.ReportCN.AccountStatement.Liability.tot_liab,  # unit=1;rptType=1;Period=Q;Days=Alldays
        Common.Finance.ReportCN.AccountStatement.Equity.cap_stk,  # unit=1;rptType=1;Period=Q;Days=Alldays
        Common.Finance.Analysis.DebtAbility.longdebttodebt  # unit=1;rptType=1;Period=Q;Days=Alldays
    ])

