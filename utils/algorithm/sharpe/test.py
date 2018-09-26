import datetime as dt
from importlib import reload

from utils.algorithm.sharpe import factors_sharpe_pri

reload(factors_sharpe_pri)


def pprint(sf):
    r = factors_sharpe_pri.ResultProxy(sf)
    print("P-VALUE: \n", r.frame_pval, "\n")
    print("T-VALUE: \n", r.s.t_value, "\n")
    print("Coef: \n", r.frame_coef, "\n")
    print("Coef(Grouped): \n", r.frame_coef_grouped, "\n")
    print("Return Contri: \n", r.frame_retcontri, "\n")
    print("Return Contri Ratio: \n", sf.ret_contri_ratio, "\n")
    print("Risk Contri: \n: \n", r.frame_rskcontri, "\n")
    print("Risk Contri Ratio: \n", sf.rsk_contri_ratio, "\n")


def main():
    testcases = [
        ("JR113791", "w", dt.date(2012, 6, 1), dt.date(2013, 5, 31), "industry"),
        ("JR113791", "w", dt.date(2012, 11, 1), dt.date(2013, 5, 31), "style"),
        ("JR113791", "w", dt.date(2012, 6, 1), dt.date(2013, 5, 31), "industry"),
        ("JR000005", "w", dt.date(2016, 1, 1), dt.date(2016, 12, 31),  "style"),
        ("JR000007", "w", dt.date(2017, 1, 1), dt.date(2017, 11, 30),  "style"),
        ("JR001373", "w", dt.date(2016, 1, 1), dt.date(2016, 7, 15),  "style"),
        ("JR000005", "w", dt.date(2016, 10, 1), dt.date(2017, 4, 30),  "style"),
        ("JR000168", "w", dt.date(2015, 8, 1), dt.date(2016, 1, 31),  "style"),
        ("JR001330", "w", dt.date(2015, 11, 1), dt.date(2016, 4, 30),  "style"),
        ("JR000001", "w", dt.date(2017, 1, 1), dt.date(2017, 11, 30),  "style"),
        ("JR000001", "w", dt.date(2017, 1, 1), dt.date(2017, 11, 30), "industry"),
        ("JR000162", "w", dt.date(2014, 6, 1), dt.date(2014, 12, 31), "style")
    ]

    def f(*args):
        fid, freq, s, e, factor_type = args
        try:
            res = factors_sharpe_pri.PriSharpeFactor(fid, s, e, freq, factor_type, tol=1e-18, options={"maxiter": 1000})
            res.rsquare_truncated
            pprint(res)
        except factors_sharpe_pri.DataError as err:
            print(args, err)

    [f(*case) for case in testcases]


if __name__ == "__main__":
    main()

# def test_performance():
#     import os
#     import pstats
#
#     # Profile
#     opath = "c:/Users/Yu/Desktop/sp.out"
#     ipath = "D:/Projects/Python/Python35/FoF/utils/algorithm/stock/factors_sharpe.py"
#     cmd = "start python -m cProfile -o {opath} {ipath}".format(opath=opath, ipath=ipath)
#     os.system(cmd)
#
#     p = pstats.Stats(opath)
#     a = p.sort_stats("cumulative").print_stats()
#
#     # Graphviz with dot
#     ipath = "D:/Projects/Python/Python35/FoF/utils/algorithm/stock/factors_sharpe.py"
#     path_cprofile = "myLog.profile"
#     path_dotfile = "callingGraph.dot"
#     path_png = "c:/Users/Yu/Desktop/callingGraph.png"
#     s1 = "python -m cProfile -o \"{path_cprofile}\" \"{ipath}\"".format(path_cprofile=path_cprofile, ipath=ipath)
#     s2 = "gprof2dot -f pstats \"{path_cprofile}\" -o \"{path_dotfile}\"".format(path_cprofile=path_cprofile, path_dotfile=path_dotfile)
#     s3 = "dot -Tpng -o \"{path_png}\" \"{path_dotfile}\"".format(path_png=path_png, path_dotfile=path_dotfile)
#     for cmd in (s1, s2, s3):
#         # print(cmd)
#         os.system(cmd)
#
#     """
#     python -m cProfile -o myLog.profile D:/Projects/Python/Python35/FoF/utils/algorithm/stock/factors_sharpe.py
#     gprof2dot -f pstats myLog.profile -o callingGraph.dot
#     dot -Tpng -o"c:/Users/Yu/Desktop/callingGraph.png" callingGraph.dot
#     """


# def tp():
#     import pandas as pd
#
#
# def tf():
#     import pandas as pd
#     cols_map = ("H11062.CSI", "H11063.CSI", "H11064.CSI", "H11065.CSI", "H11066.CSI", "H11067.CSI", "H11068.CSI", "H11069.CSI", "R001.CM")
#     q = pd.read_csv("C:\\Users\\Yu\\Documents\\WeChat Files\\wxid_rc6z8uvrexqz21\\Files\\t6.csv", encoding="gbk")
#     q.columns = ["date", *q.columns[1:]]
#     q2 = q.set_index("date")
#     q2.columns = cols_map
#
#     q1 = sf.factors.return_series
#     (q1 - q2).abs().max()
