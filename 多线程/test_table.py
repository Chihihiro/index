from multiprocessing.dummy import Pool as ThreadPool
from multiprocessing import Pool
import pandas as pd
from utils.database import config as cfg, io
from sqlalchemy import create_engine
from sqlalchemy import create_engine
engine = cfg.load_engine()["2Gb"]
engine_test_gt = cfg.load_engine()["8612test_sync"]






def fetch_nv(fids):
    # fids = str(tuple(fids))
    fids = "'"+'\',\''.join(fids)+"'"
    print(fids)
    sql = "select org_id, org_name, org_name_en, org_name_py, org_full_name, \
       org_category, found_date, base_date, org_code, reg_code, \
       reg_time, is_reg_now, is_member, member_type, master_strategy, \
       fund_num, manage_type, \
       asset_mgt_scale, property, reg_capital, real_capital, \
       real_capital_proportion, legal_person, is_qualified, \
       qualifying_way, region, prov, city, area, address, \
       reg_address, org_web, final_report_time, currency, \
       initiation_time, law_firm_name, lawyer_name, phone, profile, \
       legal_person_resume, prize, team, major_shareholder, \
       shareholder_structure, special_tips, investment_idea, email, \
       entry_time, update_time FROM org_info WHERE org_id in ({fids})".format(fids=fids)
    # sql = "SELECT * FROM base.org_info " \
    #       "WHERE org_id in ({fids})".format(
    #     fids=fids
    # )
    df = pd.read_sql(sql, engine)
    df.rename(columns={"fund_total_num": "total_fund_num"}, inplace=True)
    print(df)
    io.to_sql('test_gt.org_info', engine_test_gt, df, type="update")





def main():
    # all_ids = ["0" * (6 - len(str(i))) + str(i) for i in range(100)]
    ids_list = pd.read_sql("SELECT DISTINCT org_id FROM base.org_info", engine)
    all_ids = ids_list["org_id"].tolist()
    STEP = 10
    sliced = [all_ids[i: i+STEP] for i in range(0, len(all_ids), STEP)]
    print(sliced)


    pool = ThreadPool(6)
    p2 = Pool(2)

    result = pool.map(fetch_nv, sliced)


if __name__ == "__main__":
    main()
