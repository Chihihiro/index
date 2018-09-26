# coding:utf-8

import pandas as pd
import numpy as np
import datetime
from sqlalchemy import create_engine
from utils.database import config as cfg

engine_rd = cfg.load_engine()["2Gb"]


def calculate_swanav():
    sql_ids = "SELECT DISTINCT fund_id FROM fund_allocation_data WHERE fund_id IN (SELECT DISTINCT fund_id FROM fund_nv_data_standard WHERE swanav IS NULL AND nav <> added_nav AND nav IS NOT NULL AND added_nav IS NOT NULL) AND source_code = 0 AND statistic_date IS NOT NULL "
    id_list = pd.read_sql(sql_ids, engine_rd)['fund_id'].tolist()
    if len(id_list) != 0:
        idset = []
        for fund_id in id_list:
            idset.append("'{}'".format(fund_id))
        idset = ','.join(idset)
        sql_allo = "SELECT fund_id,statistic_date,after_tax_bonus,split_ratio FROM fund_allocation_data WHERE fund_id IN ({}) AND source_code = 0".format(idset)
        sql_nav = "SELECT fund_id,statistic_date,nav,added_nav FROM fund_nv_data_standard WHERE fund_id IN ({}) AND nav IS NOT NULL AND added_nav IS NOT NULL".format(idset)
        print('{} Getting Data!'.format(datetime.datetime.now().strftime("%H:%M:%S")))
        allo = pd.read_sql(sql_allo, engine_rd)
        nav = pd.read_sql(sql_nav, engine_rd)
        print('{} Data Get!'.format(datetime.datetime.now().strftime("%H:%M:%S")))
        df_nav = nav.merge(allo, how='outer', on=['fund_id', 'statistic_date'])
        df_nav['after_tax_bonus'] = df_nav['after_tax_bonus'].fillna(0)
        df_nav['split_ratio'] = df_nav['split_ratio'].fillna(1)
        df_nav['nav'] = df_nav['nav'].fillna(-100)
        df_nav = df_nav.sort_values(['fund_id', 'statistic_date'])
        df_nav.index = range(len(df_nav))
        nan_index = df_nav[(df_nav['nav'] == -100)].index
        nan_ind_list = sorted(list(nan_index))
        drop_id_list = []
        for i in range(len(nan_ind_list) - 1):
            if nan_ind_list[i] == nan_ind_list[i + 1] - 1:
                drop_id_list.append(df_nav['fund_id'][nan_ind_list[i]])
        drop_list = list(set(drop_id_list))
        nan_al_sr = df_nav.loc[nan_index][['after_tax_bonus', 'split_ratio']]
        df_nav.loc[nan_index - 1, ['after_tax_bonus']] = nan_al_sr['after_tax_bonus'].tolist()
        df_nav.loc[nan_index - 1, ['split_ratio']] = nan_al_sr['split_ratio'].tolist()
        df_nav = df_nav.drop(nan_index)
        df_nav.index = df_nav['fund_id'].tolist()
        df_nav = df_nav.drop(drop_list)
        df_nav['swanav'] = 0
        for ids in id_list:
            if ids in drop_list:
                continue
            df_ids = df_nav[ids:ids]
            nv = df_ids['nav'].as_matrix()
            bonus = df_ids['after_tax_bonus'].as_matrix()
            ratio = df_ids['split_ratio'].as_matrix()
            temp_ratio = np.multiply(np.divide(bonus, nv) + 1, ratio)
            swanav = np.multiply(nv, np.multiply.accumulate(temp_ratio))
            df_nav.loc[ids, ['swanav']] = swanav
        drop_index = list(set(check_nav(df_nav)))
        df_nav = df_nav.drop(drop_index)
        df_nav = df_nav[['fund_id', 'statistic_date', 'swanav']]
        print('{} Done Calculation!'.format(datetime.datetime.now().strftime("%H:%M:%S")))
    else:
        df_nav = []
    return df_nav


def check_nav(df):
    return df.loc[(df['nav'] != df['added_nav']) & (df['nav'] == df['swanav'])].index


if __name__ == "__main__":
    df = calculate_swanav()
    print(df)
