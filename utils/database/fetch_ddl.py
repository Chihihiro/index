import pandas as pd
from sqlalchemy.engine import reflection
from utils.database import config as cfg
from openpyxl import load_workbook
from utils.script import scriptutils as su
import os


def get_ddl(schema, engine):
    su.tic("fetching ddl...")
    insp = reflection.Inspector.from_engine(engine)
    tb_names = insp.get_table_names(schema)

    cols = {tb: [] for tb in tb_names}
    pks = {tb: [] for tb in tb_names}
    for tb_name in tb_names:
        cols[tb_name] = insp.get_columns(table_name=tb_name, schema=schema)
        pks[tb_name] = insp.get_primary_keys(table_name=tb_name, schema=schema)

    for k, v in cols.items():
        tb = pd.DataFrame.from_dict(v)
        tb = tb[["name", "type", "nullable", "default"]]
        cols[k] = tb

    for k, v in pks.items():
        cols[k]["is_pk"] = cols[k]["name"].apply(lambda x: x in v)
        cols[k]["is_pk"] = cols[k]["is_pk"].astype(bool)
        # cols[k]["is_pk"] = cols[k]["is_pk"].apply(lambda x: int(x))
        # cols[k]["is_pk"] = cols[k]["is_pk"].astype(int)
        cols[k].loc[cols[k]["is_pk"] == False, "is_pk"] = None

    return cols


def get_cmt():
    su.tic("fetching comments...")

    dfs = pd.read_excel("D:\Work\Others\项目\国泰君安项目\国泰君安项目表结构文件\私募产品数据库表结构（基础表）4.0版本.xls", sheetname=None, header=2)
    cmt_dict = {}
    for k, v in dfs.items():
        v = v.drop(v.columns[6:], axis=1)
        v.columns = ["name", "name_sc", "type", "is_pk", "default", "comment"]
        dfs[k] = v
        tmp = dict(zip(v["name"], list(zip(v["name_sc"], v["comment"]))))
        cmt_dict.update(tmp)
    cmt_dict["year_persistence"] = cmt_dict["year_persietance"]
    cmt_dict["total_persistence"] = cmt_dict["total_persietance"]
    cmt_dict["m3_persistence"] = cmt_dict["m3_persietance"]
    cmt_dict["m6_persistence"] = cmt_dict["m6_persietance"]
    cmt_dict["y1_persistence"] = cmt_dict["y1_persietance"]
    cmt_dict["y2_persistence"] = cmt_dict["y2_persietance"]
    cmt_dict["y3_persistence"] = cmt_dict["y3_persietance"]
    cmt_dict["y5_persistence"] = cmt_dict["y5_persietance"]

    return cmt_dict


def get_dictvalue(dictionary, key, idx):
    try:
        return dictionary[key][idx]
    except KeyError:
        return None


def merge(ddls, cmts):
    su.tic("merging results...")
    for k, v in ddls.items():
        v["name_sc"] = v["name"].apply(lambda x: get_dictvalue(cmts, x, 0))
        v["comments"] = v["name"].apply(lambda x: get_dictvalue(cmts, x, 1))
        ddls[k] = v[["name", "name_sc", "type", "default", "nullable", "comments", "is_pk"]]
    return ddls


def export(df_dict, file_name="ddls", path=su.get_desktop_path()):
    su.tic("exporting...")
    file_path = os.path.join(path, file_name)
    if ".xlsx" not in file_path.lower():
        file_path += ".xlsx"
    tmp = pd.DataFrame()
    tmp.to_excel(file_path, index=False)

    dict_items = sorted(df_dict.items(), key=lambda d: d[0], reverse=False)

    # for k, v in df_dict.items():
    for k, v in dict_items:
        book = load_workbook("{path}".format(path=file_path))
        writer = pd.ExcelWriter("{path}".format(path=file_path), engine='openpyxl')
        writer.book = book
        writer.sheets = dict((ws.title, ws) for ws in book.worksheets)
        v = v.fillna("")
        v = v.astype(str)
        v.to_excel(writer, "{tb_name}".format(tb_name=k, index=False), index=False)
        writer.save()


def get_source():
    server_name = input("choose a server...\n   optional: {1G, 2G, 4G}\n>>>")
    db_name = input("choose a DB and export_to_xl ddl of tables in it...\n>>>")
    file_name = input("input your file_name...\n>>>")
    if file_name == "":
        file_name = "{0}_{1}".format(server_name, db_name)

    return server_name, db_name, file_name


def main():
    server_name, db_name, file_name = get_source()
    engine_read = cfg.load_engine()[server_name]
    ddls = get_ddl(db_name, engine_read)
    cmts = get_cmt()
    dfs = merge(ddls, cmts)
    export(dfs, file_name=file_name)
    su.tic("done...")


if __name__ == "__main__":
    main()
