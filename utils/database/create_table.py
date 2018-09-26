import pandas as pd
from utils.database import config as cfg


def get_tables(file):
    tb_dict = pd.read_excel(file, sheetname=None)
    tbs = list(tb_dict.keys())
    tbs.sort()
    tbs = {k: v for k, v in zip(range(len(tbs)), tbs)}
    return tb_dict, tbs


def choose_table(tbs):
    for k in tbs.keys():
        print("{idx}: {tb}".format(idx=k, tb=tbs[k]))

    idxs = input(
        "choose table to create...\n    optional:{`1`-`9`to create specified table; `-1` to create all tables}")

    if int(idxs) == -1:
        tb_names_used = [tbs[idx] for idx in tbs.keys()]
    elif int(idxs) in range(len(tbs)):
        tb_names_used = [tbs[int(idxs)]]

    return tb_names_used


def sql_parser(tb_name, tb_df):
    cols = ["name", "name_sc", "type", "default", "nullable", "comments", "is_pk"]
    tb_df[cols[3]] = tb_df[cols[3]].fillna("")
    tb_df[cols[3]] = tb_df[cols[3]].astype(str)
    tb_df.loc[tb_df[cols[4]] == False, cols[4]] = "NOT NULL"
    tb_df.loc[tb_df[cols[4]] == True, cols[4]] = ""
    tb_df[cols[5]] = tb_df[cols[5]].fillna("")
    tb_df[cols[5]] = tb_df[cols[5]].apply(lambda x: x.replace("%", "%%"))
    tb_df[cols[6]] = tb_df[cols[6]].fillna("")

    names, name_scs, types, defaults, nullables, comments, is_pks = [tb_df[col].tolist() for col in cols]
    cols = zip(*[names, name_scs, types, defaults, nullables, comments, is_pks])
    sql_cols = ""
    sql_pks = "PRIMARY KEY " + str(tuple(tb_df.loc[tb_df["is_pk"] == 1, "name"])).replace("'", "`")
    if sql_pks[-2] == ",":
        sql_pks = sql_pks[:-2] + sql_pks[-1:]

    for name, name_sc, tp, default, nullable, comment, is_pk in cols:
        if default.lower() not in ("", "auto_increment"):
            default = "DEFAULT " + str(default)
        elif default.lower() == "auto_increment":
            pass

        if comment != "":
            comment = ":  {" + comment + "}"
        sql_cols += "`{name}` {type} {nullable} {default} COMMENT '{comment}', ".format(
            name=name,
            type=tp,
            nullable=nullable,
            default=default,
            comment="{n}{cmt}".format(n=name_sc, cmt=comment)
        )

    sql_create_table = "CREATE TABLE `{tb_name}` ({sql_cols}{sql_pks}) ENGINE=InnoDB DEFAULT CHARSET=utf8;".format(
        tb_name=tb_name,
        sql_cols=sql_cols,
        sql_pks=sql_pks
    )
    return sql_create_table


def main():
    db = input("choose a database to create...\n    optional {2Gt}\n>>>")
    ddl = input("input ddl excel...\n>>>")
    engine = cfg.load_engine()[db]
    conn = engine.connect()
    tb_dict, tbs = get_tables(ddl)
    tbs_to_create = choose_table(tbs)
    for tb in tbs_to_create:
        sql = sql_parser(tb, tb_dict[tb])
        try:
            conn.execute(sql)
        except Exception as e:
            print(tb)
            print(e)


if __name__ == "__main__":
    main()

# C:\\Users\\Yu\\Desktop\\2G_crawl_public.xlsx