from sqlalchemy.engine import reflection
from utils.database import config as cfg, io
import pandas as pd
import re

_SUBPATTS = {
    "column_name": "(?P<column_name>`.*?`)",
    "column_type": "(?P<column_type>.*?[ unsigned]?)(?=\s)",
    "nullable": "(NOT NULL)?",
    "default": "(?P<default>DEFAULT .*?)?",
    "comment": "(?P<comment>COMMENT '.*?')?"
}

_PATT = "{column_name}\s{column_type}\s?{nullable}\s?{default}\s?{comment},".format(**_SUBPATTS)


def get_schema_names(bind):
    insp = reflection.Inspector.from_engine(bind)
    schema_names = insp.get_schema_names()
    return schema_names


def get_table_names(bind):
    insp = reflection.Inspector.from_engine(bind)
    table_names = insp.get_table_names()
    return sorted(table_names)


def fetch_ddl(bind):
    """
        Fetch ddl string of all tables of the binding engine by using regex defined in `_PATT`;
    Args:
        bind:

    Returns:
        dict of str
        e.g.
        {"table_name": ddl_str}
    """
    table_names = get_table_names(bind)
    result = {}
    for schema_name in table_names:
        sql = "SHOW CREATE TABLE `{schema_name}`".format(schema_name=schema_name)
        result[schema_name] = pd.read_sql(sql, bind).ix[0, "Create Table"].replace("\n", "")
    return result


def parse(bind):
    """
        Parse ddl of all tables of the binding engine by using regex defined in `_PATT`;
    Args:
        bind: db engine instance

    Returns:
        dict of pandas.DataFrame
        e.g.
        {"table_name": dataframe}
    """
    result = {}
    ddl_dict = fetch_ddl(bind)
    for table_name, ddl in ddl_dict.items():
        try:
            patt_prefix = "(?<=CREATE TABLE `{tb_name}` )(?P<cols>.*?)  PRIMARY KEY \((?P<constraint>.*?)\)".format(tb_name=table_name)
            gdict = re.search(patt_prefix, ddl).groupdict()
            constraints = set(gdict["constraint"].split(","))
            matched = re.findall(_PATT, gdict["cols"])
            df = pd.DataFrame(matched)
            df.columns = ["name", "type", "nullable", "default", "comments"]
            df.ix[df["name"].apply(lambda x: x in constraints), "is_pk"] = 1
            df["name"] = df["name"].apply(lambda x: x.replace("`", ""))
            df["default"] = df["default"].apply(lambda x: x.replace("NULL", ""))
            df["nullable"] = df["nullable"].apply(lambda x: {"NOT NULL": False}.get(x, True))
            df["comments"] = df["comments"].apply(lambda x: re.match("COMMENT '(.*)'", x))
            df["comments"] = df["comments"].apply(lambda x: x.groups()[0] if x is not None else "")
            result[table_name] = df
        except Exception as e:
            print(e, table_name, ddl)
    return result


def main():
    db = input("choose a db...")
    engine = cfg.load_engine()[db]
    df_dict = parse(engine)
    for tb, df in df_dict.items():
        print(tb)
        df["comments"] = df["comments"].apply(lambda x: re.search("(?P<name_2>.*?)[:|：]\s*\{(?P<comment>.*)\}|(?P<name_1>.*)", x).groupdict())
        df["name_sc_1"] = df["comments"].apply(lambda x: x["name_1"])
        df["name_sc_2"] = df["comments"].apply(lambda x: x["name_2"])
        df["name_sc"] = df["name_sc_1"].fillna(df["name_sc_2"])
        df["comments"] = df["comments"].apply(lambda x: x["comment"])
        df["default"] = df["default"].apply(lambda x: x.replace("DEFAULT ", ""))
        del df["name_sc_1"], df["name_sc_2"]
        df = df.fillna("")
        df = df.astype(str)
        df = df[["name", "name_sc", "type", "nullable", "default", "comments", "is_pk"]]
        df_dict[tb] = df

    io.export_to_xl(df_dict, db)


if __name__ == "__main__":
    main()
