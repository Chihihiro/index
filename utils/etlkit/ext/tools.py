from utils.database import config as cfg
from utils.decofactory import common
from sqlalchemy.engine import reflection
from utils.database.sqlfactory import SQL
import pandas as pd
import re

ENGINE = cfg.load_engine()["2Gb"]


class TableComparer:
    def __init__(self, table_new, table_old, engine, engine_old=None, **kwargs):
        """

        Args:
            table_new: str
                "schema.table" 格式
            table_old: str
                "schema.table" 格式
            engine: sqlalchemy.engine.base.Engine
            engine_old: sqlalchemy.engine.base.Engine, default None
                if None, use `engine` instead

            **kwargs:
                col_map: dict
                    新旧表字段名映射, 如{"col_old_name": "col_new_name", ...}

                cols_included: set, or dict;
                cols_excluded: set, or dict
                    * `cols_inclueded`和`cols_excluded`字段不会同时生效
                    需要/不需要调取的字段名称, 如
                    {"update_time", "entry_time"},
                    或者:
                    {
                        "table_new": {"col1", "col2"},
                        "table_old": {col1, col2...}
                    }
        """

        self._schema_new, self._table_new = table_new.split(".")
        self._schema_old, self._table_old = table_old.split(".")
        self._engine_new, self._engine_old = [engine, engine_old or engine]

        self._col_map = kwargs.get("col_map")
        self._cols_included = kwargs.get("cols_included")
        self._cols_excluded = kwargs.get("cols_excluded")

    @classmethod
    def _clean_wrong(cls, dataframe):
        dataframe = dataframe.applymap(lambda x: re.sub("\s|-", "", x) if type(x) is str else x)
        dataframe = dataframe.applymap(lambda x: None if (type(x) is str and x == "") else x)
        return dataframe


    @property
    def col_map(self):
        if self._col_map:
            return self._col_map
        return {}

    @property
    def cols_in(self):
        if self._cols_included:
            if type(self._cols_included) is set:
                return {}.fromkeys([self._table_new, self._table_old], self._cols_included)
            elif type(self._cols_included) is dict:
                return self._cols_included
        return {}

    @property
    def cols_ex(self):
        if self._cols_excluded:
            if type(self._cols_excluded) is set:
                return {}.fromkeys([self._table_new, self._table_old], self._cols_excluded)
            elif type(self._cols_excluded) is dict:
                return self._cols_excluded
        return {}

    def cols_need(self, table, schema, engine):
        if self.cols_in:
            return self.cols_in[table]

        insp = reflection.Inspector.from_engine(engine)
        ddl_cols = insp.get_columns(table_name=table, schema=schema)
        total_cols = set([x["name"] for x in ddl_cols])
        if self.cols_ex:
            return total_cols - self.cols_ex[table]
        return total_cols

    def get_data(self, table, schema, engine):
        cols_need = self.cols_need(table, schema, engine)
        sql = "SELECT {cols} FROM {sche}.{tb}".format(cols=SQL.values4sql(cols_need, "column"), sche=schema, tb=table)
        return pd.read_sql(sql, engine)

    @classmethod
    def compare(cls, df_new, df_old):

        s1 = df_new.notnull().sum()
        s2 = df_old.notnull().sum()

        res_relpct_new = s1 / len(df_new)
        res_relpct_old = s2 / len(df_old)

        res_abspct = (s1 - s2) / s2
        res_relpct = res_relpct_new - res_relpct_old

        res = pd.DataFrame([s1, res_relpct_new, s2, res_relpct_old, res_relpct, res_abspct]).T
        res.columns = ["新表非空总数(N_1)", "新表非空占新表比例(PCT_1)", "旧表非空总数(N_2)", "旧表非空占旧表比例(PCT_2)", "占比相对变化(PCT_1 - PCT_2)", "非空记录数变化占比(N1 / N2 - 1)"]
        return res

    @property
    @common.inscache("_cache")
    def result(self):
        df_old = self.get_data(self._table_old, self._schema_old, self._engine_old)
        df_old.rename(columns=self.col_map, inplace=True)
        df_new = self.get_data(self._table_new, self._schema_new, self._engine_new)
        df_new, df_old = self._clean_wrong(df_new), self._clean_wrong(df_old)
        return self.compare(df_new, df_old)


def test():
    t = TableComparer("data_test.fund_info_test001", "base.fund_info", ENGINE,
                      cols_included={"currency", "fund_status", "is_umbrella_fund", "fund_custodian"})
    print(t.result)
