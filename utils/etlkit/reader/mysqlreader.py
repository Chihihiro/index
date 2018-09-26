import pandas as pd
from sqlalchemy.orm.query import Query


class MysqlInput:
    def __init__(self, bind, stmt):
        self._stmt = stmt
        self._bind = bind

    def _read_sql(self):
        if type(self._stmt) is str:
            return pd.read_sql(self._stmt, self._bind)
        elif type(self._stmt) is Query:
            return pd.read_sql(self._stmt.statement, self._bind)
        else:
            raise TypeError("Bad `stmt` type")

    @property
    def dataframe(self):
        if not hasattr(self, "_dataframe"):
            self.__setattr__("_dataframe", self._read_sql())
        return self._dataframe


class MysqlNativeInput(MysqlInput):
    def _read_sql(self):
        return pd.read_sql(self._stmt, self._bind)


class MysqlOrmInput(MysqlInput):
    def _read_sql(self):
        return pd.read_sql(self._stmt.statement, self._bind)


class Output:
    def __init__(self):
        pass
