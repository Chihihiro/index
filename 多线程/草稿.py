import datetime as dt
import pandas as pd
from utils.database import config as cfg, io
from sqlalchemy import create_engine


ENGINE_GM = cfg.load_engine()["2Gbp"]
ENGINE_RD = cfg.load_engine()["2Gb"]
ENGINE_8612_gm = cfg.load_engine()["8612app_mutual"]

engine_data_test = create_engine(
    "mysql+pymysql://{}:{}@{}:{}/{}".format('jr_admin_qxd', 'jr_admin_qxd', '182.254.128.241', 4171, 'data_test', ),
    connect_args={"charset": "utf8"}, echo=True, )


tasks = [
    # ["fund_daily_return", ENGINE_GM, "fund_daily_return", ENGINE_8612_gm],
    # ["fund_daily_risk", ENGINE_GM, "fund_daily_risk", ENGINE_8612_gm],
    # ["fund_daily_risk2", ENGINE_GM, "fund_daily_risk2", ENGINE_8612_gm]
    [""]
    ["fund_manager_mapping", ENGINE_GM, "fund_manager_mapping", ENGINE_8612_gm]
    ["market_index", ENGINE_RD, "market_index", engine_data_test]
         ]


class sync_same_table:
    def __init__(self, tb1, Engine1, tb2, Engine2, chunck_size=5):
        self.new = tb1
        self.engine_new = Engine1
        self.old = tb2
        self.engine_old = Engine2
        self.chunck_size = chunck_size
        self.now = dt.datetime.now()
        self.diff_index = []
        self.diff_data = []
        self.del_index = []
        self.del_data = []

    def _get_id(self):
        col = pd.read_sql("SELECT column_name FROM INFORMATION_SCHEMA.`KEY_COLUMN_USAGE`"
                          "WHERE table_name='{}' "
                          "AND CONSTRAINT_SCHEMA='base' "
                          "AND constraint_name='PRIMARY'".format(self.new), self.engine_new)
        col.loc[len(col)] = {'column_name': 'update_time'}
        a = ",".join(col['column_name'].tolist())
        self.target_column = a
        self.target_index = col['column_name'].tolist()

    def get_new_index(self):

        sql_com = "select {} from {}".format(self.target_column, self.new)
        self.new_index = pd.read_sql(sql_com, self.engine_new)
        self.new_index[self.target_index[0]] = self.new_index[self.target_index[0]].apply(lambda x: str(x))
        return self.new_index

    def get_old_index(self):
        sql_com = "select {} from {}".format(self.target_column, self.old)
        self.old_index = pd.read_sql(sql_com, self.engine_old)
        self.old_index[self.target_index[0]] = self.old_index[self.target_index[0]].apply(lambda x: str(x))
        return self.old_index

    def get_new_data(self, ii):
        str_list = "'" + '\',\''.join(ii) + "'"
        sql_com = "select * from {} where {} in ({})".format(self.new, self.first_index, str_list)
        self.new_data = pd.read_sql(sql_com, self.engine_new)
        return self.new_data

    def get_old_data(self, ii):
        str_list = "'" + '\',\''.join(ii) + "'"
        sql_com = "select * from {} where {} in ({})".format(self.old, self.first_index, str_list)
        self.new_data = pd.read_sql(sql_com, self.engine_old)
        return self.new_data

    def delete(self, tb_name, conn, dataframe):
        dataframe = dataframe.dropna(how='all').drop_duplicates()
        for i in range(len(dataframe)):
            df = dataframe.iloc[i, :]
            c = pd.DataFrame(df).T
            condition = self.generate_condition(c)
            sql = "DELETE FROM {tb} WHERE {criterion}".format(
                tb=tb_name, criterion=condition
            )
            conn.execute(sql)

    def to_db(self, dataframe):
        dataframe = dataframe.dropna(how='all').drop_duplicates()
        for i in range(len(dataframe)):
            df = dataframe.iloc[i, :]
            c = pd.DataFrame(df).T
            condition = self.generate_condition(c)
            sql = "SELECT * FROM {tb} WHERE {criterion}".format(
                tb=self.new, criterion=condition
            )
            df = pd.read_sql(sql, self.engine_new)
            print(df)
            io.to_sql(self.old, self.engine_old, df, type="update")


    def generate_condition(self, dataframe):
        index = self.target_index[:-1]
        dataframe = dataframe.astype(str)
        del_target = dataframe[index].values.tolist()
        condition = []
        for i in range(len(del_target)):
            if i == 0:
                condition.append("{} = '{}' ".format(self.target_index[0], del_target[0][0]))
            else:
                condition.append("and {} = '{}' ".format(self.target_index[i], del_target[i][0]))

        condition_str = ' '.join(condition)
        return condition_str

    def sync(self):
        if not hasattr(self, 'target_column'):
            self._get_id()
        self.first_index = self.target_index[0]

        new_index = self.get_new_index()
        old_index = self.get_old_index()
        new_index.rename(columns={'update_time': 'n_time'}, inplace=True)
        old_index.rename(columns={'update_time': 'o_time'}, inplace=True)
        index = self.target_index[:-1]
        chunck_list = new_index[self.first_index].drop_duplicates().values.tolist()
        chunck_list = [chunck_list[i:i + self.chunck_size] for i in range(0, len(chunck_list), self.chunck_size)]
        for ii in chunck_list:
            print(ii)
            tn_ind = new_index[new_index[self.first_index].isin(ii)]
            tn_ind.set_index(index, inplace=True)
            to_ind = old_index[old_index[self.first_index].isin(ii)]
            to_ind.set_index(index, inplace=True)
            con_index_data = pd.merge(tn_ind, to_ind, left_index=True, right_index=True, how='left')
            diff_index = con_index_data[con_index_data.n_time != con_index_data.o_time]
            del_index_data = pd.merge(tn_ind, to_ind, left_index=True, right_index=True, how='right')
            del_index = del_index_data[del_index_data.n_time.isnull()]
            if diff_index.empty and del_index.empty:
                continue
            if not diff_index.empty:
                df_new = self.get_new_data(ii)
                new_data = df_new.set_index(index)
                self.diff_index.append(diff_index)
                diff_data = pd.merge(diff_index, new_data, left_index=True, right_index=True, how='left')
                diff_data.reset_index(inplace=True)
                del diff_data['o_time']
                del diff_data['n_time']
                self.diff_data.append(diff_data)

                df_old = self.get_old_data(ii)
                old_col = df_old.columns.values.tolist()
                new_col = df_new.columns.values.tolist()
                same = [l for l in old_col if l in new_col]
                last_df = diff_data.loc[:, same]

                self.to_db(last_df)

            if not del_index.empty:
                old_data = self.get_old_data(ii).set_index(index)
                del_data = pd.merge(del_index, old_data, left_index=True, right_index=True, how='left')
                del_data.reset_index(inplace=True)
                del del_data['o_time']
                del del_data['n_time']
                self.delete(self.old, self.engine_old, del_data)
                self.del_data.append(del_data)


# def test():
#     gg = sync_same_table('fund_info', ENGINE_RD, 'fund_info', engine_data_test, chunck_size=100)
#     gg.sync()
#     print('finish')


if __name__ == '__main__':
    for i in tasks:
        gg = sync_same_table(i[0], i[1], i[2], i[3], chunck_size=100)
        gg.sync()
        print('finish')


