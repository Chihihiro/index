from multiprocessing.dummy import Pool as ThreadPool
from multiprocessing import Pool
import pandas as pd
from utils.database import config as cfg, io
from sqlalchemy import create_engine

engine = cfg.load_engine()["2Gb"]

engine_crawl_backup = create_engine(
    "mysql+pymysql://{}:{}@{}:{}/{}".format('sm_data_qxd', '59bGeT374w4j', '182.254.128.241', 4119, 'crawl_backup', ),
    connect_args={"charset": "utf8"}, echo=True, )




def fetch_nv(fids):
    # fids = str(tuple(fids))
    fids = "'"+'\',\''.join(fids)+"'"
    print(fids)
    sql = "SELECT person_id,resume FROM base.`person_description`" \
          "WHERE person_id in ({fids})".format(
        fids=fids
    )
    df = pd.read_sql(sql, engine)
    print(df)
    io.to_sql('base.person_info', engine_crawl_backup, df, type="update")





def main():
    # all_ids = ["0" * (6 - len(str(i))) + str(i) for i in range(100)]
    # ids_list = pd.read_sql("SELECT DISTINCT person_id FROM base.`person_description`", engine)
    # all_ids = ids_list["person_id"].tolist()
    all_ids = range(300)
    STEP = 10
    sliced = [all_ids[i: i+STEP] for i in range(0, len(all_ids), STEP)]
    print(sliced)
    pool = ThreadPool(6)
    p2 = Pool(2)
    result = pool.map(fetch_nv, sliced)


if __name__ == "__main__":
    main()



class Student(object):

    def __init__(self, num):
        self.num = num

    @property
    def score(self):
        return self.num

    @score.setter
    def score(self, value):
        if not isinstance(value, int):
            raise ValueError('分数必须是整数才行呐')
        if value < 0 or value > 100:
            raise ValueError('分数必须0-100之间')
        self.num = value
