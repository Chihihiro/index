import pymssql
import pandas as pd


class MSSQL:
    def __init__(self, host, user, pwd, db):
        self.host = host
        self.user = user
        self.pwd = pwd
        self.db = db

    def __GetConnect(self):
        if not self.db:
            raise (NameError, "没有设置数据库信息")
        self.conn = pymssql.connect(host=self.host, user=self.user, password=self.pwd, database=self.db, charset="utf8")
        cur = self.conn.cursor()
        if not cur:
            raise (NameError, "连接数据库失败")
        else:
            return cur

    def ExecQuery(self, sql):
        cur = self.__GetConnect()
        cur.execute(sql)
        resList = cur.fetchall()

        # 查询完毕后必须关闭连接
        self.conn.close()
        return resList

    def ExecNonQuery(self, sql):
        cur = self.__GetConnect()
        cur.execute(sql)
        self.conn.commit()
        self.conn.close()


def main():
    ms = MSSQL(host="10.1.5.121", user="dmp", pwd="123456", db="JYPRIME")
    # resList = ms.ExecQuery("SELECT * FROM [dbo].[cmdCONVDEF1]")

    sql = "SELECT * FROM [dbo].[cmdCONVDEF1]"

    df = pd.read_sql(sql, ms)
    print(df)

    # print(resList)
    # for (_id, lv, name, prestige, award) in resList:
    #     print(name)


if __name__ == '__main__':
    main()
