from sqlalchemy import create_engine
import platform

servers = {
    # "127": {
    #     "outer": ("120.55.69.127", 3306),
    #     "inner": ("127.0.0.1", 3306)
    # },
    # "150": {
    #     "outer": ("121.40.18.150", 3306),
    # },
    # "1G": {
    #     "outer": ("583296d77f47e.sh.cdb.myqcloud.com", 7701)
    # },
    # "2G": {
    #     "outer": ("583e37fb680e7.sh.cdb.myqcloud.com", 3774),
    #     "outer": ("182.254.128.241", 3774),
    #     "inner": ("10.66.119.90", 3306)
    # },
    # "4G": {
    #     "outer": ("5837fc8ea3a6b.sh.cdb.myqcloud.com", 5948),
    #     "inner": ("10.66.186.149", 3306)
    # },
    "2G": {
        "outer": ("182.254.128.241", 4171),
        "inner": ("10.66.175.186", 3306)
    },
    "4G": {
        "outer": ("182.254.128.241", 4119),
        "inner": ("10.66.180.207", 3306)
    },
    "8G_1": {
        "outer": ("115.159.123.84", [22, 3306, 8080, 8090, 11111]),
        "inner": ("10.105.224.254", "")
    },# 123.206.109.188
    "8G_2": {
        "outer": ("123.206.211.100", [22, 3306, 8080, 8090, 11111]),
        "inner": ("10.105.215.110", "")
    },# 123.206.107.13
    "8G_3": {
        "outer": ("115.159.209.223", 22),
        "inner": ("10.105.200.249", "")
    },
    "8G_4": {
        "outer": ("182.254.131.212", 22),
        "inner": ("10.105.237.100", "")
    },
    "2G_new": {
        "outer": ("58cb57c164977.sh.cdb.myqcloud.com", 4171),
        "inner": ("10.66.175.186", 3306)
    },
    "4G_new": {
        "outer": ("182.254.128.241", 4119),
        "inner": ("10.66.180.207", 3306)
    }
}

accounts = {
    "1G": {
        "root": ("root", "smyt1234"),
        "script": ("smyt", "qU6mHuzH")
    },
    # "2G": {
    #     "root": ("root", "smyt1234"),
    #     "script": ("sm0202", "V4qVoXB8")
    # },
    # "4G": {
    #     "root": ("root", "smyt1234"),
    #     "script": ("sm0402", "T1wwmMuE")
    # },
    "2G": {
        "root": ("sm_data_calc", "yxJ85Y353Ztq"),
    },
    "4G": {
        "root": ("sm_data_calc", "JG2cNhJ4D293"),
    },
    "2G_new": {
        "root": ("sm_data_calc", "yxJ85Y353Ztq")
    },
    "4G_new": {
        "root": ("sm_data_calc", "JG2cNhJ4D293")
    }
}


def get_url(server_alias, conntype="outer", authority="root"):
    url = "mysql+pymysql://{acc}:{pwd}@{ip}:{port}".format(
        acc=accounts[server_alias][authority][0],
        pwd=accounts[server_alias][authority][1],
        ip=servers[server_alias][conntype][0],
        port=servers[server_alias][conntype][1]
    )
    return url


if platform.system() == "Linux":
    conntype = "inner"
    db_base = "10.66.175.186:3306"
    db_product = "10.66.180.207:3306"
    db_web = "s10.66.111.130:3306"
else:
    db_base = "58cb57c164977.sh.cdb.myqcloud.com:4171"
    db_product = "58cb57c164977.sh.cdb.myqcloud.com:4119"
    db_web = "58cb57c164977.sh.cdb.myqcloud.com:8612"

def load_engine():
    # aliases = ["2G", "2G", "4G", "1G", "2G", "4G", "4G", "2G", "2G", "1G", "2G", "4G_new", "2G_new", "4G_new", "2G"]
    # db_names = ["base", "product", "product", "base", "", "", "easy", "user_info", "test_gt", "test", "crawl", "product", "", "", "factor"]
    # engines = {alias + db_name[:1]:
    #     create_engine(get_url(alias) + "/" + db_name, pool_size=50, connect_args={"charset": "utf8"})
    #            for alias, db_name in zip(aliases, db_names)}

    aliases = ["2G", "2G", "4G", "2G", "4G", "4G", "2G", "2G","2G", "4G_new", "2G_new", "4G_new", "2G"]
    db_names = ["base", "product", "product", "", "", "easy", "user_info", "test", "crawl",
                "product", "", "", "factor"]

    if platform.system() == "Linux":
        conntype = "inner"
        db_2g = "sm_data_calc:yxJ85Y353Ztq@10.66.175.186:3306"
        db_product = "sm_data_calc:JG2cNhJ4D293@10.66.180.207:3306"
        db_web = "sm_data_calc:M7aYPg3887Sr@10.66.111.130:3306"
    else:
        conntype = "outer"
        db_2g = "sm_data_calc:yxJ85Y353Ztq@58cb57c164977.sh.cdb.myqcloud.com:4171"
        db_product = "sm_data_calc:JG2cNhJ4D293@58cb57c164977.sh.cdb.myqcloud.com:4119"
        db_web = "sm_data_calc:M7aYPg3887Sr@58cb57c164977.sh.cdb.myqcloud.com:8612"


    engines = {}
    for alias, db_name in zip(aliases, db_names):
        engines[alias + db_name[:1]] = create_engine(get_url(alias, conntype) + "/" + db_name, pool_size=50, connect_args={"charset": "utf8"})


    engines["2Gbf"] = create_engine("mysql+pymysql://{db}/base_finance".format(db=db_2g),
                                    pool_size=50, connect_args={"charset": "utf8"})
    engines["2Gbt"] = create_engine("mysql+pymysql://{db}/base_test".format(db=db_2g),
                                    connect_args={"charset": "utf8"})
    engines["2Gbf"] = create_engine("mysql+pymysql://{db}/base_finance".format(db=db_2g),
                                    connect_args={"charset": "utf8"})
    engines["2Gcp"] = create_engine("mysql+pymysql://{db}/crawl_public".format(db=db_2g),
                                    connect_args={"charset": "utf8"})
    engines["2Gcpri"] = create_engine("mysql+pymysql://{db}/crawl_private".format(db=db_2g),
                                    connect_args={"charset": "utf8"})
    engines["2Gbp"] = create_engine("mysql+pymysql://{db}/base_public".format(db=db_2g),
                                    connect_args={"charset": "utf8"})
    engines["2Gcfg"] = create_engine("mysql+pymysql://{db}/config".format(db=db_2g),
                                     connect_args={"charset": "utf8"})
    engines["2Ght"] = create_engine("mysql+pymysql://{db}/ht_competition".format(db=db_2g),
                                    connect_args={"charset": "utf8"})
    engines["2Glog"] = create_engine("mysql+pymysql://{db}/logs".format(db=db_2g),
                                    connect_args={"charset": "utf8"})
    engines["2Gf"] = create_engine("mysql+pymysql://{db}/factor".format(db=db_2g),
                                    connect_args={"charset": "utf8"})

    engines["4Gpp"] = create_engine("mysql+pymysql://{db}/product_mutual".format(db=db_product),
                                    connect_args={"charset": "utf8"})
    engines["8612ts"] = create_engine("mysql+pymysql://{db}/test_subsidiary".format(db=db_web),
                                    connect_args={"charset": "utf8"})
    engines["8612test_sync"] = create_engine("mysql+pymysql://{db}/test_gt".format(db=db_web),
                                             pool_size=50, connect_args={"charset": "utf8"})
    engines["8612app_mutual"] = create_engine("mysql+pymysql://{db}/app_mutual".format(db=db_web),
                                              pool_size=50, connect_args={"charset": "utf8"})
    engines["8612app_finance"] = create_engine("mysql+pymysql://{db}/app_finance".format(db=db_web),
                                              pool_size=50, connect_args={"charset": "utf8"})
    engines["8612factor"] = create_engine("mysql+pymysql://{db}/factor".format(db=db_web),
                                              pool_size=50, connect_args={"charset": "utf8"})


    # engines["4171_crawl_finance"] = create_engine("mysql+pymysql://root:smyt0317@182.254.128.241:4171/crawl_finance", pool_size=50,
    #                               connect_args={"charset": "utf8"})
    # engines["4171_base_finance"] = create_engine("mysql+pymysql://root:smyt0317@182.254.128.241:4171/base_finance", pool_size=50,
    #                               connect_args={"charset": "utf8"})
    engines["etl_finance"] = create_engine("mysql+pymysql://jr_etl_01:smytetl01@182.254.128.241:4171", pool_size=50, connect_args={"charset": "utf8"})
    engines["etl_crawl_finance"] = create_engine("mysql+pymysql://jr_etl_01:smytetl01@182.254.128.241:4171/crawl_finance", pool_size=50, connect_args={"charset": "utf8"})
    engines["etl_base_finance"] = create_engine("mysql+pymysql://jr_etl_01:smytetl01@182.254.128.241:4171/base_finance", pool_size=50, connect_args={"charset": "utf8"})
    engines["etl01"] = create_engine("mysql+pymysql://jr_etl_01:smytetl01@182.254.128.241:4171", pool_size=50, connect_args={"charset": "utf8"})
    engines["etl_base_private"] = create_engine("mysql+pymysql://jr_etl_01:smytetl01@182.254.128.241:4171/base", pool_size=50, connect_args={"charset": "utf8"})
    engines["etl_base_test"] = create_engine("mysql+pymysql://jr_etl_01:smytetl01@182.254.128.241:4171/base_test", pool_size=50, connect_args={"charset": "utf8"})
    engines["etl_crawl_private"] = create_engine("mysql+pymysql://jr_etl_01:smytetl01@182.254.128.241:4171/crawl_private", pool_size=50, connect_args={"charset": "utf8"})
    engines["etl_crawl_private_old"] = create_engine("mysql+pymysql://root:smyt0317@182.254.128.241:4171/crawl", pool_size=50, connect_args={"charset": "utf8"})
    return engines


if __name__ == "__main__":
    load_engine()