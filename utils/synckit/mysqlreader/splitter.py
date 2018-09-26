import re
from functools import partial
from multiprocessing.dummy import Pool as ThreadPool
import pandas as pd
from sqlalchemy.inspection import inspect
from utils.database import io
from utils.decofactory import mydecorator
from utils.synckit.core import common


STRICT = True


class MysqlReader:
    def __init__(self, name, bind, **kwargs):
        """

        Args:
            name:
            bind:
            **kwargs:
                columns: list
                mapping: dict
        """
        self._name = name
        self._bind = bind
        self._columns = kwargs.get("columns")
        self._mapping = kwargs.get("mapping")

    def __repr__(self):
        return "Table `{tb}`\nBind Engine: {eg}".format(tb=self._name, eg=self._bind)

    @property
    def ddl(self):
        ddl_str = self._bind.execute("SHOW CREATE TABLE {tb}".format(tb=self._name)).fetchone()[1]
        idxs = [x.replace("`", "") for x in re.findall("KEY `.*` \((`.*`)\)", ddl_str)]
        pk = re.search("PRIMARY KEY \((?P<pk>.*)\)", ddl_str).groupdict()["pk"].replace("`", "").split(",")
        return {
            "pk": pk, "idx": idxs
        }

    @property
    def columns(self):
        if self._columns is None:
            inspector = inspect(self._bind)
            cols = [x["name"] for x in inspector.get_columns(self._name)]
        else:
            cols = self._columns
        return cols

    @property
    def formatted_columns(self):
        cols = self.columns
        cols = str(list(cols))[1:-1].replace("'", "`")
        return cols

    @property
    def mapping(self):
        return self._mapping

    @property
    def mapped_colums(self):
        return [self.mapping.get(x, x) for x in self.columns]

    @property
    def candidate_key(self):
        return self.ddl["pk"]
    # def candidate_key(self):
    #     ddl = self.ddl
    #     pk = set(ddl["pk"])
    #     idx = set(ddl["idx"])
    #     priority = {y: x for x, y in enumerate(self.ddl["pk"])}
    #     candidate_splitter = [
    #         *sorted(pk.intersection(idx), key=lambda x: priority[x]),
    #         *sorted(pk - idx, key=lambda x: priority[x])
    #     ]
    #     return candidate_splitter

    def _bind_splitter(self):
        return Splitter(self)


class Splitter:
    def __init__(self, table: MysqlReader, method="pk", level=None, where="", pool_size=10):
        self._table = table
        self._method = method
        self._level = level
        self._where = where
        self._pool = pool_size

    def _fetch_pk(self, limit):
        if self._level is None:
            pk = ", ".join(self._table.candidate_key[:1])
        else:
            pk = ", ".join(self._level)

        sql = "SELECT DISTINCT {pk} FROM {tb} {where} LIMIT {start}, {offset}".format(
            pk=pk, tb=self._table._name, start=limit[0], offset=limit[1], where=self._where
        )
        result = pd.read_sql(sql, self._table._bind)
        return result

    # @mydecorator.log(level="performance")
    def _fetch_all(self, limit):
        sql = "SELECT {cols} FROM {tb} {where} LIMIT {start}, {offset}".format(
            cols=self._table.formatted_columns, tb=self._table._name, where=self._where, start=limit[0], offset=limit[1]
        )
        result = pd.read_sql(sql, self._table._bind)
        return result

    def fetch_all(self):
        tpool = ThreadPool(self._pool)
        pk = ", ".join(self._table.ddl["pk"])

        print("fetching total records for updating...")
        total_records = self._table._bind.execute("SELECT COUNT(DISTINCT {pk}) FROM {tb} {where}".format(
            pk=pk, tb=self._table._name, where=self._where
        )).fetchone()[0]
        step = max(total_records // (self._pool - 1), 1)
        limits = [(i, step) for i in range(0, total_records, step)]
        print("records num: %s" % total_records)
        result = tpool.map(self._fetch_all, limits)
        tpool.close()
        return result

    @property
    def candidate_key(self):
        if self._level is None:
            return self._table.candidate_key[:1]
        else:
            return self._level

    @mydecorator.log(level="performance")
    def fetch_chunk(self):
        tpool = ThreadPool(self._pool)
        pk = ", ".join(self.candidate_key)

        print("fetching total num....Using KEY %s for splitting" % pk)
        total_pk = self._table._bind.execute("SELECT COUNT(DISTINCT {pk}) FROM {tb} {where}".format(
                pk=pk, tb=self._table._name, where=self._where
        )).fetchone()[0]
        step = max(total_pk // (self._pool - 1), 1)

        print("fetching pks: total:{tt}, chunk_size:{cs}".format(tt=total_pk, cs=step))
        limits = [(i, step) for i in range(0, total_pk, step)]
        func = partial(self._fetch_pk)
        result = tpool.map(func, limits)
        tpool.close()
        if len(result) == 0:
            raise ValueError("Get zero-chunk when splitting by pk, check whether this table is am empty table")
        return result


class TaskGenerator:
    def __init__(self, src_table, tgt_table=None, pool_size={"update": 2, "delete": 15}, level=None, where="", **kwargs):
        self._src_table = src_table
        self._tgt_table = tgt_table
        self._pool_size = pool_size
        self._apply = kwargs.get("apply", None)
        self._colmap = kwargs.get("colmap", {})
        self._where_del = kwargs.get("where_del", "")
        self._src_deleter = Splitter(self._src_table, level=level, pool_size=self._pool_size.get("delete", 15))
        self._tgt_deleter = Splitter(self._tgt_table, level=[self._colmap.get(x, x) for x in self._src_deleter.candidate_key], pool_size=self._pool_size.get("delete", 15), where=self._where_del)
        self._src_updater = Splitter(self._src_table, where=where, pool_size=self._pool_size.get("update", 2))

    def _get_del_chunk(self):
        pool = ThreadPool(2)
        res = pool.map(lambda x: x.fetch_chunk(), [self._src_deleter, self._tgt_deleter])
        pool.close()
        return res

    # @mydecorator.log(level="performance")
    def _generate_task_del(self, querys):
        query_src, query_tgt = querys
        pool = ThreadPool(2)
        data_src, data_tgt = pool.starmap(pd.read_sql, [(query_src, self._src_table._bind), (query_tgt, self._tgt_table._bind)])
        pool.close()
        if self._apply is not None:
            for col, lmbd in self._apply.items():
                if col in data_src.columns:
                    data_src[col] = data_src[col].apply(lmbd)
        data_src = data_src.rename(columns=self._colmap)
        data_del = common.get_difference(data_src, data_tgt)
        return data_del

    def columns(self):
        return list(set([self._colmap.get(x, x) for x in self._src_table.columns]).intersection(self._tgt_table.columns))

    def generate_task_del(self):
        """"""
        chunk_src, chunk_tgt = self._get_del_chunk()
        src_all, tgt_all = common.merge_dataframe(chunk_src), common.merge_dataframe(chunk_tgt)

        # Primary key may also be in the `apply` dict,
        # So need to apply corresponding lambda before comparing pk
        if self._apply is not None:
            for col, lmbd in self._apply.items():
                if col in src_all.columns:
                    src_all[col] = src_all[col].apply(lmbd)
        src_all = src_all.rename(columns=self._colmap)

        rec_1 = common.get_difference(src_all, tgt_all, main="source")
        rec_2 = None

        if len(self._src_table.ddl["pk"]) > 1:
            print("len chunks: ", len(chunk_src))
            pk = self._src_table.ddl["pk"]
            query1 = "SELECT {pk} FROM {tb} ".format(pk=", ".join(pk), tb=self._src_table._name)
            query2 = "SELECT {pk} FROM {tb} ".format(pk=", ".join([self._colmap.get(x, x) for x in pk]), tb=self._tgt_table._name)
            query = [
                (
                    query1 + "WHERE {condition}".format(condition=io.generate_condition(chunk)),
                    query2 + "WHERE {condition}".format(condition=io.generate_condition(chunk.rename(columns=self._colmap)))
                )
                for chunk in chunk_src
            ]
            # return query
            pool = ThreadPool(self._pool_size.get("delete"))
            rec_2 = pool.map(self._generate_task_del, query)
            pool.close()
            rec_2 = common.merge_dataframe(rec_2)
        return rec_1, rec_2

    def generate_task_upt(self):
        return [x.rename(columns=self._colmap) if x is not None else x for x in self._src_updater.fetch_all()]


class Job:
    def __init__(self, src_table, tgt_table=None, pool_size=10, level=None, where="", **kwargs):
        """

        Args:
            src_table:
            tgt_table:
            pool_size:
            level:
            where:
            **kwargs:
                apply: dict<str: lambda>
                mapping:
                columns: list
        """
        self._src_table = src_table
        self._tgt_table = tgt_table
        self._pool_size = pool_size
        self._level = level
        self._where = where
        self._apply = kwargs.get("apply")
        self._colmap = kwargs.get("colmap", {})
        self._tasks = TaskGenerator(
            self._src_table, self._tgt_table, pool_size=self._pool_size, level=self._level, where=self._where, apply=self._apply, colmap=self._colmap,
            where_del=kwargs.get("where_del")
        )

    @mydecorator.log(level="performance")
    def update(self, chunksize=2000):
        try:
            cols_used = self._tasks.columns()
            task_upt = self._tasks.generate_task_upt()
            for task in task_upt:
                if task is None or len(task) == 0:
                    continue
                if type(self._apply) is dict:
                    for col, lmbd in self._apply.items():
                        task[col] = task[col].apply(lmbd)
                tmp = task[cols_used].rename(columns=self._colmap)
                io.to_sql(self._tgt_table._name, self._tgt_table._bind, tmp, chunksize=chunksize)

        except Exception as e:
            print(e)

    @mydecorator.log(level="performance")
    def delete(self):
        try:
            task_del = self._tasks.generate_task_del()
            for task in task_del:
                if task is None or len(task) == 0:
                    continue
                io.delete(self._tgt_table._name, self._tgt_table._bind, task)

        except Exception as e:
            print(e)

    def sync(self):
        try:
            if STRICT:
                self.delete()
        except ValueError as e:
            print(e)

        try:
            self.update()
        except Exception as e:
            print(e)
