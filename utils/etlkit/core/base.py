from collections import OrderedDict
import pandas as pd
from utils.etlkit.reader import mysqlreader
from multiprocessing.dummy import Pool as ThreadPool


class Name:
    def __init__(self, name):
        self._name = name

    @property
    def name(self):
        if self._name is None:
            self._name = self.__hash__()
        return self._name

    @name.setter
    def name(self, val):
        self._name = val


class Frame(Name):
    def __init__(self, input_, name=None):
        super().__init__(name)
        self._input = input_
        self._input_type = type(input_)

    @property
    def dataframe(self):
        if not hasattr(self, "_dataframe"):
            if self._input_type is pd.DataFrame:
                val = self._input.copy()
            elif isinstance(self._input, (mysqlreader.MysqlInput, Frame, Stream, Confluence)):
                if hasattr(self._input, "flow"):
                    self._input.flow()
                val = self._input.dataframe.copy()
            self.__setattr__("_dataframe", val)
        return self._dataframe

    @dataframe.setter
    def dataframe(self, val):
        self._dataframe = val

    def fields(self):
        return self.dataframe.columns


class Stream(Name):
    def __init__(self, inpdata, transform: list, name=None):
        super().__init__(name)
        self._inpdata = inpdata
        self._frame = Frame(inpdata)
        self._transform = transform
        self._current_stack_depth = 0
        self._max_stack_depth = len(transform)

    def _yield_from_stack(self):
        for tr in self._transform:
            yield self.dataframe
            self.dataframe = tr.process(self.dataframe)
            self._current_stack_depth += 1

    def stack(self):
        if not hasattr(self, "_stack"):
            self.__setattr__("_stack", self._yield_from_stack())
        try:
            return next(self._stack)
        except StopIteration:
            pass

    def flow(self, stack_depth=None):
        if stack_depth is None:
            stack_depth = self._max_stack_depth
        assert ((stack_depth <= self._max_stack_depth) and (stack_depth >= self._current_stack_depth))
        while self._current_stack_depth < stack_depth:
            self.stack()
        return self.data

    @property
    def frame(self):
        if not hasattr(self, "_frame"):
            val = Frame(self._inpdata)
            self.__setattr__("_frame", val)
        return self._frame

    @property
    def dataframe(self):
        return self.frame.dataframe

    @dataframe.setter
    def dataframe(self, val):
        self.frame.dataframe = val

    @property
    def data(self):
        return self.frame.dataframe, self._current_stack_depth

    @property
    def stack_depth(self):
        return self._current_stack_depth

    @property
    def transform(self):
        return self._transform

    @transform.setter
    def transform(self, val):
        self._transform = val
        self._max_stack_depth = len(val)


class Confluence(Name):
    def __init__(self, *inputs, on=None, prio_l1=None, prio_l2=None, name=None, pool=None):
        """

        Args:
            *inputs: Frame, Stream, or Confluence

            on: list<str>

            prio_l1: dict
                e.g.
                {
                0: {
                "fund_name": ("source_id", "000001"),
                "fund_full_name": ("source_id", "000001"),
                "reg_time": ("source_id", "010003"),
                "reg_code": ("source_id", "010002"),
                },
                1: {
                "reg_code": ("source_id", "010003"),
                },
                }

            prio_l2: dict
                {
                0: "stream_name_1",
                1: "stream_name_2",
                }

        """

        super().__init__(name)
        self._inputs = inputs
        self._on = on
        self._prio_l1 = prio_l1
        self._prio_l2 = prio_l2
        self._namedict = OrderedDict([(inp.name, inp) for inp in inputs])
        self.pool = pool or len(inputs)

    @property
    def prio_l1(self):
        return self._prio_l1

    @property
    def prio_l2(self):
        if self._prio_l2 is None:
            self._prio_l2 = dict(enumerate(self._namedict))
        return self._prio_l2

    def flowall(self):
        if self.pool:
            def fl(x):
                if type(x) is Stream:
                    x.flow()
            pool = ThreadPool(self.pool)
            pool.map(fl, self._inputs)
            pool.close()
            pool.join()
        else:
            for inp in self._inputs:
                if type(inp) is Stream:
                    inp.flow()

    def merge_frame(self):
        self.flowall()
        if self._on is not None:
            inputs = [self._inputs[k].dataframe for k in self.prio_l2]
            output = self._merge_dataframe(*inputs, on=self._on, prio_cond=self.prio_l1)
        else:
            output = pd.DataFrame()
            for inp in self._inputs:
                output = output.append(inp.dataframe)
        return output

    @property
    def frame(self):
        if not hasattr(self, "_frame"):
            self.__setattr__("_frame", Frame(self.merge_frame()))
        return self._frame

    @property
    def dataframe(self):
        return self.frame.dataframe

    # helper function
    @classmethod
    def _init_baseframe(cls, *dataframes):
        from functools import reduce
        cols = list(reduce(lambda lcol, rcol: lcol.union(rcol), [set(frame) for frame in dataframes]))
        indexes = list(reduce(lambda lidx, ridx: lidx.union(ridx), [set(frame.index) for frame in dataframes]))
        frame_merged = pd.DataFrame(columns=cols, index=indexes)
        return frame_merged

    @classmethod
    def _merge_dataframe(cls, *dataframes, on, prio_cond=None):
        for dataframe in dataframes:
            dataframe.index = dataframe[on]

        base_frame = cls._init_baseframe(*dataframes)
        if prio_cond is not None:
            for prio in prio_cond.keys():
                keys = prio_cond[prio]
                for key, cond in keys.items():
                    for dataframe in dataframes:
                        if (key in dataframe) and (cond[0] in dataframe):
                            c = (dataframe[cond[0]] == cond[1])
                            v = dataframe[c][[key]]

                            base_frame = base_frame.fillna(v)

        for frame in dataframes:
            base_frame = base_frame.fillna(frame.drop_duplicates(subset=on))

        return base_frame
