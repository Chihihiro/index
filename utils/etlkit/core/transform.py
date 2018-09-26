import re
from utils.etlkit.core.base import Frame
from utils.decofactory import common
import numpy as np
import pandas as pd


class BaseTransform:
    custom_name = None

    def __init__(self):
        pass

    def process(self):
        pass

    # helper function
    @property
    def name(self):
        return self.custom_name or self.__class__.__name__


class AddConst(BaseTransform):
    def __init__(self, const):
        self._const = const

    def process(self, frame):
        for col, val in self._const.items():
            frame[col] = val
        return frame


class KeyMap(BaseTransform):
    def __init__(self, mapper, drop_unmapped=False):
        """
        Key Map

        Args:
            mapper: dict<key: expr>
                $key is the original column name, and $expr is map method used. $expr can be a constant;
                e.g. {
                    "foundation_date": "found_date",
                    "registration_code": "reg_code"
                }
        """

        self._mapper = mapper
        self._drop_unmapped = drop_unmapped

    def process(self, frame):
        frame.rename(columns=self._mapper, inplace=True)
        if self._drop_unmapped:
            cols = sorted(set(self._usedkeys).intersection(set(frame.columns)), key=frame.columns.tolist().index)
            return frame[cols]
        return frame


class ValueMap(BaseTransform):
    def __init__(self, mapper, tolerant_exec=True):
        """
        Value Map

        Args:
            mapper: dict<key: expr>
                map method used. $key is the column to execute map $expr. $expr can be a dict<val, mapped val>, a const,
                or a callable, which input is the value to map, and output is the mapped value.
                e.g. {
                    "gender": {"gentle man": "male", "lady": "female", ...,}
                    "address": "shanghai",
                    "age": lambda x: int(x),
                    "name": (lambda x, y: x + y, "family name", "first name")
                }
            tolerant_exec: bool, default True
                automatically deal with exceptions(return None) when applying callable to a column and occurs exception.
        """

        self._mapper = mapper.copy()
        self._tolerant_exec = tolerant_exec

    def check_tolerant(self):
        if self._tolerant_exec:
            for key, expr in self._mapper.items():
                if callable(expr):
                    self._mapper[key] = common.tolerant_exec()(expr)
                elif type(expr) is tuple:
                    self._mapper[key] = (common.tolerant_exec()(expr[0]), *expr[1:])

    def process(self, frame):
        self.check_tolerant()
        for key, expr in self._mapper.items():
            if callable(expr):
                frame[key] = frame[key].apply(expr)
            elif type(expr) is dict:
                frame[key] = frame[key].apply(lambda val: expr.get(val, val))  # return val it self when it's not found
            elif type(expr) is tuple:
                lmbd = expr[0]
                cols = expr[1:]
                frame[key] = list(map(lmbd, *(frame[col] for col in cols)))
            # elif type(expr) is str:
            else:
                frame[key] = expr
        return frame


class SelectKeys(BaseTransform):
    def __init__(self, keys):
        self._usedkeys = keys

    def process(self, frame):
        cols = sorted(set(self._usedkeys).intersection(set(frame.columns)), key=frame.columns.tolist().index)
        return frame[cols]


class MapSelectKeys(BaseTransform):
    def __init__(self, mapper):
        self._mapper = mapper

    def process(self, frame):
        keys_notused = set(frame) - set(self._mapper.keys())
        frame.drop(list(keys_notused), axis=1, inplace=True)
        frame.rename(columns=lambda x: (self._mapper.get(x) or x), inplace=True)
        return frame


class Dropna(BaseTransform):
    def __init__(self, axis=0, how="any", thresh=None, subset=None, inplace=False):
        self._axis = axis
        self._how = how
        self._thresh = thresh
        self._subset = subset
        self._inplace = inplace

    def process(self, frame):
        if self._inplace:
            frame.dropna(self._axis, self._how, self._thresh, self._subset, self._inplace)
        else:
            frame = frame.dropna(self._axis, self._how, self._thresh, self._subset, self._inplace)
        return frame


class DropKeys(BaseTransform):
    def __init__(self, keys):
        self._keys = keys

    def process(self, frame):
        return frame.drop(self._keys, axis=1, errors="ignore")


class FillnaByColumn(BaseTransform):
    def __init__(self, mapper):
        """

        Args:
            mapper: dict{key_to_be_filled: key_used_to_fill}

        """
        self._mapper = mapper

    def process(self, frame):
        for key_filled, key_fill in self._mapper.items():
            frame[key_filled] = frame[key_filled].fillna(frame[key_fill])
        return frame


class CleanWrongToNone(BaseTransform):
    def __init__(self, patts, repls=None):
        """

        Args:
            patt:
            repl: {key: wrong_patt}

        """

        self._patts = patts
        self._repls = repls or {}

    def process(self, frame):
        flag = ""
        for key, patt in self._patts.items():
            cpatt = re.compile(patt)
            frame.loc[frame[key].apply(
                lambda x: cpatt.sub(flag, x) if type(x) is str else flag) == flag, key] = self._repls.get(key)
        return frame


class CleanRight(BaseTransform):
    def __init__(self, patts):
        self._patts = patts

    def process(self, frame):
        for key, patt in self._patts.items():
            cpatt = re.compile(patt)
            frame[key] = frame[key].apply(lambda x: cpatt.search(x) if type(x) is str else None)
            frame[key] = frame[key].apply(lambda x: x.group() if x is not None else None)
        return frame


class DropDuplicate(BaseTransform):
    def __init__(self, subset=None, keep="first"):
        self._subset = subset
        self._keep = keep

    def process(self, frame):
        frame.drop_duplicates(subset=self._subset, keep=self._keep, inplace=True)
        return frame


class SortDropduplicate(BaseTransform):
    def __init__(self, sort_by, ascending=True, subset=None, keep="first"):
        self._sort_by = sort_by
        self._ascending = ascending
        self._subset = subset
        self._keep = keep

    def process(self, frame):
        if self._sort_by is not None:
            frame.sort_values(self._sort_by, ascending=self._ascending, inplace=True)
        frame.drop_duplicates(subset=self._subset, keep=self._keep, inplace=True)
        return frame


class SortFillnaDropduplicate(BaseTransform):
    def __init__(self, sort_by, ascending=None, key=None, subset=None, keep="first"):
        """

        Args:
            sort_by: str, or list<str>
            ascending: bool, or list<bool>
            key: None, or dict
            subset: strEQ or list
            keep: "first", "last", or False(drop all duplicates)
        """

        self._sort_by = sort_by
        self._ascending = ascending
        self._key = key
        self._subset = subset
        self._keep = keep

    def process(self, frame):
        idx_for_sort = []
        frame.index = range(len(frame))
        for sb, asc in zip(self._sort_by, self._ascending):
            if sb in self._key:
                subidx = frame[sb].apply(lambda x: self._key[sb].get(x, np.inf)).tolist()
            else:
                subidx = frame[sb].tolist()

            idx_for_sort.append(subidx)

        sorted_idx = pd.DataFrame(list(zip(*idx_for_sort)), columns=self._sort_by).sort_values(by=self._sort_by, ascending=self._ascending).index

        frame = frame.reindex(sorted_idx)
        frame.bfill(inplace=True)
        frame.drop_duplicates(subset=self._subset, keep=self._keep, inplace=True)
        return frame


class Join(BaseTransform):
    def __init__(self, right, how="inner", on=None, left_on=None, right_on=None, suffixes=("_x", "_y")):
        """

        Args:
            right: pandas.DataFrame, MysqlInput, or Frame
            how:
            on:
            left_on:
            right_on:
            suffixes:
        """
        self._right = Frame(right) if type(right) is not Frame else right
        self._how = how
        self._on = on
        self._left_on = left_on
        self._right_on = right_on
        self._suffixes = suffixes

    def process(self, frame):
        return frame.merge(self._right.dataframe, how=self._how, on=self._on, left_on=self._left_on,
                           right_on=self._right_on,
                           suffixes=self._suffixes)


def test():
    a = pd.DataFrame([[1, 2, 2, 2, 3], ["a", "c", "b", "e", "d"]]).T
    # tr = SortFillnaDropduplicate([0, 1], key={1: {"b": 0, "c": 1, "a": 2, "e": 0.5}})
    # tr = SortFillnaDropduplicate([1, 0], ascending=[False, True], key={1: {"b": 1, "c": 0, "a": 2}})

    tr = SortFillnaDropduplicate([1], ascending=[False], key={1: {"b": 0, "c": 1, "a": 2, "e": 0.5}})


    tr.process(a)


if __name__ == "__main__":
    test()
