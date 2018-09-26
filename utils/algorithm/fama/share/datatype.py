import datetime as dt
from utils.algorithm import timeutils as tu
from utils.algorithm.base import exceptions
from utils.algorithm.fama.share import sqlfactory
from utils.decofactory import common
from utils.timeutils import const


# base data type
class FreqData:
    PERIODS = {
        "d": const.bday_chn,
        "w": dt.timedelta(7)
    }

    def __init__(self, end, freq, start=None, **kwargs):
        """
            Framework for fetching, processing data with different frequency;

        Args:
            end: datetime.date
            freq: str, optional {"d", "w"}
                data frequency
            start: datetime.date
            **kwargs:
                shift: timedelta
        """

        self._one_period = self.PERIODS[freq]
        self.end = end
        self.start = self.end - self._one_period if start is None else start
        self.start -= kwargs.get("shift", dt.timedelta(0))
        self.freq = freq
        self._weekdays = {0: "Mon", 1: "Tue", 2: "Wed", 3: "Thu", 4: "Fri", 5: "Sat", 6: "Sun"}

    def __repr__(self):
        freqs = {"d": "daily", "w": "weekly"}

        return "{name} for {sd}(wday={weekday}) calculation (freq={freq})".format(
            name=self.__class__.__name__, sd=self.end,
            weekday=self._weekdays[self.end.weekday()], freq=freqs[self.freq].upper()
        )

    @property
    def exstart(self):
        exstart = self.start - self._one_period
        try:
            exstart = exstart.date()
        except:
            pass
        return exstart

    def resample(self, dataframe):
        """

        Args:
            dataframe:
            levels: list
                length of levels should be 2
                List of column(s) for groupby and resample operation. *** The first element SHOULD BE the column that
                contains datetime-like value (for resampling)

        Returns:

        """
        # if len(levels) == 2:
        #     df_grouped = dataframe.pivot(index=levels[0], columns=levels[1])
        #     df_resampled = tu.resample(df_grouped, self.freq, weekday=self.end.weekday())
        #     return df_resampled.stack(level=levels[1])
        # elif len(levels) == 1:
        #     return tu.resample(dataframe.set_index(levels[0]), self.freq, weekday=self.end.weekday())
        # else:
        #     raise DataError
        return tu.resample(dataframe, self.freq, weekday=self.end.weekday())

    def _check_date(self, data, date_column):
        if len(data) == 0:
            raise Exception("No Data between ({lb}, {ub}]".format(lb=self.exstart, ub=self.end))
        else:
            # check daily-frequency data on given `statistic_date`
            if (self.freq == "d") & (data[date_column].max() != self.end):
                raise ValueError("Missing Data on date {sd} for daily-frequency calculation".format(sd=self.end))

            # check whether there are two distinct date (last_period & statistic_date)
            if len(data[date_column].drop_duplicates().tolist()) < 2:
                raise ValueError("Not Enough Data for calculation: Less than 2 periods")


# Data Class for constructing Factors
class StockPrice(FreqData):
    def __init__(self, end, freq, start=None, **kwargs):
        FreqData.__init__(self, end, freq, start, **kwargs)

    def preprocessed(self, dataframe):
        # Drop NaN of critical column
        if len(dataframe) == 0:
            raise ValueError("{sd}下没有交易数据".format(sd=self.end))
        dataframe = dataframe.dropna(subset=["date", "close"], how="any")

        return dataframe

    @property
    @common.unhash_inscache()
    def data(self):
        df_data = sqlfactory.FamaDataLoader.load_stockdata(self.exstart, self.end)
        return df_data

    @property
    @common.unhash_inscache()
    def price(self):
        return self.resample(self.data["close"])

    @property
    @common.unhash_inscache()
    def circulated_price(self):
        return self.resample(self.data["circulated_price"])

    @property
    def return_series(self):
        if len(self.price) < 2:
            raise exceptions.NotEnoughSampleError("Not enough data for calculating return")
        return (self.price / self.price.shift(1))[1:]


class MarketIndex(FreqData):
    def __init__(self, end, freq, start=None, **kwargs):
        FreqData.__init__(self, end, freq, start, **kwargs)

    @property
    @common.unhash_inscache()
    def data(self):
        df = sqlfactory.FamaDataLoader.load_bmdata(self.exstart, self.end)
        df = self.resample(df)

        return df

    @property
    def return_series(self):
        tmp = self.data["value"]

        return (tmp / tmp.shift(1) - 1)[1:]

    @property
    def excess_return_series(self):
        tb = TreasuryRate(self.end, self.freq, start=self.start)

        return self.return_series - tb.return_series


class TreasuryRate(MarketIndex):
    def __init__(self, end, freq, start=None, **kwargs):
        MarketIndex.__init__(self, end, freq, start, **kwargs)

    def preprocessed(self, df_market_index):
        period_num = {"d": 250, "w": 52}[self.freq]
        df_market_index["value"] = df_market_index["value"].apply(lambda x: x / 100 / period_num)
        return df_market_index

    @property
    @common.unhash_inscache()
    def data(self):
        df = sqlfactory.FamaDataLoader.load_rfdata(self.exstart, self.end)
        df = self.preprocessed(df)
        df = self.resample(df).fillna(method="ffill")

        return df

    @property
    def return_series(self):
        return self.data["value"][1:]


# Data class for constructing attribution
class Fund(FreqData):
    def __init__(self, end, freq, fund_id, **kwargs):
        FreqData.__init__(self, end, freq, **kwargs)
        self.fund_id = fund_id

    @property
    def foundation_date(self):
        return sqlfactory.FamaDataLoader.load_fdate(self.fund_id)

    @property
    def data(self):
        df = sqlfactory.FamaDataLoader.load_nvdata(self.fund_id, self.exstart, self.end)
        df = self.resample(df)

        return df

    @property
    @common.unhash_inscache()
    def return_series(self):
        tmp = self.data["value"]

        return (tmp / tmp.shift(1) - 1)[1:]

    @property
    def excess_return_series(self):
        tb = TreasuryRate(self.end, self.freq, start=self.start)

        return self.return_series - tb.return_series


class Factors(FreqData):
    def __init__(self, end, freq, ids, start=None, **kwargs):
        FreqData.__init__(self, end, freq, start, **kwargs)
        self.ids = ids

    @property
    def data(self):
        df = sqlfactory.FamaDataLoader.load_factordata(self.ids, self.exstart, self.end, freq=self.freq)
        df = self.resample(df)
        try:
            return df[self.ids]
        except KeyError:
            raise exceptions.DataError("Not enough factor data")


def main():
    args = (
        (dt.date(2018, 2, 28), "d", "JR000001"),
        (dt.date(2018, 3, 28), "w", "JR000001"),
        (dt.date(2018, 4, 28), "d", "JR000001"),
        (dt.date(2018, 5, 28), "w", "JR000001"),
    )

    def test(ed, freq, fid):
        sp = StockPrice(ed, freq)
        mi = MarketIndex(ed, freq)
        rf = TreasuryRate(ed, freq)
        f = Fund(ed, freq, fid)

        res = (sp.data, sp.price, sp.circulated_price, sp.return_series,
               mi.data, mi.excess_return_series,
               rf.data, rf.return_series,
               f.data, f.return_series, f.excess_return_series)
        for r in res:
            print(r)

    for arg in args:
        test(*arg)


if __name__ == "__main__":
    main()
