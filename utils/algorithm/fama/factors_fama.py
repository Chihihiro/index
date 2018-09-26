import calendar as cld
import datetime as dt
from abc import ABCMeta, abstractmethod
from dateutil.relativedelta import relativedelta
from utils.algorithm.fama.share import sqlfactory, datatype


class IFactor(metaclass=ABCMeta):
    @abstractmethod
    def factor(self) -> dict:
        pass


# Construct Factors
class StyleFactor:
    def __init__(self, end: dt.date):
        self.end = end

    @property
    def _year(self):
        return self.end.year

    @property
    def _season(self):
        return (self.end.month - 1) // 3 + 1

    @property
    def last_season_end(self):
        d = dt.date(
            self._year, self._season * 3, cld.monthrange(self._year, self._season * 3)[1]) - relativedelta(months=6)

        return dt.date(d.year, d.month, cld.monthrange(d.year, d.month)[1])

    @property
    def current_season_end(self):
        d = dt.date(
            self._year, self._season * 3, cld.monthrange(self._year, self._season * 3)[1]) - relativedelta(months=3)

        return dt.date(d.year, d.month, cld.monthrange(d.year, d.month)[1])


class FamaStyleFactor(IFactor, StyleFactor):
    data_loader = sqlfactory.FamaDataLoader

    def __init__(self, end: dt.date, freq: str):
        self.end = end
        self.stock = datatype.StockPrice(end, freq)
        StyleFactor.__init__(self, end)

    @classmethod
    def _group_return(cls, r, w_data, *groups):
        res = []
        for group in groups:
            # Starting in 0.21.0, using .loc or []
            # with a list with one or more missing labels, is deprecated,
            # in favor of .reindex.
            grouped = w_data.reindex(group).dropna()
            w_group = grouped / grouped.sum()
            r_group = (r.reindex(group) * w_group / w_group.sum()).sum()
            res.append(r_group)

        return res

    @property
    def type_value(self) -> dict:
        return self.data_loader.load_type_value(self.last_season_end, self.current_season_end)

    @property
    def type_scale(self) -> dict:
        return self.data_loader.load_type_scale(self.last_season_end, self.current_season_end)

    @property
    def smb(self):
        r = self.stock.return_series.iloc[0]
        w_data = self.stock.circulated_price.iloc[0]

        # Combination of each group (set)
        sl, sm, sh, bl, bm, bh = [
            self.type_scale[tp].intersection(self.type_value[bp]) for tp in ("S", "B") for bp in ("L", "M", "H")
        ]

        r_sl, r_sm, r_sh, r_bl, r_bm, r_bh = self._group_return(r, w_data, *(sl, sm, sh, bl, bm, bh))
        smb = ((r_sl + r_sm + r_sh) - (r_bl + r_bm + r_bh)) / 3

        return smb

    @property
    def hml(self):
        r = self.stock.return_series.iloc[0]
        w_data = self.stock.circulated_price.iloc[0]

        # Combination of each group (set)
        sl, sh, bl, bh = [
            self.type_scale[tp].intersection(self.type_value[bp]) for tp in ("S", "B") for bp in ("L", "H")
        ]

        r_sl, r_sh, r_bl, r_bh = self._group_return(r, w_data, *(sl, sh, bl, bh))
        hml = ((r_sh + r_bh) - (r_sl + r_bl)) / 3

        return hml

    @property
    def factor(self):
        return {self.end: {"SMB": self.smb, "HML": self.hml}}


class CarhartStyleFactor(FamaStyleFactor):
    data_loader = sqlfactory.CarhartDataLoader

    @property
    def type_mom(self) -> dict:
        return self.data_loader.load_type_mom(self.last_season_end, self.current_season_end)

    @property
    def smb(self):
        r = self.stock.return_series.iloc[0]
        w_data = self.stock.circulated_price.iloc[0]

        # Combination of each group (set)
        sl, sm, sh, bl, bm, bh = [
            self.type_scale[tp].intersection(self.type_value[bp]) for tp in ("S", "B") for bp in ("L", "M", "H")
        ]
        sl_, sd, sw, bl_, bd, bw = [
            self.type_scale[tp].intersection(self.type_mom[bp]) for tp in ("S", "B") for bp in ("L", "D", "W")
        ]

        r_sl, r_sm, r_sh, r_bl, r_bm, r_bh, r_sl_, r_sd, r_sw, r_bl_, r_bd, r_bw = self._group_return(
            r, w_data, *(sl, sm, sh, bl, bm, bh, sl_, sd, sw, bl_, bd, bw))

        smb_sv = ((r_sh + r_sm + r_sl) - (r_bh + r_bm + r_bl)) / 3
        smb_sm = ((r_sl_ + r_sd + r_sw) - (r_bl_ + r_bd + r_bw)) / 3
        smb = (smb_sv + smb_sm) / 2

        return smb

    @property
    def mom(self):
        r = self.stock.return_series.iloc[0]
        w_data = self.stock.circulated_price.iloc[0]

        # Combination of each group (set)
        sl, sw, bl, bw = [
            self.type_scale[t1].intersection(self.type_mom[t2]) for t1 in ("S", "B") for t2 in ("L", "W")
        ]

        r_sl, r_sw, r_bl, r_bw = self._group_return(r, w_data, *(sl, sw, bl, bw))
        mom = ((r_bw + r_sw) - (r_bl + r_sl)) / 2

        return mom

    @property
    def factor(self):
        return {self.end: {"SMB": self.smb, "HML": self.hml, "MOM": self.mom}}


class FamaFrenchStyleFactor(FamaStyleFactor):
    data_loader = sqlfactory.FamaFrenchDataloader

    @property
    def type_rmw(self) -> dict:
        return self.data_loader.load_type_rmw(self.last_season_end, self.current_season_end)

    @property
    def type_cma(self) -> dict:
        return self.data_loader.load_type_cma(self.last_season_end, self.current_season_end)

    @property
    def smb(self):
        r = self.stock.return_series.iloc[0]
        w_data = self.stock.circulated_price.iloc[0]

        # Combination of each group (set)
        sl, sm, sh, bl, bm, bh = [
            self.type_scale[tp].intersection(self.type_value[bp]) for tp in ("S", "B") for bp in ("L", "M", "H")
        ]
        sw, sm_, sr, bw, bm_, br = [
            self.type_scale[tp].intersection(self.type_rmw[bp]) for tp in ("S", "B") for bp in ("W", "M", "R")
        ]
        sc, sm__, sa, bc, bm__, ba = [
            self.type_scale[tp].intersection(self.type_cma[bp]) for tp in ("S", "B") for bp in ("C", "M", "A")
        ]

        r_sl, r_sm, r_sh, r_bl, r_bm, r_bh, \
        r_sw, r_sm_, r_sr, r_bw, r_bm_, r_br, \
        r_sc, r_sm__, r_sa, r_bc, r_bm__, r_ba = self._group_return(
            r, w_data, *(sl, sm, sh, bl, bm, bh, sw, sm_, sr, bw, bm_, br, sc, sm__, sa, bc, bm__, ba))

        smb_sv = ((r_sh + r_sm + r_sl) - (r_bh + r_bm + r_bl)) / 3
        smb_sp = ((r_sw + r_sm_ + r_sr) - (r_bw + r_bm_ + r_br)) / 3
        smb_si = ((r_sc + r_sm__ + r_sa) - (r_bc + r_bm__ + r_ba)) / 3
        smb = (smb_sv + smb_sp + smb_si) / 3

        return smb

    @property
    def rmw(self):
        r = self.stock.return_series.iloc[0]
        w_data = self.stock.circulated_price.iloc[0]

        # Combination of each group (set)
        sw, sr, bw, br = [
            self.type_scale[tp].intersection(self.type_rmw[bp]) for tp in ("S", "B") for bp in ("W", "R")
        ]

        r_sw, r_sr, r_bw, r_br = self._group_return(r, w_data, *(sw, sr, bw, br))
        rmw = ((r_br + r_sr) - (r_bw + r_sw)) / 2

        return rmw

    @property
    def cma(self):
        r = self.stock.return_series.iloc[0]
        w_data = self.stock.circulated_price.iloc[0]

        # Combination of each group (set)
        sc, sa, bc, ba = [
            self.type_scale[tp].intersection(self.type_cma[bp]) for tp in ("S", "B") for bp in ("C", "A")
        ]

        r_sc, r_sa, r_bc, r_ba = self._group_return(r, w_data, *(sc, sa, bc, ba))
        cma = ((r_ba + r_sa) - (r_bc + r_sc)) / 2

        return cma

    @property
    def factor(self):
        return {self.end: {"SMB": self.smb, "HML": self.hml, "RMW": self.rmw, "CMA": self.cma}}


class MarketFactor(IFactor):
    def __init__(self, end, freq):
        self.benchmark = datatype.MarketIndex(end, freq)
        self.end = end

    @property
    def factor(self):
        mkt = self.benchmark.excess_return_series.iloc[0]
        return {self.end: {"MKT": mkt}}
