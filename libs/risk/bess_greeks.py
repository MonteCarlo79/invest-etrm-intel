"""Per-month BESS delta/vega from the Margrabe spread-call strip. Pure: no DB, no UI.

Mapping onto libs.decision_models.bess_spread_call_strip._run (unchanged):
  q_max  = roundtrip_eff × power_mw × duration_h            (MWh/day)
  F2_eff = offpeak / roundtrip_eff + om_cost (om_cost = 0)  (¥/MWh)
  sigma_s = _spread_vol(peak_vol, offpeak_vol, corr)
Each month is a strip of n_days daily calls. T_i continues the strip
convention (i/252 for day i of remaining life counted from `as_of`), so a
month starting d0 days after as_of is valued as
_strip_value(d0 + n) − _strip_value(d0).
"""
from __future__ import annotations

import calendar
from datetime import date

import pandas as pd

from libs.decision_models.bess_spread_call_strip import _spread_vol, _strip_value

COLUMNS = ["delivery_month", "n_days", "peak_cny_mwh", "offpeak_cny_mwh", "value_cny",
           "delta_peak_mwh", "delta_offpeak_mwh", "vega_cny_per_volpt", "note"]


def _mark(curve: pd.DataFrame, province: str, month: date, block: str) -> float | None:
    """Exact-block curve mark (no fallback to base)."""
    if curve is None or curve.empty:
        return None
    dm = curve["delivery_month"].map(lambda v: pd.Timestamp(v).to_period("M"))
    hit = curve[(curve["province"] == province) & (dm == pd.Timestamp(month).to_period("M"))
                & (curve["tou_block"] == block)]
    return None if hit.empty else float(hit["price_cny_mwh"].iloc[0])


def bess_monthly_greeks(power_mw, duration_h, roundtrip_eff, months: list[date], curve,
                        province, peak_vol: float, offpeak_vol: float, corr: float = 0.85,
                        *, as_of: date) -> pd.DataFrame:
    q_max = roundtrip_eff * power_mw * duration_h

    def value(pk: float, off: float, sp: float, so: float, d0: int, n: int) -> float:
        f2 = off / roundtrip_eff
        s = _spread_vol(sp, so, corr)
        return _strip_value(pk, f2, s, q_max, d0 + n, 0.0) - _strip_value(pk, f2, s, q_max, d0, 0.0)

    rows = []
    nan = float("nan")
    for m in months:
        m = date(pd.Timestamp(m).year, pd.Timestamp(m).month, 1)
        dim = calendar.monthrange(m.year, m.month)[1]
        nxt = date(m.year + (m.month == 12), m.month % 12 + 1, 1)
        if as_of >= nxt:
            rows.append([m, 0, nan, nan, 0.0, 0.0, 0.0, 0.0, "expired"])
            continue
        d0 = max((m - as_of).days, 0)
        n = dim if as_of <= m else (nxt - as_of).days
        pk = _mark(curve, province, m, "peak")
        off = _mark(curve, province, m, "offpeak")
        if pk is None or off is None:
            rows.append([m, n, pk if pk is not None else nan, off if off is not None else nan,
                         nan, nan, nan, nan, "no peak/offpeak mark"])
            continue
        v = value(pk, off, peak_vol, offpeak_vol, d0, n)
        dpk = (value(pk + 1, off, peak_vol, offpeak_vol, d0, n)
               - value(pk - 1, off, peak_vol, offpeak_vol, d0, n)) / 2.0
        doff = (value(pk, off + 1, peak_vol, offpeak_vol, d0, n)
                - value(pk, off - 1, peak_vol, offpeak_vol, d0, n)) / 2.0
        vega = (value(pk, off, peak_vol + 0.01, offpeak_vol + 0.01, d0, n)
                - value(pk, off, peak_vol - 0.01, offpeak_vol - 0.01, d0, n)) / 2.0
        rows.append([m, n, pk, off, v, dpk, doff, vega, ""])
    return pd.DataFrame(rows, columns=COLUMNS)
