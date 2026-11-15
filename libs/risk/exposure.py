"""Exposure frame (physical vs financial MWh) and curve-aware MtM. Pure: no DB, no UI."""
from __future__ import annotations

import numpy as np
import pandas as pd

from libs.curves.curve import curve_lookup
from libs.positions.conventions import signed_mwh

KEYS = ["book_id", "province", "delivery_month", "tou_block"]
_OUT = KEYS + ["physical_mwh", "financial_mwh", "net_mwh",
               "mark_cny_mwh", "mtm_cny", "hedge_ratio"]


def exposure(positions: pd.DataFrame, curve: pd.DataFrame) -> pd.DataFrame:
    if positions is None or positions.empty:
        return pd.DataFrame(columns=_OUT)
    df = positions.copy()
    df["signed"] = [signed_mwh(d, v) for d, v in zip(df.direction, df.volume_mwh)]
    df["is_phys"] = df.source == "physical"
    rows = []
    for key, g in df.groupby(KEYS, sort=True):
        book, prov, month, block = key
        phys = float(g.loc[g.is_phys, "signed"].sum())
        fin = float(g.loc[~g.is_phys, "signed"].sum())
        mark = curve_lookup(curve, prov, month, block)
        mark = np.nan if mark is None else float(mark)
        priced = g[g.price_cny_mwh.notna()]
        if np.isnan(mark):
            mtm = np.nan
        else:
            mtm = float((priced.signed * (mark - priced.price_cny_mwh.astype(float))).sum())
        rows.append(dict(book_id=book, province=prov, delivery_month=month, tou_block=block,
                         physical_mwh=phys, financial_mwh=fin, net_mwh=phys + fin,
                         mark_cny_mwh=mark, mtm_cny=mtm,
                         hedge_ratio=-fin / phys if phys != 0 else np.nan))
    return pd.DataFrame(rows, columns=_OUT)


def portfolio_exposure(exp: pd.DataFrame) -> pd.DataFrame:
    k = ["province", "delivery_month", "tou_block"]
    if exp is None or exp.empty:
        return pd.DataFrame(columns=k + ["physical_mwh", "financial_mwh", "net_mwh",
                                         "mark_cny_mwh", "mtm_cny", "hedge_ratio"])
    out = exp.groupby(k, sort=True).agg(
        physical_mwh=("physical_mwh", "sum"), financial_mwh=("financial_mwh", "sum"),
        net_mwh=("net_mwh", "sum"), mark_cny_mwh=("mark_cny_mwh", "first"),
        mtm_cny=("mtm_cny", lambda s: s.sum(min_count=1))).reset_index()
    out["hedge_ratio"] = np.where(out.physical_mwh != 0,
                                  -out.financial_mwh / out.physical_mwh.replace(0, np.nan), np.nan)
    return out
