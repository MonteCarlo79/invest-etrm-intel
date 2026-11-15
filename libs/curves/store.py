"""DB store for the reference curve (marketdata.rm_forward_curves, product='reference')."""
from __future__ import annotations

import logging
from datetime import date

import pandas as pd
from sqlalchemy import bindparam, text

from libs.curves.curve import CURVE_COLUMNS, _to_month
from libs.positions.conventions import PROVINCE_ALIASES, canonical_province

log = logging.getLogger(__name__)

PRODUCT = "reference"
_PRICE_COLS = ("rt_price", "da_price")
PROVINCE_ALIASES_FLAT = frozenset(a for al in PROVINCE_ALIASES.values() for a in al)


def load_curve(engine, province: str | None = None, as_of: date | None = None) -> pd.DataFrame:
    """Latest saved reference curve per province (curve_date <= as_of if given)."""
    sql = ("SELECT province, delivery_date, tou_block, price_cny_kwh, source, curve_date "
           "FROM marketdata.rm_forward_curves WHERE product = :p")
    params: dict = {"p": PRODUCT}
    if province is not None:
        sql += " AND province IN :provs"
        params["provs"] = tuple(set(PROVINCE_ALIASES[canonical_province(province)]))
    if as_of is not None:
        sql += " AND curve_date <= :as_of"
        params["as_of"] = as_of
    stmt = text(sql)
    if province is not None:
        stmt = stmt.bindparams(bindparam("provs", expanding=True))
    with engine.connect() as c:
        df = pd.read_sql(stmt, c, params=params)
    if df.empty:
        return pd.DataFrame(columns=CURVE_COLUMNS)
    known = df["province"].isin(PROVINCE_ALIASES_FLAT)
    if not known.all():  # read path (I6): skip unmapped provinces, never raise
        log.warning("load_curve: skipping %d rows with unmapped province(s) %s", int((~known).sum()),
                    sorted(set(df.loc[~known, "province"].astype(str))))
        df = df[known]
        if df.empty:
            return pd.DataFrame(columns=CURVE_COLUMNS)
    df["province"] = df["province"].map(canonical_province)
    df["curve_date"] = pd.to_datetime(df["curve_date"])
    df = df[df["curve_date"] == df.groupby("province")["curve_date"].transform("max")]
    df["delivery_month"] = df["delivery_date"].map(_to_month)
    df["tou_block"] = df["tou_block"].fillna("base")
    df["price_cny_mwh"] = df["price_cny_kwh"] * 1000.0
    return df[CURVE_COLUMNS].sort_values(
        ["province", "delivery_month", "tou_block"]).reset_index(drop=True)


def save_curve(engine, curve: pd.DataFrame, curve_date: date) -> int:
    """Idempotent: DELETE-then-INSERT in one transaction, scoped to
    (province, product='reference', curve_date) for provinces in `curve`."""
    c = curve.dropna(subset=["delivery_month", "price_cny_mwh"])
    no_src = c["source"].isna() | (c["source"].astype(str).str.strip() == "")
    if no_src.any():  # M3: never write source '' — drop with a warning
        log.warning("save_curve: dropping %d rows without a source", int(no_src.sum()))
        c = c[~no_src]
    if c.empty:
        return 0
    rows = [{"province": canonical_province(r.province), "product": PRODUCT, "cd": curve_date,
             "dd": _to_month(r.delivery_month), "blk": r.tou_block,
             "kwh": float(r.price_cny_mwh) / 1000.0, "src": r.source}
            for r in c.itertuples()]
    with engine.begin() as conn:
        for prov in {r["province"] for r in rows}:
            conn.execute(text("DELETE FROM marketdata.rm_forward_curves WHERE province IN :p "
                              "AND product = :prod AND curve_date = :cd"
                              ).bindparams(bindparam("p", expanding=True)),
                         {"p": tuple(PROVINCE_ALIASES[prov]), "prod": PRODUCT, "cd": curve_date})
        conn.execute(text(
            "INSERT INTO marketdata.rm_forward_curves (province, product, curve_date, "
            "delivery_date, delivery_hour, tou_block, price_cny_kwh, source) "
            "VALUES (:province, :product, :cd, :dd, NULL, :blk, :kwh, :src)"), rows)
    return len(rows)


def load_spot_hourly(engine, province: str, start: date, end: date,
                     price_col: str = "rt_price") -> pd.DataFrame:
    """Hourly spot prices as (datetime, price) for spot_seasonal; all province aliases."""
    if price_col not in _PRICE_COLS:
        raise ValueError(f"price_col must be one of {_PRICE_COLS}")
    names = tuple(set(PROVINCE_ALIASES[canonical_province(province)]) | {province})
    stmt = text(f"SELECT datetime, {price_col} AS price FROM marketdata.spot_prices_hourly "
                "WHERE province IN :provs AND datetime >= :s AND datetime < :e "
                "ORDER BY datetime").bindparams(bindparam("provs", expanding=True))
    with engine.connect() as c:
        return pd.read_sql(stmt, c, params={"provs": names, "s": start,
                                            "e": end + pd.Timedelta(days=1)})
