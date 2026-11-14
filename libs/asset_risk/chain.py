"""Dispatch-chain SQL loaders and price join (moved from apps/asset_risk/tab_diagnostics.py)."""
from __future__ import annotations

import numpy as np
import pandas as pd
from sqlalchemy import bindparam, text

from libs.asset_risk.diagnostics import AUTO_NOM_CODE, PLANT_MAP


def _ts_expr(engine) -> str:
    """Beijing-wall timestamp expression for rm_dispatch_chain.interval_start."""
    if engine.dialect.name == "sqlite":
        return "dc.interval_start"  # fixture stores Beijing-naive TEXT
    return "(dc.interval_start AT TIME ZONE 'Asia/Shanghai')"



def _month_token_bounds(start: str | None, end: str | None) -> tuple[str | None, str | None]:
    """Normalize 'YYYY-MM' month tokens to valid date bounds (start → first of
    month, end → first of next month, exclusive). Full ISO dates pass through."""
    if start and len(start) == 7:
        start = start + "-01"
    if end and len(end) == 7:
        end = (pd.Timestamp(end + "-01") + pd.offsets.MonthBegin(1)).strftime("%Y-%m-%d")
    return start, end


def _load_chain(engine, asset_ids: list[int], start: str | None, end: str | None) -> pd.DataFrame:
    ts = _ts_expr(engine)
    start, end = _month_token_bounds(start, end)
    sql = f"""
        SELECT dc.asset_id, a.name AS asset, a.capacity_mw, {ts} AS ts,
               dc.nominated_mw, dc.da_cleared_mw, dc.rt_cleared_mw, dc.actual_mw,
               dc.restriction, dc.soc_pct
        FROM marketdata.rm_dispatch_chain dc
        JOIN marketdata.rm_assets a ON a.id = dc.asset_id
        WHERE dc.asset_id IN :ids
    """
    params: dict = {"ids": asset_ids}
    if start:
        sql += f" AND {ts} >= :start"
        params["start"] = start
    if end:
        sql += f" AND {ts} < :end"
        params["end"] = end
    stmt = text(sql).bindparams(bindparam("ids", expanding=True))
    with engine.connect() as conn:
        df = pd.read_sql(stmt, conn, params=params)
    df["ts"] = pd.to_datetime(df["ts"])
    return _merge_auto_nominations(
        df, _load_auto_nominations(engine, asset_ids, start, end))


def _load_auto_nominations(engine, asset_ids: list[int],
                           start: str | None, end: str | None) -> pd.DataFrame:
    """Auto-scraped nominations for the requested rm_assets ids:
    (asset_id, ts, auto_nominated_mw, auto_rt_cleared_mw), ts naive Beijing
    (data_date + (slot−1)·15min — the same frame the PG branch of _load_chain
    renders). Empty frame when no requested asset maps to a registry code.

    auto_rt_cleared_mw is md_id_cleared_energy.cleared_energy_mwh (MW-scale,
    per the 2026-11-05 unit verification) — used
    ONLY to fill chain-uncovered slots (the manual chain ingest stopped
    2026-08-30), never to override the chain's own values.
    """
    start, end = _month_token_bounds(start, end)
    with engine.connect() as conn:
        names = pd.read_sql(
            text("SELECT id, name FROM marketdata.rm_assets WHERE id IN :ids")
            .bindparams(bindparam("ids", expanding=True)),
            conn, params={"ids": asset_ids})
    id_to_code = {int(r.id): AUTO_NOM_CODE[r.name]
                  for r in names.itertuples() if r.name in AUTO_NOM_CODE}
    if not id_to_code:
        return pd.DataFrame(
            columns=["asset_id", "ts", "auto_nominated_mw", "auto_rt_cleared_mw"])

    from services.mengxi_nodal.nomination_curves import ensure_table as _ensure_nom
    _ensure_nom(engine)
    sql = """
        SELECT asset_code, data_date, slot, nominated_mw
        FROM marketdata.bess_nomination_96
        WHERE asset_code IN :codes
    """
    params: dict = {"codes": list(id_to_code.values())}
    if start:
        sql += " AND data_date >= :s"
        params["s"] = start[:10]
    if end:
        sql += " AND data_date < :e"
        params["e"] = end[:10]
    nom = pd.read_sql(text(sql).bindparams(bindparam("codes", expanding=True)),
                      engine, params=params)
    code_to_id = {c: i for i, c in id_to_code.items()}
    out = pd.DataFrame(columns=["asset_id", "ts", "auto_nominated_mw",
                                "auto_rt_cleared_mw"])
    if not nom.empty:
        nom["asset_id"] = nom["asset_code"].map(code_to_id)
        nom["ts"] = (pd.to_datetime(nom["data_date"].astype(str).str[:10])
                     + pd.to_timedelta((nom["slot"].astype(int) - 1) * 15,
                                       unit="min"))
        out = nom.rename(columns={"nominated_mw": "auto_nominated_mw"})[
            ["asset_id", "ts", "auto_nominated_mw"]]
        out["auto_rt_cleared_mw"] = np.nan

    # RT-cleared fill for the extension rows (chain has no Sep+ slots at all).
    # UNIT: md_id_cleared_energy.cleared_energy_mwh is average MW per interval
    # (mislabeled name, verified 2026-11-05) — no ×4.
    plants = {i: PLANT_MAP[n] for i, n in zip(names["id"], names["name"])
              if int(i) in id_to_code and n in PLANT_MAP}
    if plants:
        sql = """
            SELECT plant_name, datetime, cleared_energy_mwh AS rt_mw
            FROM marketdata.md_id_cleared_energy
            WHERE plant_name IN :plants
        """
        params = {"plants": list(plants.values())}
        if start:
            sql += " AND datetime >= :s"
            params["s"] = start
        if end:
            sql += " AND datetime < :e"
            params["e"] = end
        rt = pd.read_sql(text(sql).bindparams(bindparam("plants", expanding=True)),
                         engine, params=params)
        if not rt.empty:
            plant_to_id = {p: int(i) for i, p in plants.items()}
            rt["asset_id"] = rt["plant_name"].map(plant_to_id)
            rt = rt.rename(columns={"datetime": "ts"})
            rt["ts"] = pd.to_datetime(rt["ts"])
            if out.empty:
                out = rt[["asset_id", "ts"]].assign(auto_nominated_mw=np.nan)
                out["auto_rt_cleared_mw"] = rt["rt_mw"]
            else:
                out = out.merge(
                    rt[["asset_id", "ts", "rt_mw"]], on=["asset_id", "ts"],
                    how="outer")
                out["auto_rt_cleared_mw"] = out["auto_rt_cleared_mw"].combine_first(
                    out.pop("rt_mw"))
    return out


def _merge_auto_nominations(chain: pd.DataFrame, auto: pd.DataFrame) -> pd.DataFrame:
    """WeCom auto nominations win on (asset_id, ts) overlap; auto-only slots
    EXTEND the chain (their rt_cleared_mw fills from the exchange-cleared
    source; da/actual/soc/restriction stay NaN — honest gaps). Identity
    columns (asset, capacity_mw) fill from the chain's own mapping."""
    if auto.empty:
        return chain
    key = ["asset_id", "ts"]
    base = chain.set_index(key)
    add = auto.set_index(key)
    idx = base.index.union(add.index, sort=False)
    out = base.reindex(idx)
    nom = add["auto_nominated_mw"].reindex(idx)
    out["nominated_mw"] = nom.combine_first(out["nominated_mw"])
    rtc = add["auto_rt_cleared_mw"].reindex(idx)
    out["rt_cleared_mw"] = out["rt_cleared_mw"].combine_first(rtc)
    id_asset = dict(zip(chain["asset_id"], chain["asset"]))
    id_cap = dict(zip(chain["asset_id"], chain["capacity_mw"]))
    level_ids = out.index.get_level_values("asset_id")
    out["asset"] = out["asset"].fillna(pd.Series(level_ids.map(id_asset),
                                                 index=out.index))
    out["capacity_mw"] = out["capacity_mw"].fillna(
        pd.Series(level_ids.map(id_cap), index=out.index))
    return out.reset_index()


def _load_prices(engine, plants: list[str], start: str | None, end: str | None) -> pd.DataFrame:
    start, end = _month_token_bounds(start, end)
    sql = """
        SELECT plant_name, datetime, cleared_price
        FROM marketdata.md_id_cleared_energy
        WHERE plant_name IN :plants
    """
    params: dict = {"plants": plants}
    if start:
        sql += " AND datetime >= :start"
        params["start"] = start
    if end:
        sql += " AND datetime < :end"
        params["end"] = end
    stmt = text(sql).bindparams(bindparam("plants", expanding=True))
    with engine.connect() as conn:
        df = pd.read_sql(stmt, conn, params=params)
    df["datetime"] = pd.to_datetime(df["datetime"])
    return df


def _attach_prices(chain: pd.DataFrame, prices: pd.DataFrame,
                   plant_map: dict[str, str]) -> pd.DataFrame:
    """Add price_cny_mwh: per-plant RT price mapped at ts + 15min (period-end).
    NaN where no price row — MWh sums keep the row, ¥ sums skip it."""
    df = chain.copy()
    df["price_cny_mwh"] = pd.NA
    for asset, plant in plant_map.items():
        p = prices[prices["plant_name"] == plant]
        if p.empty:
            continue
        s = pd.Series({pd.Timestamp(r[0]): float(r[1])
                       for r in zip(p["datetime"], p["cleared_price"]) if pd.notna(r[1])})
        s = s[~s.index.duplicated(keep="last")].sort_index()
        mask = df["asset"] == asset
        df.loc[mask, "price_cny_mwh"] = (df.loc[mask, "ts"] + pd.Timedelta(minutes=15)).map(s)
    df["price_cny_mwh"] = pd.to_numeric(df["price_cny_mwh"], errors="coerce")
    return df
