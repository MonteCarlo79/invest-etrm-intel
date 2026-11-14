"""Dispatch-chain SQL loaders and price join (moved from apps/asset_risk/tab_diagnostics.py)."""
from __future__ import annotations

import pandas as pd
from sqlalchemy import bindparam, inspect, text

from libs.asset_risk.diagnostics import AUTO_NOM_CODE, PLANT_MAP
from libs.asset_risk.registry import asset_identity, has_asset_code


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


CHAIN_COLUMNS = ["asset_id", "asset", "capacity_mw", "ts", "nominated_mw", "da_cleared_mw",
                 "rt_cleared_mw", "actual_mw", "restriction", "soc_pct"]
_KEY = ["asset_id", "ts"]


def _load_legacy_chain(engine, asset_ids: list[int], start: str | None,
                       end: str | None) -> pd.DataFrame:
    """Manual dispatch chain (rm_dispatch_chain; ingest stopped 2026-08-30)."""
    ts = _ts_expr(engine)
    sql = f"""
        SELECT dc.asset_id, {ts} AS ts,
               dc.nominated_mw, dc.da_cleared_mw, dc.rt_cleared_mw, dc.actual_mw,
               dc.restriction, dc.soc_pct
        FROM marketdata.rm_dispatch_chain dc
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
    return df


def _asset_keys(engine, asset_ids: list[int], nom_code_map: dict[str, str] | None,
                plant_map: dict[str, str] | None) -> pd.DataFrame:
    """(asset_id, asset, capacity_mw, asset_code, plant_names) per requested id.

    Post-migration (rm_assets.asset_code present): code + ALL exchange plant
    names from the registry identity frame (武川 has two accounts). Pre-migration:
    nom_code_map / plant_map (None → the legacy literal AUTO_NOM_CODE / PLANT_MAP),
    one plant name per asset — exactly today's behaviour for the six stations.
    """
    with engine.connect() as conn:
        a = pd.read_sql(
            text("SELECT id AS asset_id, name AS asset, capacity_mw "
                 "FROM marketdata.rm_assets WHERE id IN :ids")
            .bindparams(bindparam("ids", expanding=True)),
            conn, params={"ids": asset_ids})
    a["asset_id"] = a["asset_id"].astype(int)
    if has_asset_code(engine):
        ident = asset_identity(engine).set_index("asset_id")
        a["asset_code"] = [ident["asset_code"].get(i) for i in a["asset_id"]]
        a["plant_names"] = [tuple(ident["plant_names"].get(i) or ()) for i in a["asset_id"]]
    else:
        codes = AUTO_NOM_CODE if nom_code_map is None else nom_code_map
        plants = PLANT_MAP if plant_map is None else plant_map
        a["asset_code"] = [codes.get(n) for n in a["asset"]]
        a["plant_names"] = [(plants[n],) if n in plants else () for n in a["asset"]]
    return a


def _slot96_ts(df: pd.DataFrame) -> pd.Series:
    """data_date + (slot−1)·15min — CST-naive wall clock."""
    return (pd.to_datetime(df["data_date"].astype(str).str[:10])
            + pd.to_timedelta((df["slot"].astype(int) - 1) * 15, unit="min"))


def _load_slot96(engine, table: str, value_col: str, code_to_id: dict[str, int],
                 start: str | None, end: str | None) -> pd.Series:
    """bess_nomination_96 / bess_actual_96 value series indexed by (asset_id, ts)."""
    sql = f"""
        SELECT asset_code, data_date, slot, {value_col} AS v
        FROM marketdata.{table}
        WHERE asset_code IN :codes
    """
    params: dict = {"codes": list(code_to_id)}
    if start:
        sql += " AND data_date >= :s"
        params["s"] = start[:10]
    if end:
        sql += " AND data_date < :e"
        params["e"] = end[:10]
    df = pd.read_sql(text(sql).bindparams(bindparam("codes", expanding=True)),
                     engine, params=params)
    if df.empty:
        return pd.Series(dtype=float, index=pd.MultiIndex.from_tuples([], names=_KEY))
    df["asset_id"] = df["asset_code"].map(code_to_id)
    df["ts"] = _slot96_ts(df)
    return pd.to_numeric(df.set_index(_KEY)["v"], errors="coerce")


def _ops_ts_expr(engine) -> str:
    """Beijing-wall timestamp for ops_bess_dispatch_15min.interval_start
    (TIMESTAMPTZ on Postgres; session TZ is UTC, so render via the named zone —
    never '+08:00', which POSIX-inverts to UTC−8)."""
    if engine.dialect.name == "sqlite":
        return "o.interval_start"  # fixture stores Beijing-naive TEXT
    return "(o.interval_start AT TIME ZONE 'Asia/Shanghai')"


def _load_ops_actuals(engine, code_to_id: dict[str, int], start: str | None,
                      end: str | None) -> pd.Series:
    """ops_bess_dispatch_15min.actual_dispatch_mw by (asset_id, interval_start)
    — the pre-2026-04-20 fallback (feed died 2026-04-21). Absent table → empty."""
    empty = pd.Series(dtype=float, index=pd.MultiIndex.from_tuples([], names=_KEY))
    if not inspect(engine).has_table("ops_bess_dispatch_15min", schema="marketdata"):
        return empty
    ts = _ops_ts_expr(engine)
    sql = f"""
        SELECT o.asset_code, {ts} AS ts, o.actual_dispatch_mw AS v
        FROM marketdata.ops_bess_dispatch_15min o
        WHERE o.asset_code IN :codes
    """
    params: dict = {"codes": list(code_to_id)}
    if start:
        sql += f" AND {ts} >= :s"
        params["s"] = start
    if end:
        sql += f" AND {ts} < :e"
        params["e"] = end
    df = pd.read_sql(text(sql).bindparams(bindparam("codes", expanding=True)),
                     engine, params=params)
    if df.empty:
        return empty
    df["asset_id"] = df["asset_code"].map(code_to_id)
    df["ts"] = pd.to_datetime(df["ts"])
    s = pd.to_numeric(df.set_index(_KEY)["v"], errors="coerce")
    return s[~s.index.duplicated(keep="last")]


def _load_cleared_sum(engine, plant_to_id: dict[str, int], start: str | None,
                      end: str | None) -> pd.Series:
    """Exchange-cleared MW summed over ALL of an asset's accounts per (asset_id, ts):
    accounts with a row contribute; no row anywhere → absent (NaN after reindex).

    UNIT: md_id_cleared_energy.cleared_energy_mwh is interval-average MW
    (mislabeled name, verified 2026-11-05) — no ×4. Exact plant_name IN, never LIKE.
    """
    sql = """
        SELECT plant_name, datetime AS ts, cleared_energy_mwh AS v
        FROM marketdata.md_id_cleared_energy
        WHERE plant_name IN :plants
    """
    params: dict = {"plants": list(plant_to_id)}
    if start:
        sql += " AND data_date >= :ds AND datetime >= :s"
        params["ds"] = start[:10]
        params["s"] = start
    if end:
        sql += " AND data_date <= :de AND datetime < :e"
        params["de"] = end[:10]
        params["e"] = end
    df = pd.read_sql(text(sql).bindparams(bindparam("plants", expanding=True)),
                     engine, params=params)
    if df.empty:
        return pd.Series(dtype=float, index=pd.MultiIndex.from_tuples([], names=_KEY))
    df["asset_id"] = df["plant_name"].map(plant_to_id)
    df["ts"] = pd.to_datetime(df["ts"])
    df["v"] = pd.to_numeric(df["v"], errors="coerce")
    return df.groupby(_KEY)["v"].sum(min_count=1)


def _check_sign(df: pd.DataFrame) -> None:
    """Per (asset, month) over slots with both actual and nominated: raise when
    Σ actual·nominated < 0 (sign conventions disagree). Σ == 0 carries no
    directional evidence and passes (R16: 景蓝乌尔图 2026-06, one zero slot);
    months with no overlap pass."""
    both = df[df["actual_mw"].notna() & df["nominated_mw"].notna()]
    if both.empty:
        return
    prod = both["actual_mw"].astype(float) * both["nominated_mw"].astype(float)
    month = both["ts"].dt.strftime("%Y-%m")
    for (asset, m), total in prod.groupby([both["asset"], month]).sum().items():
        if total < 0:
            raise ValueError(f"actual/nominated sign disagreement {asset} {m}")


def _load_chain(engine, asset_ids: list[int], start: str | None, end: str | None,
                nom_code_map: dict[str, str] | None = None,
                plant_map: dict[str, str] | None = None) -> pd.DataFrame:
    """Dispatch chain per (asset_id, ts), CST-naive 15-min slots. Sources by priority:

    - nominated_mw: WeCom bess_nomination_96 overrides the legacy chain on overlap
      and extends it;
    - rt_cleared_mw: legacy chain, else md_id_cleared_energy summed over all the
      asset's exchange accounts;
    - actual_mw: legacy chain, else bess_actual_96, else
      ops_bess_dispatch_15min.actual_dispatch_mw;
    - da_cleared_mw / restriction / soc_pct: legacy chain only (NaN elsewhere).

    Slots present only in the new sources extend the chain. Raises ValueError on
    an actual/nominated sign disagreement (see _check_sign). Assets without an
    asset_code (post-migration) or outside the literal maps (pre-migration)
    return legacy rows only.
    """
    start, end = _month_token_bounds(start, end)
    keys = _asset_keys(engine, asset_ids, nom_code_map, plant_map)
    legacy = _load_legacy_chain(engine, asset_ids, start, end)

    code_to_id = {c: int(i) for i, c in zip(keys["asset_id"], keys["asset_code"]) if c}
    plant_to_id = {p: int(i) for i, ps in zip(keys["asset_id"], keys["plant_names"])
                   for p in ps}
    nom = act96 = ops = cleared = None
    if code_to_id:
        from services.mengxi_nodal.nomination_curves import ensure_actual_table, ensure_table
        ensure_table(engine)
        ensure_actual_table(engine)
        nom = _load_slot96(engine, "bess_nomination_96", "nominated_mw", code_to_id, start, end)
        act96 = _load_slot96(engine, "bess_actual_96", "actual_mw", code_to_id, start, end)
        ops = _load_ops_actuals(engine, code_to_id, start, end)
    if plant_to_id:
        cleared = _load_cleared_sum(engine, plant_to_id, start, end)
    extra = [s for s in (nom, act96, ops, cleared) if s is not None and not s.empty]

    base = legacy.set_index(_KEY)
    idx = base.index
    for s in extra:
        idx = idx.union(s.index, sort=False)
    if len(idx) == 0:
        return pd.DataFrame(columns=CHAIN_COLUMNS)
    out = base.reindex(idx)
    for col in ("nominated_mw", "da_cleared_mw", "rt_cleared_mw", "actual_mw", "soc_pct"):
        out[col] = pd.to_numeric(out[col], errors="coerce")
    if nom is not None and not nom.empty:
        out["nominated_mw"] = nom.reindex(idx).combine_first(out["nominated_mw"])
    if cleared is not None and not cleared.empty:
        out["rt_cleared_mw"] = out["rt_cleared_mw"].combine_first(cleared.reindex(idx))
    for s in (act96, ops):
        if s is not None and not s.empty:
            out["actual_mw"] = out["actual_mw"].combine_first(s.reindex(idx))
    out = out.reset_index()
    ident = keys.set_index("asset_id")
    out["asset_id"] = out["asset_id"].astype(int)
    out["asset"] = out["asset_id"].map(ident["asset"])
    out["capacity_mw"] = out["asset_id"].map(ident["capacity_mw"])
    out["ts"] = pd.to_datetime(out["ts"])
    out = out.sort_values(_KEY, kind="stable").reset_index(drop=True)[CHAIN_COLUMNS]
    _check_sign(out)
    return out


load_chain = _load_chain


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
