# -*- coding: utf-8 -*-
"""零碳46 (悦盛昌渠) monthly settlement replication engine.

Replicates the grid 上网电费结算单 (book 6) from market data + trades:

    电能电费 = Σ_intervals gen_i × RT_i          (spot value at nodal price)
             + Σ_contracts vol_c × (price_c − ref)   (medium/long-term CfD)
    绿电溢价 = Σ_contracts vol_c × env_c
    fees     = bill values (allocated market-wide, not computable bottom-up)

Data sources:
- md_id_cleared_energy: 15-min intraday cleared schedule (MW, despite the
  column name — validated: Feb/Jun ×0.25 match bill spot volume to <0.5%)
- md_rt_nodal_price: 15-min RT nodal price for 内蒙.悦盛昌渠风光储电站/220kV.1M
- md_settlement_ref_price: hourly CfD reference (呼包以东/以西/system)
- wind_trades / trades files: monthly contract position
- rm_settlement_items: book 6 bill line items (replication target)
"""
from __future__ import annotations

import pandas as pd


# ── pure calculations ────────────────────────────────────────────────────────

def cfd_value(position: pd.DataFrame, ref_price: float) -> float:
    """CfD settlement: Σ vol_c × (contract price − reference price)."""
    if position.empty:
        return 0.0
    return float((position["volume_mwh"] * (position["energy_price"] - ref_price)).sum())


def green_value(position: pd.DataFrame) -> float:
    """Green premium: Σ vol_c × environmental value (missing env = 0)."""
    if position.empty:
        return 0.0
    env = position["env_value"].fillna(0.0)
    return float((position["volume_mwh"] * env).sum())


def capture_price(intervals: pd.DataFrame) -> float | None:
    """Generation-weighted RT price: Σ(gen×RT)/Σgen."""
    total = intervals["gen_mwh"].sum()
    if total == 0:
        return None
    return float((intervals["gen_mwh"] * intervals["rt_price"]).sum() / total)


def id_cleared_to_energy(raw: pd.DataFrame) -> pd.DataFrame:
    """Convert md_id_cleared_energy rows (MW per 15-min) to energy (MWh).

    Negative intervals (storage charging) floor at 0 for generation.
    """
    out = raw.copy()
    out["gen_mwh"] = (out["cleared_energy_mwh"].clip(lower=0.0)) * 0.25
    return out


# ── green premium: Σ_t min(合约曲线_t, 实际计量_t) ───────────────────────────

_WINDOW_RE = __import__("re").compile(r"\((\d{8})-(\d{8})\)")


def _contract_windows(position: pd.DataFrame, month: str) -> pd.DataFrame:
    """Per-contract delivery window (clamped to the month) from 品种 name.

    Falls back to the whole month when no (YYYYMMDD-YYYYMMDD) range is present.
    Returns position + [start, end] (Timestamps, end exclusive).
    """
    mstart = pd.Timestamp(f"{month}-01")
    mend = mstart + pd.offsets.MonthBegin(1)
    out = position.copy()
    starts, ends = [], []
    for tt in out["trade_type"]:
        m = _WINDOW_RE.search(str(tt))
        if m:
            s = pd.Timestamp(m.group(1))
            e = pd.Timestamp(m.group(2)) + pd.Timedelta(days=1)
        else:
            s, e = mstart, mend
        starts.append(max(s, mstart))
        ends.append(min(e, mend))
    out["start"], out["end"] = starts, ends
    return out


def green_covered(position: pd.DataFrame, intervals: pd.DataFrame, month: str) -> pd.DataFrame:
    """Per-contract covered volume: Σ_t min(合约曲线_t, 实际_t) allocated by rate share.

    Contract curves are flat within their delivery window (直线). Each 15-min
    interval's covered volume min(total contract rate, actual) is split across
    contracts pro-rata to their rate. Returns position + [covered_mwh].
    """
    if position.empty or intervals.empty:
        out = position.copy()
        out["covered_mwh"] = 0.0
        return out

    cons = _contract_windows(position, month)
    idx = intervals["datetime"].to_numpy()
    gen = intervals["gen_mwh"].to_numpy(dtype=float)

    # rate matrix: one column per contract, MWh per 15-min inside its window
    rates = []
    for _, c in cons.iterrows():
        in_win = (idx >= c["start"].to_datetime64()) & (idx < c["end"].to_datetime64())
        n = int(in_win.sum())
        rates.append(in_win.astype(float) * (float(c["volume_mwh"]) / n if n else 0.0))
    rate_mat = pd.DataFrame(rates).T  # intervals × contracts

    import numpy as np
    total_rate = rate_mat.sum(axis=1).to_numpy()
    covered_total = np.minimum(total_rate, gen)
    share = rate_mat.div(pd.Series(np.where(total_rate == 0, np.nan, total_rate)), axis=0).fillna(0.0)
    covered = share.mul(pd.Series(covered_total), axis=0).sum(axis=0).to_numpy()

    out = cons.copy()
    out["covered_mwh"] = covered
    return out


def green_premium(position: pd.DataFrame, intervals: pd.DataFrame, month: str) -> float:
    """绿电溢价 = Σ_c covered_c × env_c (per-interval min, 曲线合理度 basis)."""
    if position.empty:
        return 0.0
    cov = green_covered(position, intervals, month)
    env = cov["env_value"].fillna(0.0)
    return float((cov["covered_mwh"] * env).sum())


# ── DB access ────────────────────────────────────────────────────────────────

def load_intervals(engine, plant: str, node: str, month: str) -> pd.DataFrame:
    """Per-interval generation proxy (MWh) + RT nodal price for one month.

    month = 'YYYY-MM'. Returns columns: datetime, gen_mwh, rt_price.
    Reads the wind_dispatch_15min extract (indexed PK) — a direct
    md_id_cleared_energy × md_rt_nodal_price JOIN takes minutes per month
    over the cross-Pacific link.
    """
    from sqlalchemy import text

    start = f"{month}-01"
    end = pd.Timestamp(start) + pd.offsets.MonthBegin(1)
    q = text("""
        SELECT datetime, gen_mwh, rt_price
        FROM marketdata.wind_dispatch_15min
        WHERE plant_name = :plant
          AND datetime >= :start AND datetime < :end
        ORDER BY datetime
    """)
    return pd.read_sql(q, engine, params={"plant": plant, "start": start, "end": end})


def load_ref_price_avg(engine, month: str, column: str = "呼包以西加权平均价格_元_mwh") -> float | None:
    """Monthly average settlement reference price (simple hourly mean)."""
    from sqlalchemy import text

    if column not in ("system_settlement_price", "呼包以东加权平均价格_元_mwh", "呼包以西加权平均价格_元_mwh"):
        raise ValueError(f"unknown ref price column: {column}")
    start = f"{month}-01"
    end = pd.Timestamp(start) + pd.offsets.MonthBegin(1)
    q = text(f"""
        SELECT AVG({column}) FROM marketdata.md_settlement_ref_price
        WHERE datetime >= :start AND datetime < :end
    """)
    with engine.connect() as conn:
        val = conn.execute(q, {"start": start, "end": end}).scalar()
    return float(val) if val is not None else None


def load_bill_items(engine, book_id: int, month: str) -> pd.DataFrame:
    """Bill line items (replication target) for one settlement month."""
    from sqlalchemy import text

    q = text("""
        SELECT i.category, i.peak_period, i.volume_mwh, i.price_cny_kwh,
               i.amount_cny, i.notes
        FROM marketdata.rm_settlement_items i
        JOIN marketdata.rm_settlements s ON i.settlement_id = s.id
        WHERE s.book_id = :book AND s.settlement_month = :month
        ORDER BY i.category, i.peak_period
    """)
    return pd.read_sql(q, engine, params={"book": book_id, "month": f"{month}-01"})


def load_clearing(engine, month: str) -> pd.DataFrame:
    """Exchange daily-clearing rows for one month (empty if not loaded)."""
    from sqlalchemy import text

    start = f"{month}-01"
    end = pd.Timestamp(start) + pd.offsets.MonthBegin(1)
    q = text("""
        SELECT datetime, metered_mwh, energy_fee, rt_nodal_price,
               contract_mwh, contract_price, curve_min
        FROM marketdata.wind_daily_clearing
        WHERE datetime >= :start AND datetime < :end
        ORDER BY datetime
    """)
    return pd.read_sql(q, engine, params={"start": start, "end": end})
