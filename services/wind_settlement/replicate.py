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


# ── DB access ────────────────────────────────────────────────────────────────

def load_intervals(engine, plant: str, node: str, month: str) -> pd.DataFrame:
    """Per-interval generation proxy (MWh) + RT nodal price for one month.

    month = 'YYYY-MM'. Returns columns: datetime, gen_mwh, rt_price.
    """
    from sqlalchemy import text

    start = f"{month}-01"
    end = pd.Timestamp(start) + pd.offsets.MonthBegin(1)
    q = text("""
        SELECT e.datetime,
               GREATEST(e.cleared_energy_mwh, 0) * 0.25 AS gen_mwh,
               p.node_price AS rt_price
        FROM marketdata.md_id_cleared_energy e
        LEFT JOIN marketdata.md_rt_nodal_price p
               ON p.datetime = e.datetime AND p.node_name = :node
        WHERE e.plant_name = :plant
          AND e.datetime >= :start AND e.datetime < :end
        ORDER BY e.datetime
    """)
    return pd.read_sql(q, engine, params={"plant": plant, "node": node, "start": start, "end": end})


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
