# services/retail_risk/reconcile.py
"""Goal 1: reconcile trades with settlement invoices, per book x month.

Checks:
  1. midlong:  Σ rm_positions cost (month)  vs  Σ items[midlong_energy]
  2. spot:     exposure x RT VWAP           vs  Σ items[spot_energy]
               exposure = invoice settled volume - cleared midlong volume
  3. residual: invoice total - (midlong+spot) vs itemised fees (imbalance/penalty/
               surcharges/redistribution/rule_charges/other/green_premium)
Status: matched | explained | flagged.

Hierarchy rule (subject codes are nested: 0101 > 010102 > 0101020302):
category AMOUNTS use the min-code-length line per category; fee sums and any
'other' buckets use lines with code length >= 4 only — the 2-digit top lines
('01 电量清分') exist for settled volume, not for category sums.
"""
from __future__ import annotations

import datetime
from dataclasses import dataclass, field

import pandas as pd
from sqlalchemy import text

from services.retail_risk import schemas

FEE_CATEGORIES = ["imbalance", "penalty", "govt_surcharges", "market_redistribution",
                  "rule_charges", "other", "green_premium"]


@dataclass
class ReconLine:
    name: str
    expected_cny: float | None
    invoice_cny: float | None
    delta_cny: float | None
    note: str = ""


@dataclass
class ReconResult:
    book_id: int
    month: datetime.date
    status: str
    lines: list[ReconLine] = field(default_factory=list)
    tolerance_cny: float = 0.0


def spot_vwap_series(df_prices: pd.DataFrame) -> float:
    return float(df_prices["rt_price"].dropna().mean())


def expected_spot_cost(exposure_mwh: float, rt_vwap: float) -> float:
    return exposure_mwh * rt_vwap


def tolerance_for(invoice_total_cny: float, pct: float, floor: float) -> float:
    return max(pct * abs(invoice_total_cny), floor)


def judge(lines: list[ReconLine], tol: float) -> str:
    core = [l for l in lines if l.name in ("midlong", "spot", "total")]
    residual = next((l for l in lines if l.name == "residual"), None)
    if all(l.delta_cny is not None and abs(l.delta_cny) <= tol for l in core):
        return "matched"
    core12 = [l for l in lines if l.name in ("midlong", "spot")]
    if (all(l.delta_cny is not None and abs(l.delta_cny) <= tol for l in core12)
            and residual is not None and residual.delta_cny is not None
            and abs(residual.delta_cny) <= tol):
        return "explained"
    return "flagged"


def invoice_by_category_frame(items: pd.DataFrame) -> dict[str, float]:
    """Per-category amounts from hierarchical subject lines.

    For each category, take lines whose subject code (notes) has the MINIMUM
    length within that category (top line of the subtree), ignoring longer
    sub-lines. The 2-digit header lines are excluded from category sums.
    """
    if items.empty:
        return {}
    df = items.copy()
    df["_code"] = df["notes"].fillna("").str.extract(r"^(\d+)")[0]
    df["_clen"] = df["_code"].str.len().fillna(99)
    df = df[df["_clen"] >= 4]
    out: dict[str, float] = {}
    for cat, g in df.groupby("category"):
        top = g[g["_clen"] == g["_clen"].min()]
        out[cat] = float(top["amount_cny"].sum())
    return out


def _invoice_by_category(conn, book_id: int, month: datetime.date) -> dict[str, float]:
    df = pd.read_sql(text("""
        SELECT si.category, si.amount_cny, si.notes
        FROM marketdata.rm_settlement_items si
        JOIN marketdata.rm_settlements s ON s.id = si.settlement_id
        WHERE s.book_id = :b AND s.settlement_month = :m
    """), conn, params={"b": book_id, "m": month})
    return invoice_by_category_frame(df)


def _invoice_totals(conn, book_id: int, month: datetime.date) -> dict:
    """Invoice total + settled volume.

    The total comes from rm_settlements ALONE — joining items repeats
    total_amount_cny once per item row (fan-out: total x N items).
    Settled volume must NOT be Σ all item volumes — hierarchical subject lines
    (01 > 0101 > 010102…) double/triple count. Rule: the volume of the item with
    the SHORTEST subject code (top line '01 电量清分' = total settled); fallback
    Σ spot_energy volumes (山东 7021 daily RT rows carry actual load)."""
    total = pd.read_sql(text("""
        SELECT SUM(total_amount_cny) AS total FROM marketdata.rm_settlements
        WHERE book_id = :b AND settlement_month = :m
          AND COALESCE(raw_data->>'total_kind', 'margin') = 'margin'
    """), conn, params={"b": book_id, "m": month}).iloc[0]["total"]
    items = pd.read_sql(text("""
        SELECT si.volume_mwh, si.category, si.notes
        FROM marketdata.rm_settlement_items si
        JOIN marketdata.rm_settlements s ON s.id = si.settlement_id
        WHERE s.book_id = :b AND s.settlement_month = :m
    """), conn, params={"b": book_id, "m": month})
    return {"total": float(total) if pd.notna(total) else None,
            "settled_vol": settle_volume(items)}


def settle_volume(items: pd.DataFrame) -> float | None:
    if items.empty:
        return None
    codes = items["notes"].fillna("").str.extract(r"^(\d+)")[0]
    if codes.notna().any():
        top = items.loc[codes[codes.notna()].str.len().idxmin()]
        if pd.notna(top["volume_mwh"]):
            return float(top["volume_mwh"])
    spot = items[items["category"] == "spot_energy"]["volume_mwh"].dropna()
    return float(spot.sum()) if not spot.empty else None


def _positions_cost(conn, book_id: int, month: datetime.date) -> tuple[float, float]:
    """Direction-signed: sell-backs （日滚动卖出, 合同转让） NET against buys —
    the invoice's midlong top line is net of them (review I1)."""
    row = pd.read_sql(text("""
        SELECT SUM(CASE WHEN direction = 'sell' THEN -1 ELSE 1 END * volume_mwh * price_cny_mwh) AS cost,
               SUM(CASE WHEN direction = 'sell' THEN -1 ELSE 1 END * volume_mwh) AS vol
        FROM marketdata.rm_positions
        WHERE book_id = :b AND start_date >= :m
          AND start_date < (:m::date + INTERVAL '1 month')::date
    """), conn, params={"b": book_id, "m": month}).iloc[0]
    return (float(row["cost"]) if row["cost"] is not None else 0.0,
            float(row["vol"]) if row["vol"] is not None else 0.0)


def _rt_vwap(conn, province: str, month: datetime.date) -> float | None:
    df = pd.read_sql(text("""
        SELECT rt_price FROM marketdata.spot_prices_hourly
        WHERE province = :p AND datetime >= :m
          AND datetime < (:m::date + INTERVAL '1 month')::date
    """), conn, params={"p": schemas.spot_province(province), "m": month})
    return spot_vwap_series(df) if not df.empty else None


def reconcile_month(conn, book_id: int, month: datetime.date,
                    tolerance_pct: float = 0.005,
                    tolerance_floor: float = 5000.0) -> ReconResult:
    province = conn.execute(text(
        "SELECT province FROM marketdata.rm_positions WHERE book_id = :b LIMIT 1"
    ), {"b": book_id}).scalar() or ""
    by_cat = _invoice_by_category(conn, book_id, month)
    totals = _invoice_totals(conn, book_id, month)
    pos_cost, pos_vol = _positions_cost(conn, book_id, month)

    inv_midlong = by_cat.get("midlong_energy", 0.0)
    inv_spot = by_cat.get("spot_energy", 0.0)
    inv_fees = sum(by_cat.get(c, 0.0) for c in FEE_CATEGORIES)
    inv_total = float(totals["total"]) if totals["total"] is not None else None

    lines: list[ReconLine] = []
    lines.append(ReconLine("midlong", pos_cost, inv_midlong, pos_cost - inv_midlong,
                           f"trades vol {pos_vol:,.0f} MWh"))
    rt = _rt_vwap(conn, province, month) if province else None
    exp_spot = None
    if rt is not None and totals["settled_vol"] is not None and pos_vol:
        exposure = float(totals["settled_vol"]) - pos_vol
        exp_spot = expected_spot_cost(exposure, rt)
    lines.append(ReconLine("spot", exp_spot, inv_spot,
                           (exp_spot - inv_spot) if exp_spot is not None else None,
                           f"RT VWAP {rt:.1f}" if rt else "no spot data"))
    if inv_total is not None:
        exp_total = pos_cost + (exp_spot or 0.0) + inv_fees
        lines.append(ReconLine("total", exp_total, inv_total, exp_total - inv_total, ""))
        residual = inv_total - inv_midlong - inv_spot
        lines.append(ReconLine("residual", inv_fees, residual, inv_fees - residual, "fees"))
        tol = tolerance_for(inv_total, tolerance_pct, tolerance_floor)
    else:
        tol = tolerance_floor
    return ReconResult(book_id=book_id, month=month,
                       status=judge(lines, tol), lines=lines, tolerance_cny=tol)
