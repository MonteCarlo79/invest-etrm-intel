# services/retail_risk/pnl_bridge.py
"""Goal 2: P&L bridge, channel alpha vs spot, trader/sales attribution (spec §7.2, D10)."""
from __future__ import annotations

import datetime
from dataclasses import dataclass, field

import pandas as pd
from sqlalchemy import text

from services.retail_risk import schemas
from services.retail_risk.reconcile import _invoice_by_category

MIDLONG_CHANNELS = ["annual", "monthly_auction", "monthly_listed", "intramonth_match"]


@dataclass
class BridgeResult:
    retail_revenue: float = 0.0
    channel_costs: dict[str, float] = field(default_factory=dict)
    spot_cost: float = 0.0
    deviation: float = 0.0
    other: float = 0.0
    net: float = 0.0                       # == printed 售电公司收益 (identity, cross-check)
    retail_avg_price: float | None = None
    wholesale_avg_cost: float | None = None
    spread: float | None = None
    coverage_pct: float | None = None
    settled_vol_mwh: float | None = None


def alpha_rows(positions_pv: pd.DataFrame, spot_vwap: float) -> pd.DataFrame:
    """positions_pv: (channel, volume_mwh, pv=Σvol*price). alpha = (spot - vwap) x vol."""
    df = positions_pv.copy()
    df["vwap"] = df["pv"] / df["volume_mwh"].replace(0, float("nan"))
    df["spot_vwap"] = spot_vwap
    df["alpha_cny_mwh"] = spot_vwap - df["vwap"]
    df["alpha_cny"] = df["alpha_cny_mwh"] * df["volume_mwh"]
    return df[["channel", "volume_mwh", "vwap", "spot_vwap", "alpha_cny_mwh", "alpha_cny"]]


def trader_alpha(our_df: pd.DataFrame, bench_df: pd.DataFrame) -> pd.DataFrame:
    """our_df: (channel, volume_mwh, vwap). bench_df: rm_market_benchmarks rows.
    Join via schemas.BENCH_CHANNEL_MAP (benchmark channel covers several of ours).
    Positive = bought below market = trader gain."""
    rows = []
    for r in our_df.itertuples(index=False):
        mkt = None
        for b in bench_df.itertuples(index=False):
            if r.channel in schemas.BENCH_CHANNEL_MAP.get(b.channel, []):
                mkt = b.avg_price_cny_mwh
                break
        rows.append({"channel": r.channel, "volume_mwh": r.volume_mwh, "vwap": r.vwap,
                     "mkt_avg": mkt,
                     "trader_alpha_cny": (mkt - r.vwap) * r.volume_mwh
                     if mkt is not None and pd.notna(r.vwap) else None})
    return pd.DataFrame(rows)


def sales_alpha(retail_revenue_cny: float, channel_fee_cny: float,
                blended_benchmark: float, retail_vol_mwh: float) -> float:
    return retail_revenue_cny - channel_fee_cny - blended_benchmark * retail_vol_mwh


def channel_fee(contracts: pd.DataFrame) -> float:
    """渠道费用 = Σ contract revenue x (1 - 渠道分成比例); share missing -> 0 (D10)."""
    fee = 0.0
    for r in contracts.itertuples(index=False):
        if pd.notna(getattr(r, "share_ratio", None)) and pd.notna(getattr(r, "price_cny_mwh", None)) \
                and pd.notna(getattr(r, "annual_mwh", None)):
            fee += r.annual_mwh * r.price_cny_mwh * (1.0 - r.share_ratio)
    return fee


def bridge_month(conn, book_id: int, month: datetime.date) -> BridgeResult:
    """P&L bridge per book x month.

    P1 invoices are 购电侧-only: retail revenue is NOT an invoice line. Per spec D7
    fallback, retail_revenue = Σ wholesale cost categories + printed 售电公司收益 —
    which makes `net == 收益` an exact identity and a built-in cross-check against
    the invoice's printed margin (复盘 批零价差 derives from the same numbers)."""
    from services.retail_risk.reconcile import _invoice_totals
    by_cat = _invoice_by_category(conn, book_id, month)
    totals = _invoice_totals(conn, book_id, month)
    res = BridgeResult()
    res.spot_cost = by_cat.get("spot_energy", 0.0)
    res.deviation = by_cat.get("imbalance", 0.0) + by_cat.get("penalty", 0.0)
    res.other = sum(by_cat.get(c, 0.0) for c in
                    ["govt_surcharges", "market_redistribution", "rule_charges", "other"])
    midlong = by_cat.get("midlong_energy", 0.0)
    res.channel_costs = {"midlong": midlong, "green_premium": by_cat.get("green_premium", 0.0)}
    total_costs = midlong + res.spot_cost + res.deviation + res.other \
        + res.channel_costs["green_premium"]
    margin = float(totals["total"]) if totals["total"] is not None else 0.0
    res.retail_revenue = total_costs + margin
    res.net = margin
    vols = totals["settled_vol"]
    if vols:
        res.wholesale_avg_cost = total_costs / float(vols)
        res.retail_avg_price = res.retail_revenue / float(vols)
        res.spread = res.retail_avg_price - res.wholesale_avg_cost
    res.settled_vol_mwh = vols
    return res


def channel_alpha(conn, book_id: int, month: datetime.date) -> pd.DataFrame:
    pos = pd.read_sql(text("""
        SELECT channel, SUM(volume_mwh) AS volume_mwh,
               SUM(volume_mwh * price_cny_mwh) AS pv
        FROM marketdata.rm_positions
        WHERE book_id = :b AND start_date >= :m
          AND start_date < (:m::date + INTERVAL '1 month')::date
          AND direction = 'buy'
        GROUP BY channel
    """), conn, params={"b": book_id, "m": month})
    if pos.empty:
        return pd.DataFrame(columns=["channel", "volume_mwh", "vwap", "spot_vwap",
                                     "alpha_cny_mwh", "alpha_cny"])
    province = conn.execute(text(
        "SELECT province FROM marketdata.rm_positions WHERE book_id = :b LIMIT 1"
    ), {"b": book_id}).scalar()
    from services.retail_risk.reconcile import _rt_vwap
    rt = _rt_vwap(conn, province, month)
    if rt is None:
        return pd.DataFrame(columns=["channel", "volume_mwh", "vwap", "spot_vwap",
                                     "alpha_cny_mwh", "alpha_cny"])
    return alpha_rows(pos, rt)


def attribution_month(conn, book_id: int, month: datetime.date) -> dict:
    """Trader alpha per channel (vs 全网 benchmark) + sales alpha + identity residual.

    sales_alpha = retail_revenue - 渠道费用 - blended_benchmark x settled_vol
    identity: trader_total + sales_alpha ≈ bridge.net - 渠道费用 (residual shown, not hidden).
    """
    alpha = channel_alpha(conn, book_id, month)
    bridge = bridge_month(conn, book_id, month)
    bench = pd.read_sql(text("""
        SELECT channel, avg_price_cny_mwh FROM marketdata.rm_market_benchmarks
        WHERE month = :m AND source = 'infohub'
    """), conn, params={"m": month})
    our = alpha.dropna(subset=["vwap"]) if not alpha.empty else alpha
    t = trader_alpha(our, bench) if not our.empty else pd.DataFrame(
        columns=["channel", "volume_mwh", "vwap", "mkt_avg", "trader_alpha_cny"])
    blended = None
    if not t.empty and t["mkt_avg"].notna().any():
        tt = t.dropna(subset=["mkt_avg"])
        blended = float((tt["mkt_avg"] * tt["volume_mwh"]).sum() / tt["volume_mwh"].sum())
    province = conn.execute(text(
        "SELECT name FROM marketdata.rm_books WHERE id = :b"
    ), {"b": book_id}).scalar().split("-", 1)[1]
    contracts = pd.read_sql(text("""
        SELECT cc.annual_forecast_mwh AS annual_mwh, cc.price_cny_mwh,
               c.revenue_share_ratio AS share_ratio
        FROM marketdata.rm_customer_contracts cc
        JOIN marketdata.rm_customers c ON c.id = cc.customer_id
        WHERE c.province = :p AND cc.contract_status = 'active'
    """), conn, params={"p": province})
    fee = channel_fee(contracts) if not contracts.empty else 0.0
    sales = None
    if blended is not None and bridge.settled_vol_mwh:
        sales = sales_alpha(bridge.retail_revenue, fee, blended, bridge.settled_vol_mwh)
    trader_total = float(t["trader_alpha_cny"].dropna().sum()) if not t.empty else None
    identity_residual = None
    if sales is not None and trader_total is not None:
        identity_residual = (trader_total + sales) - (bridge.net - fee)
    return {"trader_by_channel": t, "blended_benchmark": blended,
            "trader_alpha_total": trader_total,
            "channel_fee_cny": fee, "sales_alpha_cny": sales,
            "identity_residual_cny": identity_residual}


def persist_bridge_snapshot(conn, book_id: int, month: datetime.date,
                            bridge: BridgeResult, midlong_alpha_total: float | None) -> None:
    conn.execute(text("""
        INSERT INTO marketdata.rm_pnl_snapshots
          (book_id, snapshot_date, realized_cny, bilateral_pnl_cny, spot_pnl_cny,
           deviation_pnl_cny, other_pnl_cny)
        VALUES (:b, :d, :r, :bil, :spot, :dev, :oth)
        ON CONFLICT (book_id, snapshot_date) DO UPDATE SET
          realized_cny = EXCLUDED.realized_cny,
          bilateral_pnl_cny = EXCLUDED.bilateral_pnl_cny,
          spot_pnl_cny = EXCLUDED.spot_pnl_cny,
          deviation_pnl_cny = EXCLUDED.deviation_pnl_cny,
          other_pnl_cny = EXCLUDED.other_pnl_cny
    """), {"b": book_id, "d": month, "r": bridge.net, "bil": midlong_alpha_total,
           "spot": -bridge.spot_cost, "dev": -bridge.deviation, "oth": -bridge.other})
