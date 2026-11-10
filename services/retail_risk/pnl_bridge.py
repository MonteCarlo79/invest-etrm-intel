# services/retail_risk/pnl_bridge.py
"""Goal 2: P&L bridge, channel alpha vs spot, trader/sales attribution (spec §7.2, D10)."""
from __future__ import annotations

import datetime
import json
from dataclasses import dataclass, field

import pandas as pd
from sqlalchemy import text

from services.retail_risk import schemas
from services.retail_risk.reconcile import _invoice_by_category

MIDLONG_CHANNELS = ["annual", "monthly_auction", "monthly_listed", "intramonth_match"]


@dataclass
class BridgeResult:
    retail_revenue: float | None = 0.0
    channel_costs: dict[str, float] = field(default_factory=dict)
    spot_cost: float = 0.0
    deviation: float = 0.0
    other: float = 0.0
    net: float | None = 0.0                 # sell-side revenue - costs (independent);
                                            # == printed 售电公司收益 when consistent
    retail_avg_price: float | None = None
    wholesale_avg_cost: float | None = None
    spread: float | None = None
    coverage_pct: float | None = None
    settled_vol_mwh: float | None = None
    printed_margin_cny: float | None = None
    margin_check_cny: float | None = None   # net - printed margin (true cross-check)


def alpha_rows(positions_pv: pd.DataFrame, spot_vwap: float | None) -> pd.DataFrame:
    """positions_pv: (channel, volume_mwh, pv=Σvol*price). alpha = (spot - vwap) x vol.
    spot_vwap None (no RT prices) keeps our VWAP — trader alpha only needs that."""
    df = positions_pv.copy()
    for c in ("volume_mwh", "pv"):
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df["vwap"] = df["pv"] / df["volume_mwh"].replace(0, float("nan"))
    df["spot_vwap"] = float("nan") if spot_vwap is None else spot_vwap
    df["alpha_cny_mwh"] = df["spot_vwap"] - df["vwap"]
    df["alpha_cny"] = df["alpha_cny_mwh"] * df["volume_mwh"]
    return df[["channel", "volume_mwh", "vwap", "spot_vwap", "alpha_cny_mwh", "alpha_cny"]]


def trader_alpha(our_df: pd.DataFrame, bench_df: pd.DataFrame) -> pd.DataFrame:
    """our_df: (channel, volume_mwh, vwap). bench_df: rm_market_benchmarks rows.
    Join via schemas.BENCH_CHANNEL_MAP (benchmark channel covers several of ours).
    Per our channel: take the best-ranked source (BENCH_SOURCE_RANK) that has any
    matching row, and volume-weight its matching rows (月度集中竞价 + 月度双边 both
    map to monthly_auction); flat mean when volumes are missing.
    Positive = bought below market = trader gain."""
    rows = []
    b_all = bench_df.copy()
    if "source" not in b_all.columns:
        b_all["source"] = "infohub"
    if "volume_mwh" not in b_all.columns:
        b_all["volume_mwh"] = None
    b_all["_rank"] = b_all["source"].map(schemas.BENCH_SOURCE_RANK).fillna(9)
    for r in our_df.itertuples(index=False):
        mkt = None
        hit = b_all[b_all["channel"].map(
            lambda c: r.channel in schemas.BENCH_CHANNEL_MAP.get(c, []))]
        if not hit.empty:
            hit = hit[hit["_rank"] == hit["_rank"].min()]
            w = pd.to_numeric(hit["volume_mwh"], errors="coerce")
            px = hit["avg_price_cny_mwh"].astype(float)
            mkt = (float((px * w).sum() / w.sum()) if w.notna().all() and w.sum() > 0
                   else float(px.mean()))
        rows.append({"channel": r.channel, "volume_mwh": r.volume_mwh, "vwap": r.vwap,
                     "mkt_avg": mkt,
                     "trader_alpha_cny": (mkt - r.vwap) * r.volume_mwh
                     if mkt is not None and pd.notna(r.vwap) else None})
    return pd.DataFrame(rows)


def sales_alpha(retail_revenue_cny: float, channel_fee_cny: float,
                blended_benchmark: float, retail_vol_mwh: float) -> float:
    return retail_revenue_cny - channel_fee_cny - blended_benchmark * retail_vol_mwh


def _nz(v) -> float | None:
    return None if v is None or pd.isna(v) else float(v)


def channel_fee(contracts: pd.DataFrame, month: int | None = None,
                benchmark: float | None = None) -> float:
    """渠道费用 per contract = vol x (per-MWh 居间费 + share x max(margin, 0))
    + annual lump / 12. share = the CHANNEL's share of the margin;
    margin = spread_cny_mwh for 价差 packages, else price - benchmark (wholesale
    market average) for fixed-price packages. Losses are not shared.

    Volume: monthly_forecast[month] when available, else annual_mwh / 12.
    month=None returns the annual figure."""
    fee = 0.0
    for r in contracts.itertuples(index=False):
        vol = None
        mf = getattr(r, "monthly_forecast", None)
        if month is not None and isinstance(mf, str) and mf and mf != "{}":
            vol = json.loads(mf).get(str(month))
        annual = _nz(getattr(r, "annual_mwh", None))
        if vol is None:
            if annual is None:
                continue
            vol = annual if month is None else annual / 12.0
        spread = _nz(getattr(r, "spread_cny_mwh", None))
        price = _nz(getattr(r, "price_cny_mwh", None))
        margin = spread if spread is not None else (
            price - benchmark if price is not None and benchmark is not None else None)
        share = _nz(getattr(r, "share_ratio", None))
        per_mwh = _nz(getattr(r, "channel_fee_cny_mwh", None)) or 0.0
        if share is not None and margin is not None:
            per_mwh += share * max(margin, 0.0)
        fee += vol * per_mwh
        lump = _nz(getattr(r, "channel_fee_lump_cny", None))
        if lump:
            fee += lump if month is None else lump / 12.0
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
                    ["govt_surcharges", "market_redistribution", "rule_charges", "other", "frequency"])
    midlong = by_cat.get("midlong_energy", 0.0)
    res.channel_costs = {"midlong": midlong, "green_premium": by_cat.get("green_premium", 0.0)}
    total_costs = midlong + res.spot_cost + res.deviation + res.other \
        + res.channel_costs["green_premium"]
    printed_margin = float(totals["total"]) if totals["total"] is not None else None
    sell_side_revenue = by_cat.get("retail_revenue")
    if sell_side_revenue is not None:
        # invoice's own 售电侧: 01 top line + sell-side fee-tree adjustments
        # (山东: 偏差考核 + 封顶结算差额 + 结算调整) = what the retailer actually collects
        res.retail_revenue = sell_side_revenue + by_cat.get("retail_fee_adjustments", 0.0)
        res.net = res.retail_revenue - total_costs
    elif printed_margin is not None:
        # fallback (spec D7): revenue = costs + printed margin; net == margin by construction
        res.retail_revenue = total_costs + printed_margin
        res.net = printed_margin
    else:
        res.retail_revenue = None
        res.net = None
        res.settled_vol_mwh = totals["settled_vol"]
        return res
    res.printed_margin_cny = printed_margin
    res.margin_check_cny = (res.net - printed_margin) if printed_margin is not None else None
    vols = totals["settled_vol"]
    if vols:
        res.wholesale_avg_cost = total_costs / float(vols)
        res.retail_avg_price = res.retail_revenue / float(vols)
        res.spread = res.retail_avg_price - res.wholesale_avg_cost
    res.settled_vol_mwh = vols
    return res


def channel_alpha(conn, book_id: int, month: datetime.date) -> pd.DataFrame:
    from services.retail_risk.reconcile import _next_month
    pos = pd.read_sql(text("""
        SELECT channel, SUM(volume_mwh) AS volume_mwh,
               SUM(volume_mwh * price_cny_mwh) AS pv
        FROM marketdata.rm_positions
        WHERE book_id = :b AND start_date >= :m AND start_date < :m_next
          AND direction = 'buy'
        GROUP BY channel
    """), conn, params={"b": book_id, "m": month, "m_next": _next_month(month)})
    if pos.empty:
        return pd.DataFrame(columns=["channel", "volume_mwh", "vwap", "spot_vwap",
                                     "alpha_cny_mwh", "alpha_cny"])
    province = conn.execute(text(
        "SELECT province FROM marketdata.rm_positions WHERE book_id = :b LIMIT 1"
    ), {"b": book_id}).scalar()
    from services.retail_risk.reconcile import _rt_vwap
    rt = _rt_vwap(conn, province, month)
    return alpha_rows(pos, rt)


def _month_end(month: datetime.date) -> datetime.date:
    from services.retail_risk.reconcile import _next_month
    return _next_month(month) - datetime.timedelta(days=1)


def attribution_month(conn, book_id: int, month: datetime.date) -> dict:
    """Trader alpha per channel (vs 全网 benchmark) + sales alpha + identity residual.

    sales_alpha = retail_revenue - 渠道费用 - blended_benchmark x settled_vol
    identity: trader_total + sales_alpha ≈ bridge.net - 渠道费用 (residual shown, not hidden).
    """
    alpha = channel_alpha(conn, book_id, month)
    bridge = bridge_month(conn, book_id, month)
    province = conn.execute(text(
        "SELECT name FROM marketdata.rm_books WHERE id = :b"
    ), {"b": book_id}).scalar().split("-", 1)[1]
    bench = pd.read_sql(text("""
        SELECT channel, avg_price_cny_mwh, volume_mwh, source
        FROM marketdata.rm_market_benchmarks
        WHERE month = :m AND province = :p
    """), conn, params={"m": month, "p": province})
    our = alpha.dropna(subset=["vwap"]) if not alpha.empty else alpha
    t = trader_alpha(our, bench) if not our.empty else pd.DataFrame(
        columns=["channel", "volume_mwh", "vwap", "mkt_avg", "trader_alpha_cny"])
    blended = None
    if not t.empty and t["mkt_avg"].notna().any():
        tt = t.dropna(subset=["mkt_avg"])
        blended = float((tt["mkt_avg"] * tt["volume_mwh"]).sum() / tt["volume_mwh"].sum())
    contracts = pd.read_sql(text("""
        SELECT cc.annual_forecast_mwh AS annual_mwh, cc.price_cny_mwh, cc.contract_type,
               CAST(cc.monthly_forecast AS text) AS monthly_forecast,
               c.revenue_share_ratio AS share_ratio,
               CAST(cc.price_formula->>'spread_cny_mwh' AS double precision) AS spread_cny_mwh,
               CAST(cc.price_formula->>'channel_fee_cny_mwh' AS double precision) AS channel_fee_cny_mwh,
               CAST(cc.price_formula->>'channel_fee_lump_cny' AS double precision) AS channel_fee_lump_cny
        FROM marketdata.rm_customer_contracts cc
        JOIN marketdata.rm_customers c ON c.id = cc.customer_id
        WHERE c.province = :p AND cc.contract_status = 'active'
          AND cc.start_date <= :m_end AND cc.end_date >= :m
    """), conn, params={"p": province, "m": month, "m_end": _month_end(month)})
    for col in ("annual_mwh", "price_cny_mwh", "share_ratio"):
        contracts[col] = pd.to_numeric(contracts[col], errors="coerce")
    fee = (channel_fee(contracts, month=month.month, benchmark=blended)
           if not contracts.empty else 0.0)
    sales = None
    if blended is not None and bridge.settled_vol_mwh and bridge.retail_revenue is not None:
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
