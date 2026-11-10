# services/retail_risk/reconcile.py
"""Goal 1: reconcile trades with settlement invoices, per book x month.

Checks:
  1. midlong:  Σ rm_positions cost (month)  vs  Σ items[midlong_energy]
  2. spot:     exposure x RT VWAP           vs  Σ items[spot_energy]
               exposure = invoice settled volume - cleared midlong volume
  3. residual: invoice total - (midlong+spot) vs itemised fees (imbalance/penalty/
               surcharges/redistribution/rule_charges/other/green_premium)
Status: matched | explained | flagged, or a coverage status when an input is
missing (no_invoice | no_trades | no_prices) — grey, not red: absent data is a
coverage gap, not a disagreement.
  matched   = midlong and total within tolerance
  explained = midlong within tolerance; spot/total differ (the spot line is an
              estimate — exposure x mean RT — not a load-shape replay)
  flagged   = trades disagree with the invoice's midlong line

Midlong settlement mode (MIDLONG_MODE): 'physical' provinces (冀南, 安徽) bill
contracts at contract price and spot covers only the deviation; 'cfd' provinces
(浙江, 山东) bill the whole load at spot and 中长期 as a difference vs the DA price,
so expected midlong = Σ_h net_vol x (contract price - DA price).

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

MIDLONG_MODE = {"浙江": "cfd", "山东": "cfd"}   # default 'physical'

FEE_CATEGORIES = ["imbalance", "penalty", "govt_surcharges", "market_redistribution",
                  "rule_charges", "other", "green_premium", "frequency"]


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


def _ok(l: ReconLine | None, tol: float) -> bool:
    return l is not None and l.delta_cny is not None and abs(l.delta_cny) <= tol


def judge(lines: list[ReconLine], tol: float) -> str:
    by = {l.name: l for l in lines}
    if not _ok(by.get("midlong"), tol):
        return "flagged"
    if _ok(by.get("total"), tol):
        return "matched"
    return "explained"


def coverage_status(has_invoice: bool, has_trades: bool, has_prices: bool) -> str | None:
    if not has_invoice:
        return "no_invoice"
    if not has_trades:
        return "no_trades"
    if not has_prices:
        return "no_prices"
    return None


def expected_cfd_cost(hourly: pd.DataFrame, da: pd.DataFrame) -> tuple[float, int]:
    """Σ net_vol x (contract price - DA price) over hours; returns (cost, hours
    with volume but no DA price — those are left out of the sum)."""
    m = hourly.merge(da, on=["delivery_date", "hour"], how="left")
    m = m[m["volume_mwh"].fillna(0) != 0]
    missing = int(m["da_price"].isna().sum())
    m = m.dropna(subset=["da_price", "price_cny_mwh"])
    return float((m["volume_mwh"] * (m["price_cny_mwh"] - m["da_price"])).sum()), missing


def invoice_by_category_frame(items: pd.DataFrame) -> dict[str, float]:
    """Per-category amounts from hierarchical subject lines.

    For each category, take lines whose subject code (notes) has the MINIMUM
    length within that category (top line of the subtree), ignoring longer
    sub-lines. The 2-digit header lines are excluded from category sums.
    Cost categories (midlong/spot/fees) use BUY-side （购电侧） lines only;
    'retail_revenue' comes from SELL-side （售电侧） 01-tree lines.
    CRITICAL: when a child's category differs from its ancestors' (e.g. 0202 ->
    market_redistribution but 0202030006 -> imbalance), each ancestor carrying
    the child's amount is REDUCED by it — otherwise the fee double counts.
    """
    if items.empty:
        return {}
    df = items.copy()
    df["_code"] = df["notes"].fillna("").str.extract(r"^(\d+)")[0]
    df["_clen"] = df["_code"].str.len().fillna(99)
    if "counterparty" in df.columns:
        is_sell = df["counterparty"].eq("售电侧")
    elif "side" in df.columns:
        is_sell = df["side"].eq("sell")
    else:
        is_sell = pd.Series(False, index=df.index)
    out: dict[str, float] = {}
    for cat, g in df.groupby("category"):
        if cat == "retail_revenue":
            g2 = g[is_sell[g.index]] if is_sell.any() else g.iloc[0:0]
            # sell-side '01' (len 2) IS the revenue tree top — no clen floor here
        else:
            g2 = g[~is_sell[g.index]] if is_sell.any() else g
            g2 = g2[g2["_clen"] >= 4]     # 2-digit headers excluded from cost sums
        if g2.empty:
            continue
        top = g2[g2["_clen"] == g2["_clen"].min()]
        out[cat] = float(top["amount_cny"].sum())

    # descendant-subtraction, restricted to codes that actually entered the sums:
    # a summed child whose category differs from a summed ancestor's subtracts its
    # amount from that ancestor's category (0202 includes 0202030006 otherwise).
    summed: dict[str, str] = {}
    for cat, g in df.groupby("category"):
        if cat == "retail_revenue":
            continue
        g2 = g[~is_sell[g.index]] if is_sell.any() else g
        g2 = g2[g2["_clen"] >= 4]
        if g2.empty:
            continue
        for c in g2[g2["_clen"] == g2["_clen"].min()]["_code"]:
            if isinstance(c, str):
                summed[c] = cat
    amt_by_code = dict(zip(df["_code"], df["amount_cny"]))
    for code, cat in summed.items():
        for plen in range(len(code) - 2, 3, -2):     # ancestors with clen >= 4 only
            anc = code[:plen]
            if anc in summed and summed[anc] != cat:
                out[summed[anc]] -= float(amt_by_code[code])
                break
    if is_sell.any():
        # sell-side fee-tree top lines ('02', '03' — len 2, not '01'): they adjust
        # what the retailer actually collects (山东: 偏差考核, 封顶结算差额, 结算调整)
        fee_top = df[is_sell & df["_clen"].eq(2) & ~df["_code"].eq("01")]
        if not fee_top.empty:
            out["retail_fee_adjustments"] = float(fee_top["amount_cny"].sum())
    return out


def _invoice_by_category(conn, book_id: int, month: datetime.date) -> dict[str, float]:
    df = pd.read_sql(text("""
        SELECT si.category, si.amount_cny, si.notes, si.counterparty
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
    row = pd.read_sql(text("""
        SELECT SUM(total_amount_cny) AS total,
               MAX(CAST(raw_data->>'settled_volume_mwh' AS double precision)) AS doc_vol
        FROM marketdata.rm_settlements
        WHERE book_id = :b AND settlement_month = :m
          AND COALESCE(raw_data->>'total_kind', 'margin') = 'margin'
    """), conn, params={"b": book_id, "m": month}).iloc[0]
    total = row["total"]
    items = pd.read_sql(text("""
        SELECT si.volume_mwh, si.category, si.notes, si.counterparty
        FROM marketdata.rm_settlement_items si
        JOIN marketdata.rm_settlements s ON s.id = si.settlement_id
        WHERE s.book_id = :b AND s.settlement_month = :m
    """), conn, params={"b": book_id, "m": month})
    doc_vol = float(row["doc_vol"]) if pd.notna(row["doc_vol"]) else None
    return {"total": float(total) if pd.notna(total) else None,
            "settled_vol": doc_vol if doc_vol is not None else settle_volume(items)}


def settle_volume(items: pd.DataFrame) -> float | None:
    if items.empty:
        return None
    if "counterparty" in items.columns:
        buy = items[items["counterparty"] != "售电侧"]
        if buy.empty:
            buy = items
    else:
        buy = items
    codes = buy["notes"].fillna("").str.extract(r"^(\d+)")[0]
    if codes.notna().any():
        top = buy.loc[codes[codes.notna()].str.len().idxmin()]
        if pd.notna(top["volume_mwh"]):
            return float(top["volume_mwh"])
    spot = buy[buy["category"] == "spot_energy"]["volume_mwh"].dropna()
    return float(spot.sum()) if not spot.empty else None


def _next_month(month: datetime.date) -> datetime.date:
    return datetime.date(month.year + (month.month == 12), month.month % 12 + 1, 1)


def _positions_cost(conn, book_id: int, month: datetime.date) -> tuple[float, float]:
    """Direction-signed: sell-backs （日滚动卖出, 合同转让） NET against buys —
    the invoice's midlong top line is net of them (review I1)."""
    row = pd.read_sql(text("""
        SELECT SUM(CASE WHEN direction = 'sell' THEN -1 ELSE 1 END * volume_mwh * price_cny_mwh) AS cost,
               SUM(CASE WHEN direction = 'sell' THEN -1 ELSE 1 END * volume_mwh) AS vol
        FROM marketdata.rm_positions
        WHERE book_id = :b AND start_date >= :m AND start_date < :m_next
    """), conn, params={"b": book_id, "m": month, "m_next": _next_month(month)}).iloc[0]
    return (float(row["cost"]) if row["cost"] is not None else 0.0,
            float(row["vol"]) if row["vol"] is not None else 0.0)


def _rt_vwap(conn, province: str, month: datetime.date) -> float | None:
    df = pd.read_sql(text("""
        SELECT rt_price FROM marketdata.spot_prices_hourly
        WHERE province = :p AND datetime >= :m AND datetime < :m_next
    """), conn, params={"p": schemas.spot_province(province), "m": month,
                        "m_next": _next_month(month)})
    return spot_vwap_series(df) if not df.empty else None


_VOL_CHANNELS = ("annual", "monthly_auction", "monthly_listed", "intramonth_match")


def _hourly_positions(conn, book_id: int, month: datetime.date) -> pd.DataFrame:
    """Net hourly volume + VWAP per channel from rm_position_volumes (long form)."""
    parts = " UNION ALL ".join(
        f"SELECT delivery_date, hour, {c}_volume_mwh AS volume_mwh, "
        f"{c}_price_cny_mwh AS price_cny_mwh FROM marketdata.rm_position_volumes "
        f"WHERE book_id = :b AND delivery_date >= :m AND delivery_date < :m_next"
        for c in _VOL_CHANNELS)
    df = pd.read_sql(text(parts), conn, params={"b": book_id, "m": month,
                                                "m_next": _next_month(month)})
    for c in ("volume_mwh", "price_cny_mwh"):
        df[c] = pd.to_numeric(df[c], errors="coerce")
    return df.dropna(subset=["volume_mwh"])


def _da_prices(conn, province: str, month: datetime.date) -> pd.DataFrame:
    """Hour-beginning DA prices: datetime 00:00 -> hour 0."""
    df = pd.read_sql(text("""
        SELECT CAST(datetime AS date) AS delivery_date,
               CAST(EXTRACT(hour FROM datetime) AS int) AS hour, da_price
        FROM marketdata.spot_prices_hourly
        WHERE province = :p AND datetime >= :m AND datetime < :m_next
          AND da_price IS NOT NULL
    """), conn, params={"p": schemas.spot_province(province), "m": month,
                        "m_next": _next_month(month)})
    df["da_price"] = pd.to_numeric(df["da_price"], errors="coerce")
    return df


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
    mode = MIDLONG_MODE.get(province, "physical")
    has_prices = True

    lines: list[ReconLine] = []
    if mode == "cfd":
        da = _da_prices(conn, province, month) if province else pd.DataFrame()
        has_prices = not da.empty
        cfd, missing = (expected_cfd_cost(_hourly_positions(conn, book_id, month), da)
                        if has_prices else (None, 0))
        note = f"CfD vs DA; trades vol {pos_vol:,.0f} MWh"
        if missing:
            note += f"; {missing} hours without DA price excluded"
        lines.append(ReconLine("midlong", cfd, inv_midlong,
                               (cfd - inv_midlong) if cfd is not None else None, note))
        exp_mid = cfd or 0.0
    else:
        lines.append(ReconLine("midlong", pos_cost, inv_midlong, pos_cost - inv_midlong,
                               f"trades vol {pos_vol:,.0f} MWh"))
        exp_mid = pos_cost
    rt = _rt_vwap(conn, province, month) if province else None
    exp_spot = None
    if rt is not None and totals["settled_vol"] is not None and pos_vol:
        # cfd: the whole load settles at spot; physical: only the deviation
        exposure = float(totals["settled_vol"]) - (0.0 if mode == "cfd" else pos_vol)
        exp_spot = expected_spot_cost(exposure, rt)
    lines.append(ReconLine("spot", exp_spot, inv_spot,
                           (exp_spot - inv_spot) if exp_spot is not None else None,
                           f"RT VWAP {rt:.1f}" if rt else "no spot data"))
    if inv_total is not None:
        exp_total = exp_mid + (exp_spot or 0.0) + inv_fees
        lines.append(ReconLine("total", exp_total, inv_total, exp_total - inv_total, ""))
        residual = inv_total - inv_midlong - inv_spot
        lines.append(ReconLine("residual", inv_fees, residual, inv_fees - residual, "fees"))
        tol = tolerance_for(inv_total, tolerance_pct, tolerance_floor)
    else:
        tol = tolerance_floor
    has_invoice = inv_total is not None or bool(by_cat)
    status = (coverage_status(has_invoice, pos_vol != 0, has_prices)
              or judge(lines, tol))
    return ReconResult(book_id=book_id, month=month,
                       status=status, lines=lines, tolerance_cny=tol)
