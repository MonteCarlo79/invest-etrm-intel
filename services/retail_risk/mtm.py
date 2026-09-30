# services/retail_risk/mtm.py
"""Goal 3: per-book MtM = procurement MtM (open positions x scenario curve)
+ retail-contract MtM (fixed: price spread; indexed: locked service uplift)."""
from __future__ import annotations

import datetime
import json
from dataclasses import dataclass, field

import pandas as pd
from sqlalchemy import text

from libs.risk.mtm import compute_mtm
from services.retail_risk import schemas


@dataclass
class MtmResult:
    book_id: int
    scenario: str
    as_of: datetime.date
    procurement_mtm_cny: float = 0.0
    retail_mtm_cny: float = 0.0
    total_mtm_cny: float = 0.0
    positions_df: pd.DataFrame = field(default_factory=pd.DataFrame)
    contracts_df: pd.DataFrame = field(default_factory=pd.DataFrame)


def procurement_mtm(positions: list[dict], forward_prices: dict[str, float]) -> list[dict]:
    return compute_mtm(positions, forward_prices)


def remaining_volume(contract_row, as_of: datetime.date) -> float:
    """Remaining MWh from monthly_forecast JSON (months >= as_of month);
    fallback: annual x remaining_months/12."""
    mf = contract_row.get("monthly_forecast")
    if isinstance(mf, str) and mf and mf != "{}":
        months = json.loads(mf)
        vol = sum(float(v) for k, v in months.items() if int(k) >= as_of.month)
        if vol:
            return vol
    annual = contract_row.get("annual_forecast_mwh")
    if annual and pd.notna(annual):
        return float(annual) * (12 - as_of.month + 1) / 12.0
    return 0.0


def retail_contract_mtm(contracts_df: pd.DataFrame, forward_price: float,
                        as_of: datetime.date) -> pd.DataFrame:
    rows = []
    for r in contracts_df.itertuples(index=False):
        d = r._asdict() if hasattr(r, "_asdict") else dict(r)
        vol = remaining_volume(d, as_of)
        ctype = d.get("contract_type")
        price = d.get("price_cny_mwh") or 0.0
        if ctype in ("fixed", "peak_offpeak"):
            mtm = (price - forward_price) * vol
        else:                                   # indexed / indexed_band: service uplift
            mtm = price * vol
        rows.append({**d, "remaining_mwh": vol, "forward_price": forward_price,
                     "mtm_cny": mtm})
    return pd.DataFrame(rows)


def book_mtm(conn, book_id: int, scenario: str = "spot_base",
             as_of: datetime.date | None = None) -> MtmResult:
    as_of = as_of or datetime.date.today()
    pos_df = pd.read_sql(text("""
        SELECT direction, volume_mwh, price_cny_mwh, province, start_date, end_date, channel
        FROM marketdata.rm_positions WHERE book_id = :b AND status = 'open'
    """), conn, params={"b": book_id})
    fwd = pd.read_sql(text("""
        SELECT DISTINCT ON (province) province, price_cny_kwh * 1000 AS price
        FROM marketdata.rm_forward_curves
        WHERE product = :sc AND delivery_date >= :d
        ORDER BY province, curve_date DESC, delivery_date
    """), conn, params={"sc": scenario, "d": as_of})
    forward_prices = dict(zip(fwd["province"], fwd["price"]))
    proc = procurement_mtm(pos_df.to_dict("records"), forward_prices) if not pos_df.empty else []
    proc_total = sum(p["unrealized_pnl_cny"] for p in proc)

    province = pos_df["province"].iloc[0] if not pos_df.empty else None
    contracts = pd.read_sql(text("""
        SELECT cc.contract_type, cc.price_cny_mwh, cc.monthly_forecast, cc.annual_forecast_mwh
        FROM marketdata.rm_customer_contracts cc
        JOIN marketdata.rm_customers c ON c.id = cc.customer_id
        WHERE c.province = :p AND cc.contract_status = 'active'
          AND cc.end_date >= :d
    """), conn, params={"p": province or "", "d": as_of})
    ctr_df = pd.DataFrame()
    retail_total = 0.0
    if not contracts.empty and province in forward_prices:
        ctr_df = retail_contract_mtm(contracts, forward_prices[province], as_of)
        retail_total = float(ctr_df["mtm_cny"].sum())
    return MtmResult(book_id=book_id, scenario=scenario, as_of=as_of,
                     procurement_mtm_cny=proc_total, retail_mtm_cny=retail_total,
                     total_mtm_cny=proc_total + retail_total,
                     positions_df=pd.DataFrame(proc), contracts_df=ctr_df)


def persist_mtm(conn, result: MtmResult) -> None:
    conn.execute(text("""
        INSERT INTO marketdata.rm_pnl_snapshots (book_id, snapshot_date, unrealized_mtm_cny)
        VALUES (:b, :d, :u)
        ON CONFLICT (book_id, snapshot_date) DO UPDATE SET
          unrealized_mtm_cny = EXCLUDED.unrealized_mtm_cny
    """), {"b": result.book_id, "d": result.as_of, "u": result.total_mtm_cny})
