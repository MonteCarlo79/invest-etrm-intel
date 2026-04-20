"""
services/ops_ingestion/inner_mongolia/price_verifier.py

Consistency check: compare Excel 节点电价 (nodal price) against
md_id_cleared_energy.cleared_price (intraday session clearing price).

These are DIFFERENT price concepts in the Mengxi electricity market:
  - Excel 节点电价  = real-time locational marginal price at the BESS node
  - cleared_price   = unit-specific intraday session clearing price

A low MAE indicates the two prices are numerically close for this asset/date;
a high MAE (or low r) indicates they diverge — which is expected when the
intraday session clears at a different price than the real-time nodal price.

For a strict nodal-price verification, use md_rt_nodal_price.node_price joined
by node_name (asset→node mapping not yet implemented).

Consistency levels (stored in price_verification_level)
---------------------------------------------------------
  'high'       MAE < PRICE_VERIFY_HIGH_MAE  AND  n >= PRICE_VERIFY_MIN_N
  'medium'     MAE < PRICE_VERIFY_MEDIUM_MAE AND  n >= PRICE_VERIFY_MIN_N
  'low'        n >= PRICE_VERIFY_MIN_N but MAE >= PRICE_VERIFY_MEDIUM_MAE
  'unverified' n < PRICE_VERIFY_MIN_N  (too few matched rows to draw conclusions)

Result fields stored in ops_dispatch_asset_sheet_map:
  price_match_n            INTEGER
  price_match_mae          NUMERIC(10,3)   mean |excel_nodal - id_cleared| CNY/MWh
  price_match_r            NUMERIC(6,4)    Pearson r
  price_verification_level TEXT            consistency level (high/medium/low/unverified)
  price_verification_notes TEXT
"""
from __future__ import annotations

import math
from dataclasses import dataclass
from typing import List, Optional

# ---------------------------------------------------------------------------
# Configurable thresholds (named constants — easy to tune)
# ---------------------------------------------------------------------------

PRICE_VERIFY_HIGH_MAE: float = 5.0     # CNY/MWh — "high" if MAE below this
PRICE_VERIFY_MEDIUM_MAE: float = 20.0  # CNY/MWh — "medium" if below this
PRICE_VERIFY_MIN_N: int = 80           # minimum matched intervals for a verdict


# ---------------------------------------------------------------------------
# Result dataclass
# ---------------------------------------------------------------------------

@dataclass
class PriceVerificationResult:
    price_match_n: int
    price_match_mae: Optional[float]     # None when n=0
    price_match_r: Optional[float]       # None when n<2 or no variance
    price_verification_level: str        # 'high' | 'medium' | 'low' | 'unverified'
    price_verification_notes: str


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def verify_prices(
    excel_rows: List[dict],           # list of ParsedRow-like dicts with interval_start + nodal_price_excel
    dispatch_unit_name: str,
    data_date: str,                   # ISO date e.g. "2026-02-10"
    engine,
) -> PriceVerificationResult:
    """
    Compare Excel nodal prices to md_id_cleared_energy.cleared_price.

    Parameters
    ----------
    excel_rows : list of dict
        Each dict must have 'interval_start' (ISO TIMESTAMPTZ str) and
        'nodal_price_excel' (float | None).
    dispatch_unit_name : str
        CN dispatch unit name used in md_id_cleared_energy.
    data_date : str
        ISO date string (e.g. "2026-02-10") — used to filter the DB query.
    engine : sqlalchemy.Engine
        Connected engine for the marketdata schema.

    Returns
    -------
    PriceVerificationResult
    """
    # Build lookup: interval_start → excel_price  (skip None prices)
    excel_lookup: dict[str, float] = {}
    for row in excel_rows:
        price = row.get('nodal_price_excel')
        ts = row.get('interval_start')
        if ts is not None and price is not None:
            excel_lookup[ts] = float(price)

    if not excel_lookup:
        return _unverified("No Excel nodal prices available for comparison")

    # Fetch DB cleared prices for this dispatch unit and date
    db_lookup = _fetch_cleared_prices(dispatch_unit_name, data_date, engine)

    if not db_lookup:
        return _unverified(
            f"No rows found in md_id_cleared_energy for dispatch_unit={dispatch_unit_name!r}, "
            f"data_date={data_date}"
        )

    # Match on interval_start
    excel_vals: List[float] = []
    db_vals: List[float] = []
    for ts, excel_price in excel_lookup.items():
        db_price = db_lookup.get(ts)
        if db_price is not None:
            excel_vals.append(excel_price)
            db_vals.append(db_price)

    n = len(excel_vals)
    if n == 0:
        return _unverified(
            f"0 intervals matched between Excel and md_id_cleared_energy "
            f"(dispatch_unit={dispatch_unit_name!r}, date={data_date}). "
            "Timestamps may not align — check time offset."
        )

    mae = _mean_absolute_error(excel_vals, db_vals)
    r = _pearson_r(excel_vals, db_vals)

    level = _compute_level(n, mae)
    notes = _build_notes(n, len(excel_lookup), mae, r, level)

    return PriceVerificationResult(
        price_match_n=n,
        price_match_mae=round(mae, 3),
        price_match_r=round(r, 4) if r is not None else None,
        price_verification_level=level,
        price_verification_notes=notes,
    )


def verify_prices_no_db(
    excel_rows: List[dict],
) -> PriceVerificationResult:
    """
    Return an 'unverified' result without querying the DB.
    Used in dry-run mode or when --verify-prices is not passed.
    """
    return _unverified("Price verification not requested (--verify-prices not set)")


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _fetch_cleared_prices(dispatch_unit_name: str, data_date: str, engine) -> dict[str, float]:
    """
    Fetch interval_start → cleared_price from md_id_cleared_energy.

    Time rule: the load_excel_to_marketdata ingestion pipeline applies shift_minutes=-15
    when loading this table, so md_id_cleared_energy.datetime already stores interval_START
    (e.g. "00:00" for the first 15-min period, not "00:15").
    No further shift is needed here.

    Price concept note: cleared_price is the intraday session clearing price for the
    dispatch unit — NOT the real-time nodal price.  Excel col E (节点电价) is the nodal
    price.  These are different market-price concepts; use this comparison as a
    consistency check, not a strict verification.  For a closer comparison, use
    md_rt_nodal_price.node_price joined by node_name once the asset→node mapping is known.
    """
    from sqlalchemy import text
    sql = text("""
        SELECT
            datetime AT TIME ZONE 'Asia/Shanghai' AS interval_start,
            cleared_price
        FROM marketdata.md_id_cleared_energy
        WHERE dispatch_unit_name = :unit
          AND data_date = :data_date
          AND cleared_price IS NOT NULL
    """)
    try:
        with engine.connect() as conn:
            result = conn.execute(sql, {"unit": dispatch_unit_name, "data_date": data_date})
            rows = result.fetchall()
    except Exception:
        return {}

    lookup: dict[str, float] = {}
    for row in rows:
        if row.interval_start is not None and row.cleared_price is not None:
            # Normalise to ISO 8601 with +08:00 offset (same as parser output)
            ts_str = _normalise_ts(row.interval_start)
            lookup[ts_str] = float(row.cleared_price)
    return lookup


def _normalise_ts(ts) -> str:
    """Convert a datetime-like to ISO 8601 string with +08:00 offset."""
    import datetime
    _CST = datetime.timezone(datetime.timedelta(hours=8))
    if hasattr(ts, 'astimezone'):
        return ts.astimezone(_CST).isoformat()
    return str(ts)


def _mean_absolute_error(a: List[float], b: List[float]) -> float:
    return sum(abs(x - y) for x, y in zip(a, b)) / len(a)


def _pearson_r(a: List[float], b: List[float]) -> Optional[float]:
    """Pearson correlation; returns None if variance is zero or n < 2."""
    n = len(a)
    if n < 2:
        return None
    mean_a = sum(a) / n
    mean_b = sum(b) / n
    num = sum((x - mean_a) * (y - mean_b) for x, y in zip(a, b))
    denom_a = math.sqrt(sum((x - mean_a) ** 2 for x in a))
    denom_b = math.sqrt(sum((y - mean_b) ** 2 for y in b))
    if denom_a == 0 or denom_b == 0:
        return None
    r = num / (denom_a * denom_b)
    # Clamp to [-1, 1] to handle floating-point rounding
    return max(-1.0, min(1.0, r))


def _compute_level(n: int, mae: float) -> str:
    if n < PRICE_VERIFY_MIN_N:
        return 'unverified'
    if mae < PRICE_VERIFY_HIGH_MAE:
        return 'high'
    if mae < PRICE_VERIFY_MEDIUM_MAE:
        return 'medium'
    return 'low'


def _build_notes(n: int, total: int, mae: float, r: Optional[float], level: str) -> str:
    r_str = f", r={r:.4f}" if r is not None else ""
    return (
        f"{n}/{total} intervals matched (excel nodal_price vs id cleared_price), "
        f"MAE={mae:.1f} CNY/MWh{r_str}, consistency_level={level!r}"
    )


def _unverified(reason: str) -> PriceVerificationResult:
    return PriceVerificationResult(
        price_match_n=0,
        price_match_mae=None,
        price_match_r=None,
        price_verification_level='unverified',
        price_verification_notes=reason,
    )
