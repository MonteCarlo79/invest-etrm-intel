"""
services/monitoring/realization_monitor.py

Realization status computation service for BESS assets.

Reads rolling attribution data from reports.bess_asset_daily_attribution,
computes realization ratio and status level, writes to
monitoring.asset_realization_status.

Status levels
-------------
NORMAL        realization_ratio >= 0.70
WARN          realization_ratio in [0.50, 0.70)
ALERT         realization_ratio in [0.30, 0.50)
CRITICAL      realization_ratio < 0.30  (data present, ratio computable)
DATA_ABSENT   days_in_window < MIN_DAYS_FOR_RATIO  (not enough data to assess)
INDETERMINATE pf_grid_feasible_pnl <= 0  (benchmark unavailable — ratio undefined)

Patch history
-------------
B1 (2026-04 hardening): separate DATA_ABSENT from CRITICAL
B2 (2026-04 hardening): add INDETERMINATE for non-positive pf_grid_feasible
B5 (2026-04 hardening): structured MONITORING_ALERT log events
"""
from __future__ import annotations

import logging
from datetime import date, timedelta
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

_LOOKBACK_DAYS_DEFAULT = 30

# Minimum days of attribution data required before computing a meaningful ratio.
# Fewer than this → DATA_ABSENT (not enough sample for a reliable 30d average).
_MIN_DAYS_FOR_RATIO = 5

_THRESHOLDS = [
    (0.70, "NORMAL"),
    (0.50, "WARN"),
    (0.30, "ALERT"),
]


def _classify_status(
    ratio: Optional[float],
    days_in_window: int,
    pf_grid_positive: bool = True,
) -> str:
    """
    Classify realization status.

    Priority order:
      1. DATA_ABSENT   — not enough data rows to assess (days < _MIN_DAYS_FOR_RATIO)
      2. INDETERMINATE — pf_grid_feasible is non-positive; ratio is undefined
      3. CRITICAL/ALERT/WARN/NORMAL — ratio-based thresholds

    B1: days_in_window == 0 now returns DATA_ABSENT (was CRITICAL).
    B1: days_in_window in [1, _MIN_DAYS_FOR_RATIO) returns DATA_ABSENT.
    B2: pf_grid_positive=False returns INDETERMINATE (was CRITICAL via ratio=None).
    """
    if days_in_window < _MIN_DAYS_FOR_RATIO:
        return "DATA_ABSENT"
    if not pf_grid_positive or ratio is None:
        return "INDETERMINATE"
    for threshold, level in _THRESHOLDS:
        if ratio >= threshold:
            return level
    return "CRITICAL"


def _build_narrative(
    asset_code: str,
    ratio: Optional[float],
    status: str,
    days: int,
    dominant_bucket: Optional[str],
    avg_actual: Optional[float],
    avg_pf_grid: Optional[float],
) -> str:
    # B1: DATA_ABSENT narratives — explain the data gap, not the performance
    if days == 0:
        return (
            f"{asset_code}: STATUS=DATA_ABSENT. "
            f"No attribution rows found in the lookback window."
        )
    if days < _MIN_DAYS_FOR_RATIO:
        return (
            f"{asset_code}: STATUS=DATA_ABSENT. "
            f"Only {days} day(s) of data in the lookback window "
            f"(minimum {_MIN_DAYS_FOR_RATIO} required for a reliable ratio)."
        )

    ratio_str = f"{ratio:.1%}" if ratio is not None else "N/A"
    actual_str = f"¥{avg_actual:,.0f}" if avg_actual is not None else "N/A"
    pf_str = f"¥{avg_pf_grid:,.0f}" if avg_pf_grid is not None else "N/A"
    dominant_str = dominant_bucket.replace("_", " ").title() if dominant_bucket else "unknown"

    base = (
        f"{asset_code} ({days}d avg): "
        f"actual={actual_str}/day, grid-feasible={pf_str}/day, "
        f"realization={ratio_str}. "
        f"Dominant loss: {dominant_str}. "
        f"Status: {status}."
    )

    # B2: INDETERMINATE — explain why ratio is undefined
    if status == "INDETERMINATE":
        pf_reason = (
            "non-positive" if (avg_pf_grid is not None and avg_pf_grid <= 0)
            else "unavailable"
        )
        base += (
            f" Realization ratio cannot be computed: "
            f"grid-feasible benchmark is {pf_reason} (avg={pf_str}/day). "
            f"Asset actual PnL ({actual_str}/day) cannot be benchmarked."
        )
    elif status == "WARN":
        base += " Monitor closely — realization is below target threshold."
    elif status == "ALERT":
        base += " Realization has deteriorated significantly. Review dispatch and execution quality."
    elif status == "CRITICAL":
        # B1: removed "or data is missing" — that case is now DATA_ABSENT
        base += " Realization is critically low. Immediate review required."

    return base


def compute_realization_status(
    asset_code: str,
    snapshot_date: date,
    engine,
    lookback_days: int = _LOOKBACK_DAYS_DEFAULT,
) -> Dict[str, Any]:
    """
    Compute realization status for a single asset on a given snapshot_date.
    Returns a dict ready for upsert into monitoring.asset_realization_status.
    """
    from sqlalchemy import text

    from_date = snapshot_date - timedelta(days=lookback_days - 1)

    sql = text("""
        SELECT
            cleared_actual_pnl,
            pf_grid_feasible_pnl,
            grid_restriction_loss,
            forecast_error_loss,
            strategy_error_loss,
            nomination_loss,
            execution_clearing_loss
        FROM reports.bess_asset_daily_attribution
        WHERE asset_code = :asset
          AND trade_date >= :from_date
          AND trade_date <= :to_date
          AND cleared_actual_pnl IS NOT NULL
    """)

    with engine.begin() as conn:
        rows = conn.execute(sql, {
            "asset": asset_code,
            "from_date": from_date,
            "to_date": snapshot_date,
        }).fetchall()

    days_in_window = len(rows)

    # B1: DATA_ABSENT — return immediately for zero-row case
    if days_in_window == 0:
        return {
            "asset_code": asset_code,
            "snapshot_date": snapshot_date,
            "lookback_days": lookback_days,
            "days_in_window": 0,
            "avg_cleared_actual_pnl": None,
            "avg_pf_grid_feasible_pnl": None,
            "realization_ratio": None,
            "avg_grid_restriction_loss": None,
            "avg_forecast_error_loss": None,
            "avg_strategy_error_loss": None,
            "avg_nomination_loss": None,
            "avg_execution_clearing_loss": None,
            "dominant_loss_bucket": None,
            "status_level": "DATA_ABSENT",
            "narrative": _build_narrative(asset_code, None, "DATA_ABSENT", 0, None, None, None),
        }

    def _avg(col_idx: int) -> Optional[float]:
        vals = [r[col_idx] for r in rows if r[col_idx] is not None]
        return sum(vals) / len(vals) if vals else None

    avg_actual = _avg(0)
    avg_pf_grid = _avg(1)
    avg_grid_restrict = _avg(2)
    avg_forecast = _avg(3)
    avg_strategy = _avg(4)
    avg_nomination = _avg(5)
    avg_execution = _avg(6)

    # B2: guard against zero / negative benchmark before dividing
    pf_grid_positive = avg_pf_grid is not None and avg_pf_grid > 0

    ratio: Optional[float] = None
    if avg_actual is not None and pf_grid_positive:
        ratio = avg_actual / avg_pf_grid

    # Dominant loss bucket (meaningful even when ratio is INDETERMINATE)
    loss_map = {
        "avg_grid_restriction_loss": avg_grid_restrict,
        "avg_forecast_error_loss": avg_forecast,
        "avg_strategy_error_loss": avg_strategy,
        "avg_nomination_loss": avg_nomination,
        "avg_execution_clearing_loss": avg_execution,
    }
    non_null = {k: v for k, v in loss_map.items() if v is not None}
    dominant_bucket = max(non_null, key=non_null.__getitem__) if non_null else None

    status = _classify_status(ratio, days_in_window, pf_grid_positive)
    narrative = _build_narrative(
        asset_code, ratio, status, days_in_window, dominant_bucket, avg_actual, avg_pf_grid
    )

    # B5: structured event for actionable statuses
    if status in ("ALERT", "CRITICAL"):
        logger.info(
            "MONITORING_ALERT job=realization_monitor asset=%s status=%s date=%s "
            "ratio=%s dominant_loss=%s lookback_days=%d",
            asset_code, status, snapshot_date,
            f"{ratio:.4f}" if ratio is not None else "null",
            dominant_bucket or "null",
            lookback_days,
        )

    return {
        "asset_code": asset_code,
        "snapshot_date": snapshot_date,
        "lookback_days": lookback_days,
        "days_in_window": days_in_window,
        "avg_cleared_actual_pnl": avg_actual,
        "avg_pf_grid_feasible_pnl": avg_pf_grid,
        "realization_ratio": ratio,
        "avg_grid_restriction_loss": avg_grid_restrict,
        "avg_forecast_error_loss": avg_forecast,
        "avg_strategy_error_loss": avg_strategy,
        "avg_nomination_loss": avg_nomination,
        "avg_execution_clearing_loss": avg_execution,
        "dominant_loss_bucket": dominant_bucket,
        "status_level": status,
        "narrative": narrative,
    }


def upsert_realization_status(engine, rows: List[Dict[str, Any]]) -> None:
    from sqlalchemy import text

    sql = text("""
        INSERT INTO monitoring.asset_realization_status (
            asset_code, snapshot_date, lookback_days, days_in_window,
            avg_cleared_actual_pnl, avg_pf_grid_feasible_pnl, realization_ratio,
            avg_grid_restriction_loss, avg_forecast_error_loss, avg_strategy_error_loss,
            avg_nomination_loss, avg_execution_clearing_loss,
            dominant_loss_bucket, status_level, narrative, computed_at
        ) VALUES (
            :asset_code, :snapshot_date, :lookback_days, :days_in_window,
            :avg_cleared_actual_pnl, :avg_pf_grid_feasible_pnl, :realization_ratio,
            :avg_grid_restriction_loss, :avg_forecast_error_loss, :avg_strategy_error_loss,
            :avg_nomination_loss, :avg_execution_clearing_loss,
            :dominant_loss_bucket, :status_level, :narrative, now()
        )
        ON CONFLICT (asset_code, snapshot_date, lookback_days) DO UPDATE SET
            days_in_window              = EXCLUDED.days_in_window,
            avg_cleared_actual_pnl      = EXCLUDED.avg_cleared_actual_pnl,
            avg_pf_grid_feasible_pnl    = EXCLUDED.avg_pf_grid_feasible_pnl,
            realization_ratio           = EXCLUDED.realization_ratio,
            avg_grid_restriction_loss   = EXCLUDED.avg_grid_restriction_loss,
            avg_forecast_error_loss     = EXCLUDED.avg_forecast_error_loss,
            avg_strategy_error_loss     = EXCLUDED.avg_strategy_error_loss,
            avg_nomination_loss         = EXCLUDED.avg_nomination_loss,
            avg_execution_clearing_loss = EXCLUDED.avg_execution_clearing_loss,
            dominant_loss_bucket        = EXCLUDED.dominant_loss_bucket,
            status_level                = EXCLUDED.status_level,
            narrative                   = EXCLUDED.narrative,
            computed_at                 = now()
    """)
    with engine.begin() as conn:
        for row in rows:
            conn.execute(sql, row)


def query_realization_status(
    asset_code: Optional[str] = None,
    snapshot_date: Optional[str] = None,
    lookback_days: int = 30,
) -> List[Dict[str, Any]]:
    """
    Agent-facing DB query. Returns current realization status rows.

    Sort order: most severe first (CRITICAL → ALERT → WARN → DATA_ABSENT →
    INDETERMINATE → NORMAL). Fixed B1: was ORDER BY status_level DESC (alphabetic,
    incorrect order).
    """
    from sqlalchemy import text
    from services.common.db_utils import get_engine

    engine = get_engine()

    date_clause = "snapshot_date = :snap_date" if snapshot_date else (
        "snapshot_date = (SELECT MAX(snapshot_date) FROM monitoring.asset_realization_status)"
    )
    asset_clause = "AND asset_code = :asset_code" if asset_code else ""

    sql = text(f"""
        SELECT
            asset_code, snapshot_date, lookback_days, days_in_window,
            avg_cleared_actual_pnl, avg_pf_grid_feasible_pnl, realization_ratio,
            avg_grid_restriction_loss, avg_forecast_error_loss, avg_strategy_error_loss,
            avg_nomination_loss, avg_execution_clearing_loss,
            dominant_loss_bucket, status_level, narrative, computed_at
        FROM monitoring.asset_realization_status
        WHERE {date_clause}
          AND lookback_days = :lookback_days
          {asset_clause}
        ORDER BY
            CASE status_level
                WHEN 'CRITICAL'      THEN 1
                WHEN 'ALERT'         THEN 2
                WHEN 'WARN'          THEN 3
                WHEN 'DATA_ABSENT'   THEN 4
                WHEN 'INDETERMINATE' THEN 5
                WHEN 'NORMAL'        THEN 6
                ELSE 7
            END,
            realization_ratio ASC NULLS LAST
    """)

    params: Dict[str, Any] = {"lookback_days": lookback_days}
    if snapshot_date:
        params["snap_date"] = date.fromisoformat(snapshot_date)
    if asset_code:
        params["asset_code"] = asset_code

    try:
        with engine.begin() as conn:
            rows = conn.execute(sql, params).fetchall()
        return [dict(r._mapping) for r in rows]
    except Exception as exc:
        logger.error("query_realization_status failed: %s", exc)
        return [{"error": str(exc)}]
