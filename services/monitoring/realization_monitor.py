"""
services/monitoring/realization_monitor.py

Realization status computation service for BESS assets.

Reads rolling attribution data from reports.bess_asset_daily_attribution,
computes realization ratio and status level, writes to
monitoring.asset_realization_status.

Status thresholds:
    NORMAL   realization_ratio >= 0.70
    WARN     realization_ratio in [0.50, 0.70)
    ALERT    realization_ratio in [0.30, 0.50)
    CRITICAL realization_ratio < 0.30  OR  no data in window
"""
from __future__ import annotations

import logging
from datetime import date, timedelta
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

_LOOKBACK_DAYS_DEFAULT = 30

_THRESHOLDS = [
    (0.70, "NORMAL"),
    (0.50, "WARN"),
    (0.30, "ALERT"),
]


def _classify_status(ratio: Optional[float], days_in_window: int) -> str:
    if days_in_window == 0 or ratio is None:
        return "CRITICAL"
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
    if days == 0:
        return f"{asset_code}: No attribution data in the lookback window."

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

    if status == "WARN":
        base += " Monitor closely — realization is below target threshold."
    elif status == "ALERT":
        base += " Realization has deteriorated significantly. Review dispatch and execution quality."
    elif status == "CRITICAL":
        base += " Realization is critically low or data is missing. Immediate review required."

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
            "status_level": "CRITICAL",
            "narrative": f"{asset_code}: No attribution data in window ({from_date} – {snapshot_date}).",
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

    # Realization ratio
    ratio: Optional[float] = None
    if avg_actual is not None and avg_pf_grid and avg_pf_grid != 0:
        ratio = avg_actual / avg_pf_grid

    # Dominant loss bucket
    loss_map = {
        "avg_grid_restriction_loss": avg_grid_restrict,
        "avg_forecast_error_loss": avg_forecast,
        "avg_strategy_error_loss": avg_strategy,
        "avg_nomination_loss": avg_nomination,
        "avg_execution_clearing_loss": avg_execution,
    }
    non_null = {k: v for k, v in loss_map.items() if v is not None}
    dominant_bucket = max(non_null, key=non_null.__getitem__) if non_null else None

    status = _classify_status(ratio, days_in_window)
    narrative = _build_narrative(
        asset_code, ratio, status, days_in_window, dominant_bucket, avg_actual, avg_pf_grid
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
    Used by the query_asset_realization_status agent tool (Pattern A).
    """
    from sqlalchemy import text
    from services.common.db_utils import get_engine

    engine = get_engine()

    # Fall back to most recent snapshot if today is not yet computed
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
        ORDER BY status_level DESC, realization_ratio ASC NULLS LAST
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
