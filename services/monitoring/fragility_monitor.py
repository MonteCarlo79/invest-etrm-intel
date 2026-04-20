"""
services/monitoring/fragility_monitor.py

Fragility status computation service for BESS assets.

Reads realization status from monitoring.asset_realization_status,
computes trend, combines into a composite fragility score, writes to
monitoring.asset_fragility_status.

Composite score weights:
    realization_score  0.70  (current realization quality)
    trend_score        0.30  (directional change over last 7d vs prior 7d)

Fragility thresholds:
    LOW      composite_score < 0.25
    MEDIUM   composite_score in [0.25, 0.50)
    HIGH     composite_score in [0.50, 0.75)
    CRITICAL composite_score >= 0.75
"""
from __future__ import annotations

import logging
from datetime import date, timedelta
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

_W_REALIZATION = 0.70
_W_TREND = 0.30

_STATUS_TO_SCORE = {
    "NORMAL": 0.0,
    "WARN": 0.33,
    "ALERT": 0.67,
    "CRITICAL": 1.0,
}

_FRAGILITY_THRESHOLDS = [
    (0.75, "CRITICAL"),
    (0.50, "HIGH"),
    (0.25, "MEDIUM"),
    (0.0,  "LOW"),
]


def _classify_fragility(score: float) -> str:
    for threshold, level in _FRAGILITY_THRESHOLDS:
        if score >= threshold:
            return level
    return "LOW"


def _compute_trend_score(recent_ratio: Optional[float], prior_ratio: Optional[float]) -> float:
    """
    Score the directional trend. Higher = more fragile direction.
      improving (ratio went up)      → 0.0
      stable (delta near 0)          → 0.2
      deteriorating (ratio fell)     → 0.7
      sharp decline (> 0.15 drop)    → 1.0
    """
    if recent_ratio is None or prior_ratio is None:
        return 0.2  # neutral when trend cannot be computed

    delta = recent_ratio - prior_ratio

    if delta > 0.05:
        return 0.0   # improving
    if delta > -0.05:
        return 0.2   # stable
    if delta > -0.15:
        return 0.7   # deteriorating
    return 1.0       # sharp decline


def _load_realization_rows(
    asset_code: str,
    snapshot_date: date,
    engine,
    lookback_days: int = 30,
) -> Optional[Dict[str, Any]]:
    """Load today's realization status row for the asset."""
    from sqlalchemy import text

    sql = text("""
        SELECT
            realization_ratio,
            status_level,
            days_in_window,
            avg_cleared_actual_pnl,
            avg_pf_grid_feasible_pnl
        FROM monitoring.asset_realization_status
        WHERE asset_code = :asset
          AND snapshot_date = :snap
          AND lookback_days = :lookback
    """)
    with engine.begin() as conn:
        row = conn.execute(sql, {
            "asset": asset_code, "snap": snapshot_date, "lookback": lookback_days
        }).fetchone()
    return dict(row._mapping) if row else None


def _load_short_window_ratios(
    asset_code: str,
    snapshot_date: date,
    engine,
    window_days: int = 7,
) -> Dict[str, Optional[float]]:
    """
    Compute recent (last 7d) and prior (7d before that) realization ratios
    directly from the attribution table, for trend detection.
    """
    from sqlalchemy import text

    recent_from = snapshot_date - timedelta(days=window_days - 1)
    prior_from = snapshot_date - timedelta(days=2 * window_days - 1)
    prior_to = snapshot_date - timedelta(days=window_days)

    sql = text("""
        SELECT
            AVG(CASE WHEN trade_date >= :recent_from THEN
                CASE WHEN pf_grid_feasible_pnl != 0 THEN cleared_actual_pnl / pf_grid_feasible_pnl END
            END) AS recent_ratio,
            AVG(CASE WHEN trade_date BETWEEN :prior_from AND :prior_to THEN
                CASE WHEN pf_grid_feasible_pnl != 0 THEN cleared_actual_pnl / pf_grid_feasible_pnl END
            END) AS prior_ratio
        FROM reports.bess_asset_daily_attribution
        WHERE asset_code = :asset
          AND trade_date >= :prior_from
          AND trade_date <= :snap
          AND cleared_actual_pnl IS NOT NULL
          AND pf_grid_feasible_pnl IS NOT NULL
    """)
    with engine.begin() as conn:
        row = conn.execute(sql, {
            "asset": asset_code,
            "snap": snapshot_date,
            "recent_from": recent_from,
            "prior_from": prior_from,
            "prior_to": prior_to,
        }).fetchone()

    if row:
        return {
            "recent_ratio": float(row[0]) if row[0] is not None else None,
            "prior_ratio": float(row[1]) if row[1] is not None else None,
        }
    return {"recent_ratio": None, "prior_ratio": None}


def _build_narrative(
    asset_code: str,
    composite: float,
    level: str,
    realization_ratio: Optional[float],
    realization_level: str,
    trend_score: float,
    ratio_delta: Optional[float],
    dominant_factor: str,
) -> str:
    ratio_str = f"{realization_ratio:.1%}" if realization_ratio is not None else "N/A"
    delta_str = (
        f"{ratio_delta:+.1%}" if ratio_delta is not None else "N/A"
    )

    base = (
        f"{asset_code}: fragility={level} (score={composite:.2f}). "
        f"Realization={ratio_str} [{realization_level}], "
        f"7d trend={delta_str}. "
        f"Dominant factor: {dominant_factor.replace('_', ' ').title()}."
    )

    if level == "HIGH":
        base += " Consider reviewing execution quality and dispatch strategy."
    elif level == "CRITICAL":
        base += " Immediate attention required — asset performance is critically impaired."

    return base


def compute_fragility_status(
    asset_code: str,
    snapshot_date: date,
    engine,
    lookback_days: int = 30,
) -> Dict[str, Any]:
    """
    Compute fragility status for a single asset on snapshot_date.
    Returns a dict ready for upsert into monitoring.asset_fragility_status.
    """
    # Load realization status (must already be computed for today)
    real_row = _load_realization_rows(asset_code, snapshot_date, engine, lookback_days)

    if real_row is None:
        return {
            "asset_code": asset_code,
            "snapshot_date": snapshot_date,
            "realization_score": 1.0,
            "trend_score": 0.2,
            "composite_score": round(_W_REALIZATION * 1.0 + _W_TREND * 0.2, 4),
            "fragility_level": "CRITICAL",
            "realization_ratio": None,
            "realization_status_level": "CRITICAL",
            "days_in_window": 0,
            "recent_ratio": None,
            "prior_ratio": None,
            "ratio_delta": None,
            "dominant_factor": "realization_score",
            "narrative": (
                f"{asset_code}: Realization status not found for {snapshot_date}. "
                "Run run_realization_monitor.py first."
            ),
        }

    realization_ratio = real_row.get("realization_ratio")
    realization_level = real_row.get("status_level", "CRITICAL")
    days_in_window = real_row.get("days_in_window", 0)

    # Realization score from status level
    realization_score = _STATUS_TO_SCORE.get(realization_level, 1.0)

    # Trend computation
    trend_data = _load_short_window_ratios(asset_code, snapshot_date, engine)
    recent_ratio = trend_data["recent_ratio"]
    prior_ratio = trend_data["prior_ratio"]
    ratio_delta = (recent_ratio - prior_ratio) if (recent_ratio is not None and prior_ratio is not None) else None
    trend_score = _compute_trend_score(recent_ratio, prior_ratio)

    # Composite
    composite = round(_W_REALIZATION * realization_score + _W_TREND * trend_score, 4)
    fragility_level = _classify_fragility(composite)

    # Dominant factor
    component_scores = {
        "realization_score": realization_score * _W_REALIZATION,
        "trend_score": trend_score * _W_TREND,
    }
    dominant_factor = max(component_scores, key=component_scores.__getitem__)

    narrative = _build_narrative(
        asset_code, composite, fragility_level,
        realization_ratio, realization_level,
        trend_score, ratio_delta, dominant_factor,
    )

    return {
        "asset_code": asset_code,
        "snapshot_date": snapshot_date,
        "realization_score": realization_score,
        "trend_score": trend_score,
        "composite_score": composite,
        "fragility_level": fragility_level,
        "realization_ratio": realization_ratio,
        "realization_status_level": realization_level,
        "days_in_window": days_in_window,
        "recent_ratio": recent_ratio,
        "prior_ratio": prior_ratio,
        "ratio_delta": ratio_delta,
        "dominant_factor": dominant_factor,
        "narrative": narrative,
    }


def upsert_fragility_status(engine, rows: List[Dict[str, Any]]) -> None:
    from sqlalchemy import text

    sql = text("""
        INSERT INTO monitoring.asset_fragility_status (
            asset_code, snapshot_date,
            realization_score, trend_score, composite_score, fragility_level,
            realization_ratio, realization_status_level, days_in_window,
            recent_ratio, prior_ratio, ratio_delta,
            dominant_factor, narrative, computed_at
        ) VALUES (
            :asset_code, :snapshot_date,
            :realization_score, :trend_score, :composite_score, :fragility_level,
            :realization_ratio, :realization_status_level, :days_in_window,
            :recent_ratio, :prior_ratio, :ratio_delta,
            :dominant_factor, :narrative, now()
        )
        ON CONFLICT (asset_code, snapshot_date) DO UPDATE SET
            realization_score          = EXCLUDED.realization_score,
            trend_score                = EXCLUDED.trend_score,
            composite_score            = EXCLUDED.composite_score,
            fragility_level            = EXCLUDED.fragility_level,
            realization_ratio          = EXCLUDED.realization_ratio,
            realization_status_level   = EXCLUDED.realization_status_level,
            days_in_window             = EXCLUDED.days_in_window,
            recent_ratio               = EXCLUDED.recent_ratio,
            prior_ratio                = EXCLUDED.prior_ratio,
            ratio_delta                = EXCLUDED.ratio_delta,
            dominant_factor            = EXCLUDED.dominant_factor,
            narrative                  = EXCLUDED.narrative,
            computed_at                = now()
    """)
    with engine.begin() as conn:
        for row in rows:
            conn.execute(sql, row)


def query_fragility_status(
    asset_code: Optional[str] = None,
    snapshot_date: Optional[str] = None,
    min_level: str = "LOW",
) -> List[Dict[str, Any]]:
    """
    Agent-facing DB query. Returns fragility status rows filtered by level.
    Used by the query_asset_fragility_status agent tool (Pattern A).
    """
    from sqlalchemy import text
    from services.common.db_utils import get_engine

    _LEVEL_ORDER = {"LOW": 0, "MEDIUM": 1, "HIGH": 2, "CRITICAL": 3}
    min_score = _LEVEL_ORDER.get(min_level, 0)
    valid_levels = [k for k, v in _LEVEL_ORDER.items() if v >= min_score]
    level_placeholders = ", ".join(f"'{lv}'" for lv in valid_levels)

    date_clause = "snapshot_date = :snap_date" if snapshot_date else (
        "snapshot_date = (SELECT MAX(snapshot_date) FROM monitoring.asset_fragility_status)"
    )
    asset_clause = "AND asset_code = :asset_code" if asset_code else ""

    sql = text(f"""
        SELECT
            asset_code, snapshot_date,
            realization_score, trend_score, composite_score, fragility_level,
            realization_ratio, realization_status_level, days_in_window,
            recent_ratio, prior_ratio, ratio_delta,
            dominant_factor, narrative, computed_at
        FROM monitoring.asset_fragility_status
        WHERE {date_clause}
          AND fragility_level IN ({level_placeholders})
          {asset_clause}
        ORDER BY composite_score DESC
    """)

    params: Dict[str, Any] = {}
    if snapshot_date:
        params["snap_date"] = date.fromisoformat(snapshot_date)
    if asset_code:
        params["asset_code"] = asset_code

    engine = get_engine()
    try:
        with engine.begin() as conn:
            rows = conn.execute(sql, params).fetchall()
        return [dict(r._mapping) for r in rows]
    except Exception as exc:
        logger.error("query_fragility_status failed: %s", exc)
        return [{"error": str(exc)}]
