# -*- coding: utf-8 -*-
"""
Strategy promote loop — rolling evaluation of capture/forecast strategies.

Reads ``marketdata.bess_capture_daily``, computes per-(model, province) metrics
over a rolling window, and writes ``marketdata.strategy_experiments`` with
status transitions:

    champion            best mean_capture_rate among active models in the scope
    candidate           everything else
    retired-candidate   mean_capture_rate < RETIRE_RATIO × champion's for
                        ≥ RETIRE_WINDOWS consecutive evaluation windows
                        (recovers to candidate once it beats the ratio again)

No auto-deletion — status is a flag for human review (清鹏 leaderboard lesson,
implemented as promote/demote flags rather than auto-replacement).
"""
from __future__ import annotations

import logging
from typing import Optional

import pandas as pd
from sqlalchemy import text as sql_text
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)

RETIRE_RATIO = 0.7        # retire when capture < 70% of champion's
RETIRE_WINDOWS = 2        # for this many consecutive evaluations
SCOPE = "bess_capture"

_DDL = """
CREATE TABLE IF NOT EXISTS {schema}.strategy_experiments (
    id                      BIGSERIAL PRIMARY KEY,
    scope                   TEXT NOT NULL DEFAULT 'bess_capture',
    model                   TEXT NOT NULL,
    province                TEXT NOT NULL,
    duration_h              DOUBLE PRECISION NOT NULL,
    power_mw                DOUBLE PRECISION NOT NULL,
    roundtrip_eff           DOUBLE PRECISION NOT NULL,
    window_days             INT NOT NULL,
    window_end              DATE NOT NULL,
    days                    INT NOT NULL,
    mean_capture_rate       DOUBLE PRECISION,
    mean_realized_per_mwh   DOUBLE PRECISION,
    mean_theoretical_per_mwh DOUBLE PRECISION,
    delta_vs_champion       DOUBLE PRECISION,
    status                  TEXT NOT NULL DEFAULT 'candidate'
        CHECK (status IN ('champion', 'candidate', 'retired-candidate')),
    note                    TEXT,
    evaluated_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (scope, model, province, duration_h, power_mw, roundtrip_eff, window_days, window_end)
);
CREATE INDEX IF NOT EXISTS ix_strategy_experiments_lb
    ON {schema}.strategy_experiments (scope, province, window_end DESC);
"""


def ensure_table(engine: Engine, schema: str = "marketdata") -> None:
    with engine.begin() as conn:
        conn.execute(sql_text(_DDL.format(schema=schema)))


# ── Pure logic (testable without DB) ─────────────────────────────────────────

def compute_window_metrics(capture_df: pd.DataFrame, window_days: int,
                           window_end) -> pd.DataFrame:
    """Aggregate bess_capture_daily rows into per-(model, province) metrics.

    capture_df columns: model, province, date, capture_rate,
                        realized_profit_per_mwh_day, theoretical_profit_per_mwh_day
    Returns one row per (model, province) with days + means.
    """
    end = pd.Timestamp(window_end)
    start = end - pd.Timedelta(days=window_days - 1)
    df = capture_df[(capture_df["date"] > start) & (capture_df["date"] <= end)]
    if df.empty:
        return pd.DataFrame(columns=["model", "province", "days",
                                     "mean_capture_rate", "mean_realized_per_mwh",
                                     "mean_theoretical_per_mwh"])
    g = df.groupby(["model", "province"], as_index=False).agg(
        days=("date", "nunique"),
        mean_capture_rate=("capture_rate", "mean"),
        mean_realized_per_mwh=("realized_profit_per_mwh_day", "mean"),
        mean_theoretical_per_mwh=("theoretical_profit_per_mwh_day", "mean"),
    )
    return g


def assign_status(metrics: pd.DataFrame,
                  prev_experiments: pd.DataFrame) -> pd.DataFrame:
    """Assign champion/candidate/retired-candidate per (model, province).

    metrics: output of compute_window_metrics for the current window.
    prev_experiments: rows from strategy_experiments of earlier windows
                      (columns model, province, window_end, mean_capture_rate,
                       status) — used only for the retire-streak check.
    """
    if metrics.empty:
        return metrics
    out = metrics.copy()
    out["champion_capture"] = out.groupby("province")["mean_capture_rate"].transform("max")
    out["delta_vs_champion"] = out["mean_capture_rate"] - out["champion_capture"]

    def _streak(model: str, province: str) -> int:
        """Consecutive *current+*past windows below RETIRE_RATIO of champion."""
        cur = out[(out["model"] == model) & (out["province"] == province)]
        if cur.empty:
            return 0
        champ = float(cur["champion_capture"].iloc[0])
        cap = float(cur["mean_capture_rate"].iloc[0])
        if not champ or cap >= RETIRE_RATIO * champ:
            return 0
        streak = 1
        prev = prev_experiments[
            (prev_experiments["model"] == model)
            & (prev_experiments["province"] == province)
        ].sort_values("window_end", ascending=False)
        for _, row in prev.iterrows():
            prev_champ = prev_experiments[
                (prev_experiments["province"] == province)
                & (prev_experiments["window_end"] == row["window_end"])
            ]["mean_capture_rate"].max()
            if not prev_champ:
                break
            if row["mean_capture_rate"] < RETIRE_RATIO * prev_champ:
                streak += 1
            else:
                break
        return streak

    statuses = []
    for _, row in out.iterrows():
        if row["mean_capture_rate"] == row["champion_capture"]:
            statuses.append("champion")
        elif _streak(row["model"], row["province"]) >= RETIRE_WINDOWS:
            statuses.append("retired-candidate")
        else:
            statuses.append("candidate")
    out["status"] = statuses
    return out.drop(columns=["champion_capture"])


# ── DB layer ─────────────────────────────────────────────────────────────────

def evaluate(engine: Engine, schema: str = "marketdata",
             window_days: int = 30,
             duration_h: float = 2.0, power_mw: float = 1.0,
             roundtrip_eff: float = 0.85,
             window_end=None) -> dict:
    """Run one rolling evaluation and upsert strategy_experiments.

    Returns a summary dict {window_end, window_days, rows: [...]} for the UI.
    """
    ensure_table(engine, schema)
    capture = pd.read_sql(
        sql_text(
            f"SELECT model, province, date, capture_rate, "
            f"realized_profit_per_mwh_day, theoretical_profit_per_mwh_day "
            f"FROM {schema}.bess_capture_daily "
            f"WHERE duration_h = :d AND power_mw = :p AND roundtrip_eff = :r"
        ),
        engine, params={"d": duration_h, "p": power_mw, "r": roundtrip_eff},
    )
    if capture.empty:
        return {"window_end": None, "window_days": window_days, "rows": [],
                "note": "bess_capture_daily is empty for this scope"}
    capture["date"] = pd.to_datetime(capture["date"])
    if window_end is None:
        window_end = capture["date"].max().date()

    metrics = compute_window_metrics(capture, window_days, window_end)

    prev = pd.read_sql(
        sql_text(
            f"SELECT model, province, window_end, mean_capture_rate, status "
            f"FROM {schema}.strategy_experiments "
            f"WHERE scope = :s AND duration_h = :d AND power_mw = :p "
            f"AND roundtrip_eff = :r AND window_days = :w AND window_end < :we"
        ),
        engine, params={"s": SCOPE, "d": duration_h, "p": power_mw,
                        "r": roundtrip_eff, "w": window_days, "we": window_end},
    )

    scored = assign_status(metrics, prev)

    with engine.begin() as conn:
        for _, row in scored.iterrows():
            conn.execute(sql_text(f"""
                INSERT INTO {schema}.strategy_experiments
                    (scope, model, province, duration_h, power_mw, roundtrip_eff,
                     window_days, window_end, days, mean_capture_rate,
                     mean_realized_per_mwh, mean_theoretical_per_mwh,
                     delta_vs_champion, status)
                VALUES (:scope, :model, :province, :d, :p, :r, :w, :we, :days,
                        :cap, :real, :theo, :delta, :status)
                ON CONFLICT (scope, model, province, duration_h, power_mw,
                             roundtrip_eff, window_days, window_end)
                DO UPDATE SET days = EXCLUDED.days,
                    mean_capture_rate = EXCLUDED.mean_capture_rate,
                    mean_realized_per_mwh = EXCLUDED.mean_realized_per_mwh,
                    mean_theoretical_per_mwh = EXCLUDED.mean_theoretical_per_mwh,
                    delta_vs_champion = EXCLUDED.delta_vs_champion,
                    status = EXCLUDED.status,
                    evaluated_at = NOW()
            """), {
                "scope": SCOPE, "model": row["model"], "province": row["province"],
                "d": duration_h, "p": power_mw, "r": roundtrip_eff,
                "w": window_days, "we": window_end, "days": int(row["days"]),
                "cap": _f(row["mean_capture_rate"]),
                "real": _f(row["mean_realized_per_mwh"]),
                "theo": _f(row["mean_theoretical_per_mwh"]),
                "delta": _f(row["delta_vs_champion"]),
                "status": row["status"],
            })

    rows = scored.to_dict("records")
    logger.info("strategy_experiments: %d rows evaluated (window_end=%s)",
                len(rows), window_end)
    return {"window_end": str(window_end), "window_days": window_days,
            "rows": rows}


def load_leaderboard(engine: Engine, schema: str = "marketdata",
                     window_days: int = 30,
                     duration_h: float = 2.0, power_mw: float = 1.0,
                     roundtrip_eff: float = 0.85) -> pd.DataFrame:
    """Latest evaluation per (model, province) for the leaderboard UI."""
    return pd.read_sql(
        sql_text(f"""
            SELECT DISTINCT ON (model, province)
                   model, province, window_end, days, mean_capture_rate,
                   mean_realized_per_mwh, mean_theoretical_per_mwh,
                   delta_vs_champion, status, evaluated_at
            FROM {schema}.strategy_experiments
            WHERE scope = :s AND duration_h = :d AND power_mw = :p
              AND roundtrip_eff = :r AND window_days = :w
            ORDER BY model, province, window_end DESC
        """),
        engine, params={"s": SCOPE, "d": duration_h, "p": power_mw,
                        "r": roundtrip_eff, "w": window_days},
    )


def _f(v) -> Optional[float]:
    return None if pd.isna(v) else float(v)
