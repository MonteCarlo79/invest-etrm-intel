# services/nodal_forecast/backtest.py
"""Backtest for the L2 nodal price formation model.

nodal_mae compares the nodal model against actual nodal prices per cluster
and always reports the grid-only baseline MAE alongside — the gate is that
the nodal model must not be worse than predicting grid price directly.
Both numbers are returned (no exception on gate failure) so callers
(Task 7 S4 metrics, daily pipeline checks) decide how to surface it.
"""
from __future__ import annotations

from datetime import date

import pandas as pd

BASELINE_SUFFIX = "__grid_baseline"


def nodal_mae(model_fn, days: list[date]) -> dict[str, float]:
    """Per-cluster MAE of the nodal model vs actuals, plus grid-only baseline.

    model_fn(day) -> pd.DataFrame with columns:
        cluster   — congestion cluster / zone label the row belongs to
        nodal_hat — L2 nodal forecast for that row (day level)
        grid_hat  — grid-wide forecast used as the naive nodal predictor
        actual    — realized nodal price (md_mengxi_nodal_price_96 avg)
    Rows are typically one per node per day; days with an empty/None frame
    are skipped.

    Returns {"<cluster>": nodal_mae, "<cluster>__grid_baseline": grid_mae}
    per cluster. Gate for callers: out[c] <= out[c + "__grid_baseline"].
    """
    frames = []
    for d in days:
        df = model_fn(d)
        if df is not None and len(df):
            frames.append(df)
    if not frames:
        return {}
    all_rows = pd.concat(frames, ignore_index=True)
    out: dict[str, float] = {}
    for cluster, sub in all_rows.groupby("cluster"):
        key = str(cluster)
        out[key] = float((sub["nodal_hat"] - sub["actual"]).abs().mean())
        out[key + BASELINE_SUFFIX] = float((sub["grid_hat"] - sub["actual"]).abs().mean())
    return out
