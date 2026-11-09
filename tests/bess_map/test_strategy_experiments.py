"""Tests for the strategy promote loop (strategy_experiments.assign_status)."""
import pandas as pd
import pytest

from services.bess_map.strategy_experiments import (
    RETIRE_RATIO, assign_status, compute_window_metrics,
)


def _capture(rows):
    return pd.DataFrame(rows, columns=["model", "province", "date", "capture_rate",
                                       "realized_profit_per_mwh_day",
                                       "theoretical_profit_per_mwh_day"])


def _metrics(rows, window_end="2026-09-09", window_days=30):
    df = _capture(rows)
    df["date"] = pd.to_datetime(df["date"])
    return compute_window_metrics(df, window_days, window_end)


def test_champion_is_best_capture_per_province():
    m = _metrics([
        ("a", "p1", "2026-09-05", 0.8, 100, 100),
        ("b", "p1", "2026-09-05", 0.6, 100, 100),
        ("a", "p2", "2026-09-05", 0.5, 100, 100),
        ("b", "p2", "2026-09-05", 0.7, 100, 100),
    ])
    out = assign_status(m, pd.DataFrame(columns=["model", "province", "window_end",
                                                 "mean_capture_rate", "status"]))
    st = dict(zip(zip(out["model"], out["province"]), out["status"]))
    assert st[("a", "p1")] == "champion"
    assert st[("b", "p1")] == "candidate"
    assert st[("b", "p2")] == "champion"
    assert st[("a", "p2")] == "candidate"
    # delta math
    delta = dict(zip(zip(out["model"], out["province"]), out["delta_vs_champion"]))
    assert delta[("b", "p1")] == pytest.approx(-0.2)


def test_window_excludes_old_rows():
    m = _metrics([
        ("a", "p1", "2026-07-01", 0.9, 999, 999),   # outside 30d window
        ("a", "p1", "2026-09-05", 0.5, 100, 100),
    ])
    assert float(m["mean_capture_rate"].iloc[0]) == 0.5
    assert float(m["mean_realized_per_mwh"].iloc[0]) == 100.0


def test_retire_after_two_consecutive_bad_windows():
    # champion at 1.0, model "b" at 0.5 (< 0.7 ratio) this window and last window
    m = _metrics([
        ("a", "p1", "2026-09-05", 1.0, 100, 100),
        ("b", "p1", "2026-09-05", 0.5, 100, 100),
    ])
    prev = pd.DataFrame([
        {"model": "a", "province": "p1", "window_end": "2026-08-10",
         "mean_capture_rate": 1.0, "status": "champion"},
        {"model": "b", "province": "p1", "window_end": "2026-08-10",
         "mean_capture_rate": 0.5, "status": "candidate"},
    ])
    out = assign_status(m, prev)
    st = dict(zip(out["model"], out["status"]))
    assert st["b"] == "retired-candidate"


def test_first_bad_window_is_only_candidate():
    m = _metrics([
        ("a", "p1", "2026-09-05", 1.0, 100, 100),
        ("b", "p1", "2026-09-05", 0.5, 100, 100),
    ])
    out = assign_status(m, pd.DataFrame(columns=["model", "province", "window_end",
                                                 "mean_capture_rate", "status"]))
    assert out[out["model"] == "b"]["status"].iloc[0] == "candidate"


def test_recovery_from_retired_candidate():
    # "b" was retired-candidate but now beats the ratio again
    m = _metrics([
        ("a", "p1", "2026-09-05", 1.0, 100, 100),
        ("b", "p1", "2026-09-05", 0.75, 100, 100),
    ])
    prev = pd.DataFrame([
        {"model": "a", "province": "p1", "window_end": "2026-08-10",
         "mean_capture_rate": 1.0, "status": "champion"},
        {"model": "b", "province": "p1", "window_end": "2026-08-10",
         "mean_capture_rate": 0.5, "status": "retired-candidate"},
    ])
    out = assign_status(m, prev)
    assert out[out["model"] == "b"]["status"].iloc[0] == "candidate"
