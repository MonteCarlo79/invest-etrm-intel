"""Tests for revenue-weighted availability compute (workflows/availability)."""
import pandas as pd
import pytest

from libs.decision_models.workflows.availability import (
    compute_intervals, om_window_table, summarize, worst_intervals,
)

CALLED = 0.1
DT = 0.25


def _rows(rows):
    """rows: (asset, ts, nom, act, price)"""
    return pd.DataFrame(rows, columns=["asset_code", "interval_start",
                                       "nominated_dispatch_mw",
                                       "actual_dispatch_mw",
                                       "nodal_price_excel"])


def test_shortfall_and_delivery_math():
    df = _rows([
        ("a", "2026-04-01 10:00", 100.0, 80.0, 500.0),   # discharge shortfall 20MW
        ("a", "2026-04-01 10:15", -50.0, -40.0, -100.0),  # charge shortfall 10MW
        ("a", "2026-04-01 10:30", 0.0, 0.0, 600.0),       # idle — no shortfall
    ])
    out = compute_intervals(df)
    assert out.loc[0, "called"] and out.loc[1, "called"] and not out.loc[2, "called"]
    assert out.loc[0, "shortfall_mw"] == pytest.approx(20.0)
    assert out.loc[1, "shortfall_mw"] == pytest.approx(10.0)
    assert out.loc[2, "shortfall_mw"] == pytest.approx(0.0)
    # value rules: discharge at +500 → 500; charge at −100 → lost payment 100
    assert out.loc[0, "value_per_mwh"] == pytest.approx(500.0)
    assert out.loc[1, "value_per_mwh"] == pytest.approx(100.0)
    assert out.loc[2, "value_per_mwh"] == 0.0
    # foregone: 20×0.25×500=2500 ; 10×0.25×100=250 ; idle=0
    assert out.loc[0, "foregone_cny"] == pytest.approx(2500.0)
    assert out.loc[1, "foregone_cny"] == pytest.approx(250.0)
    assert out.loc[2, "foregone_cny"] == pytest.approx(0.0)


def test_charge_shortfall_at_positive_price_costs_nothing():
    df = _rows([("a", "2026-04-01 03:00", -50.0, -30.0, 300.0)])
    out = compute_intervals(df)
    # charging shortfall at positive price = avoided cost, not a loss
    assert out.loc[0, "value_per_mwh"] == 0.0
    assert out.loc[0, "foregone_cny"] == 0.0


def test_rwa_below_flat_when_shortfall_in_high_price_hours():
    df = _rows([
        ("a", "2026-04-01 19:00", 100.0, 50.0, 1000.0),  # 50% delivery at peak
        ("a", "2026-04-01 03:00", 100.0, 100.0, 100.0),   # perfect delivery at valley
    ])
    s = summarize(compute_intervals(df))
    r = s.iloc[0]
    assert r["flat_delivery"] == pytest.approx(0.75)
    # value_called = 100×.25×1000 + 100×.25×100 = 27500; foregone = 50×.25×1000 = 12500
    assert r["foregone_cny"] == pytest.approx(12500.0)
    assert r["rwa"] == pytest.approx(1 - 12500 / 27500)
    assert r["rwa"] < r["flat_delivery"]


def test_rwa_above_flat_when_shortfall_in_cheap_hours():
    df = _rows([
        ("a", "2026-04-01 19:00", 100.0, 100.0, 1000.0),  # perfect at peak
        ("a", "2026-04-01 03:00", 100.0, 50.0, 100.0),    # 50% at valley
    ])
    s = summarize(compute_intervals(df))
    r = s.iloc[0]
    assert r["flat_delivery"] == pytest.approx(0.75)
    assert r["rwa"] == pytest.approx(1 - (50 * DT * 100) / (100 * DT * 1000 + 100 * DT * 100))
    assert r["rwa"] > r["flat_delivery"]


def test_groupby_month():
    df = _rows([
        ("a", "2026-03-01 10:00", 100.0, 100.0, 500.0),
        ("a", "2026-04-01 10:00", 100.0, 50.0, 500.0),
        ("b", "2026-04-01 10:00", 50.0, 50.0, 400.0),
    ])
    d = compute_intervals(df)
    d["month"] = pd.to_datetime(d["interval_start"]).dt.strftime("%Y-%m")
    s = summarize(d, group_cols=("asset_code", "month"))
    assert len(s) == 3
    b = s[(s["asset_code"] == "b")].iloc[0]
    assert b["rwa"] == pytest.approx(1.0)


def test_worst_and_om_window():
    df = _rows([
        ("a", "2026-04-01 19:00", 100.0, 10.0, 1000.0),
        ("a", "2026-04-01 03:00", 100.0, 90.0, 100.0),
        ("a", "2026-04-01 04:00", 100.0, 95.0, 90.0),
    ])
    d = compute_intervals(df)
    w = worst_intervals(d, top_n=1)
    assert w.iloc[0]["foregone_cny"] == pytest.approx(90 * DT * 1000)
    om = om_window_table(d)
    assert set(om["hour"]) == {3, 4, 19}
    cheapest = om.sort_values("window_score").iloc[0]
    assert cheapest["hour"] == 4  # lowest nominated×value


def test_empty_guards():
    empty = _rows([]).astype({"nominated_dispatch_mw": float,
                              "actual_dispatch_mw": float, "nodal_price_excel": float})
    assert summarize(compute_intervals(empty)).empty
    assert worst_intervals(compute_intervals(empty)).empty
    assert om_window_table(compute_intervals(empty)).empty
