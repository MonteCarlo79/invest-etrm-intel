# tests/apps/test_aggregate_capture.py
"""Aggregate capture display: SUM(realized)/SUM(theoretical) at both levels —
per-province (loader) and overall (KPI). NaN-realized days count 0 in the
numerator; NaN-theo days excluded from the denominator."""
from pathlib import Path

_APP = Path(__file__).resolve().parents[2] / "apps" / "bess-map" / "app.py"


def test_loader_sql_is_aggregate():
    src = _APP.read_text(encoding="utf-8")
    assert "SUM(COALESCE(NULLIF(realized_profit_per_mwh_day, 'NaN'::double precision), 0.0))" in src
    assert "NULLIF(SUM(NULLIF(theoretical_profit_per_mwh_day, 'NaN'::double precision)), 0)" in src
    assert "r.sum_real, r.sum_theo" in src
    # old mean-of-daily-rate formula must be gone from the ranking loader
    assert "AVG(NULLIF(capture_rate, 'NaN'::double precision)) * 100" not in src


def test_kpi_uses_overall_aggregate():
    src = _APP.read_text(encoding="utf-8")
    assert '_sr = rank_df["sum_real"].dropna().sum()' in src
    assert '_st = rank_df["sum_theo"].dropna().sum()' in src
    assert "f\"{_sr / _st * 100:.1f}%\"" in src
    assert "avg_cap.mean()" not in src


def test_label_updated_both_locales():
    src = _APP.read_text(encoding="utf-8")
    assert '"rank_kpi_capture":     "Aggregate Capture"' in src
    assert '"rank_kpi_capture":     "综合捕获率"' in src


def test_annual_real_filters_nan():
    """annual_real must average over evaluable days only — one NaN realized
    day (model warmup / forecast hole) poisons an unfiltered AVG and the
    arbitrage bar vanishes (2026-10-04 report)."""
    src = _APP.read_text(encoding="utf-8")
    assert "AVG(NULLIF(realized_profit_per_mwh_day, 'NaN'::double precision)) * 365" in src
    # the unfiltered form must not remain in the ranking loader
    assert "AVG(realized_profit_per_mwh_day)   * 365" not in src
