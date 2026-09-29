# tests/apps/test_spread_vol_label.py
"""Spread chart: annualised-vol bar labels (σ = std of daily avg-price
changes ÷ mean price × √365) wired into load_intraday_spread + the figure."""
import re
from pathlib import Path

_APP = Path(__file__).resolve().parents[2] / "apps" / "bess-map" / "app.py"


def test_ann_vol_in_spread_sql():
    src = _APP.read_text(encoding="utf-8")
    m = re.search(r"def load_intraday_spread.*?return pd\.read_sql", src, re.DOTALL)
    assert m, "load_intraday_spread not found"
    body = m.group(0)
    assert "STDDEV_SAMP(dp)" in body
    assert "SQRT(365)" in body
    assert "LAG(p) OVER (PARTITION BY province ORDER BY d)" in body
    assert "NULLIF(AVG(p), 0)" in body


def test_vol_label_wired_into_chart():
    src = _APP.read_text(encoding="utf-8")
    assert '"vol_label"' in src
    assert 'text="vol_label"' in src
    assert "σ {v*100:.0f}%" in src


def test_caption_explains_sigma_both_locales():
    src = _APP.read_text(encoding="utf-8")
    assert "annualised volatility" in src
    assert "年化波动率" in src
