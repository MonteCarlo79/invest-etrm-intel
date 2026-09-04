import numpy as np

from services.deal_committee.charts import (
    chart_irr_distribution, chart_monthly_price, chart_revenue_distribution,
)

PNG_MAGIC = b"\x89PNG"


def test_chart_monthly_price():
    png = chart_monthly_price([(f"2026-{m:02d}", 280.0 + m * 5) for m in range(1, 13)])
    assert png.startswith(PNG_MAGIC) and len(png) > 5_000


def test_chart_revenue_distribution():
    png = chart_revenue_distribution(np.random.default_rng(1).normal(1e8, 2e7, 500))
    assert png.startswith(PNG_MAGIC) and len(png) > 5_000


def test_chart_irr_distribution():
    png = chart_irr_distribution(np.random.default_rng(2).normal(0.09, 0.03, 500),
                                 hurdle_rate=0.08)
    assert png.startswith(PNG_MAGIC) and len(png) > 5_000


def test_empty_input_raises_valueerror():
    import pytest
    with pytest.raises(ValueError):
        chart_monthly_price([])
