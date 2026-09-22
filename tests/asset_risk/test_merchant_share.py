"""Tests for merchant-exposure bucketing and share math (tab_settlement)."""
import pandas as pd
import pytest

from apps.asset_risk.tab_settlement import _bucket_of, compute_merchant_shares


def _df(rows):
    return pd.DataFrame(rows, columns=["category", "amount_cny", "month"])


def test_bucket_mapping():
    assert _bucket_of("discharge_energy") == "merchant"
    assert _bucket_of("generation_revenue") == "merchant"
    assert _bucket_of("capacity_compensation") == "contract"
    assert _bucket_of("rebate") == "contract"
    assert _bucket_of("subsidy") == "contract"
    assert _bucket_of("other") == "contract"
    assert _bucket_of("frequency") == "services"
    assert _bucket_of("charge_energy") == "cost"
    assert _bucket_of("penalty") == "cost"
    assert _bucket_of("transmission") == "cost"
    assert _bucket_of("unknown_new_cat") == "cost"


def test_share_math_simple_month():
    items = _df([
        ("discharge_energy", 70.0, "2026-08"),
        ("capacity_compensation", 20.0, "2026-08"),
        ("frequency", 10.0, "2026-08"),
        ("charge_energy", -45.0, "2026-08"),   # cost line — excluded from gross
        ("transmission", -5.0, "2026-08"),      # cost line — excluded
    ])
    monthly, latest = compute_merchant_shares(items)
    assert latest["month"] == "2026-08"
    assert latest["gross"] == pytest.approx(100.0)
    assert latest["merchant_share"] == pytest.approx(0.70)


def test_chronological_order_and_latest():
    items = _df([
        ("discharge_energy", 50.0, "2026-07"),
        ("capacity_compensation", 50.0, "2026-07"),
        ("discharge_energy", 80.0, "2026-08"),
        ("capacity_compensation", 20.0, "2026-08"),
    ])
    monthly, latest = compute_merchant_shares(items)
    assert list(monthly["month"]) == ["2026-07", "2026-08"]
    assert latest["month"] == "2026-08"
    assert monthly.iloc[0]["merchant_share"] == pytest.approx(0.5)


def test_missing_buckets_zero_filled():
    items = _df([("discharge_energy", 100.0, "2026-08")])
    monthly, latest = compute_merchant_shares(items)
    assert latest["merchant_share"] == pytest.approx(1.0)
    assert latest["contract"] == 0.0
    assert latest["services"] == 0.0


def test_zero_gross_gives_none_share():
    items = _df([("charge_energy", -50.0, "2026-08"),
                 ("penalty", -5.0, "2026-08")])
    monthly, latest = compute_merchant_shares(items)
    assert latest is None or monthly["merchant_share"].isna().all()


def test_negative_bucket_month_does_not_inflate_gross():
    # contract bucket net-negative this month → clipped out of the denominator
    items = _df([
        ("discharge_energy", 100.0, "2026-08"),
        ("rebate", -30.0, "2026-08"),
    ])
    monthly, latest = compute_merchant_shares(items)
    assert latest["gross"] == pytest.approx(100.0)
    assert latest["merchant_share"] == pytest.approx(1.0)


def test_empty_input():
    monthly, latest = compute_merchant_shares(pd.DataFrame(
        columns=["category", "amount_cny", "month"]))
    assert monthly.empty and latest is None
