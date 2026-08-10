"""Tests for services/settlement_ingest/parser_discharge.py unit normalization."""
import pytest

from services.settlement_ingest.parser_discharge import (
    _volume_to_mwh, _price_to_cny_kwh, _normalize_units,
)


class TestUnitConverters:
    def test_volume_thousand_kwh_passthrough(self):
        assert _volume_to_mwh(274.71, "千千瓦时") == pytest.approx(274.71)

    def test_volume_kwh_divides(self):
        assert _volume_to_mwh(274710.0, "千瓦时") == pytest.approx(274.71)
        assert _volume_to_mwh(274710.0, "kWh") == pytest.approx(274.71)

    def test_volume_mwh_passthrough(self):
        assert _volume_to_mwh(274.71, "兆瓦时") == pytest.approx(274.71)
        assert _volume_to_mwh(274.71, "MWh") == pytest.approx(274.71)

    def test_volume_unknown_unit_defaults_mwh(self):
        assert _volume_to_mwh(274.71, "") == pytest.approx(274.71)
        assert _volume_to_mwh(274.71, None) == pytest.approx(274.71)

    def test_price_per_thousand_kwh_divides(self):
        assert _price_to_cny_kwh(349.0, "元/千千瓦时") == pytest.approx(0.349)

    def test_price_per_kwh_passthrough(self):
        assert _price_to_cny_kwh(0.349, "元/千瓦时") == pytest.approx(0.349)

    def test_price_unknown_unit_defaults_per_mwh(self):
        assert _price_to_cny_kwh(349.0, "") == pytest.approx(0.349)


class TestNormalizeUnits:
    def test_consistent_mwh_invoice_unchanged(self):
        # 117.68 MWh @ 349 CNY/MWh = ¥41,069.78 — the real 乌兰察布 2026-01 row
        vol, price, corrected = _normalize_units(117.68, "千千瓦时", 349.0, "元/千千瓦时", 41069.78)
        assert vol == pytest.approx(117.68)
        assert price == pytest.approx(0.349)
        assert corrected is False

    def test_kwh_invoice_normalized_by_stated_unit(self):
        # kWh-denominated invoice, units read correctly by vision
        vol, price, corrected = _normalize_units(274710.0, "千瓦时", 0.142, "元/千瓦时", 38879.87)
        assert vol == pytest.approx(274.71)
        assert price == pytest.approx(0.142)
        assert corrected is False

    def test_misread_unit_caught_by_crosscheck(self):
        # Vision claims 千千瓦时 but numbers only balance if volume was kWh:
        # 274710 "MWh" x 0.000142 CNY/kWh x 1000 = ¥39,008,820 ≈ 1000x the ¥38,879.87 amount
        vol, price, corrected = _normalize_units(274710.0, "千千瓦时", 142.0, "元/千千瓦时", 38879.87)
        assert vol == pytest.approx(274.71)
        assert price == pytest.approx(0.142)
        assert corrected is True

    def test_zero_amount_skips_crosscheck(self):
        vol, price, corrected = _normalize_units(100.0, "千千瓦时", 350.0, "元/千千瓦时", 0.0)
        assert vol == pytest.approx(100.0)
        assert corrected is False

    def test_price_misread_caught_by_crosscheck(self):
        # Price printed 0.142 元/千瓦时 but labeled 元/千千瓦时: stored 1000x too small.
        # Volume correct: 274.71 MWh. implied = 274.71 x 0.000142 x 1000 = ¥39 vs ¥38,880
        vol, price, corrected = _normalize_units(274.71, "千千瓦时", 0.142, "元/千千瓦时", 38879.87)
        assert vol == pytest.approx(274.71)          # volume untouched
        assert price == pytest.approx(0.142)          # price restored to CNY/kWh
        assert corrected is True

    def test_none_volume_passthrough(self):
        vol, price, corrected = _normalize_units(None, "", 350.0, "元/千千瓦时", 1000.0)
        assert vol is None
        assert corrected is False
