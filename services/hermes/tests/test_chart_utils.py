"""Tests for chart_utils spot query param handling — multi-province support.

Regression: after the Bedrock model swap (haiku → sonnet), the extractor
returns multiple provinces as a list or comma-joined string. The old code
crashed (list → unhashable TypeError) or matched zero rows (joined string
used in an equality filter), producing a false "无数据" error even though
spot_daily holds data for every requested province.
"""
from unittest.mock import patch

import pandas as pd
import pytest

from services.hermes.chart_utils import _normalize_provinces, fetch_spot_dataframe


class TestNormalizeProvinces:
    def test_none_returns_none(self):
        assert _normalize_provinces(None) is None

    def test_single_province_string(self):
        assert _normalize_provinces("山东") == ["山东"]

    def test_comma_joined_string_splits(self):
        assert _normalize_provinces("江苏,上海,广东,山东,蒙西") == [
            "江苏", "上海", "广东", "山东", "蒙西",
        ]

    def test_chinese_enumeration_comma_splits(self):
        assert _normalize_provinces("江苏、上海") == ["江苏", "上海"]

    def test_list_passthrough(self):
        assert _normalize_provinces(["江苏", "上海"]) == ["江苏", "上海"]

    def test_alias_mapping_applied_per_item(self):
        assert _normalize_provinces("内蒙古") == ["蒙西"]
        assert _normalize_provinces(["mengxi", "shandong"]) == ["蒙西", "山东"]

    def test_empty_returns_none(self):
        assert _normalize_provinces("") is None
        assert _normalize_provinces([]) is None


def _run_fetch(params, captured):
    """Run fetch_spot_dataframe with the LLM parser and DB layer mocked."""
    def fake_read_sql(sql, conn, params=None):
        captured["sql"] = sql
        captured["params"] = params
        return pd.DataFrame({
            "report_date": ["2026-08-16"],
            "province_cn": ["江苏"],
            "rt_avg": [0.4],
        })

    with patch("services.hermes.chart_utils._parse_spot_query_params",
               return_value=params), \
         patch("psycopg2.connect"), \
         patch("pandas.read_sql_query", side_effect=fake_read_sql):
        return fetch_spot_dataframe("q", "key", "pg://x")


class TestFetchSpotDataframe:
    _BASE = {"start_date": "2026-08-16", "end_date": "2026-08-30",
             "metrics": ["rt_avg"]}

    def test_multi_province_list_uses_any(self):
        captured = {}
        df, provinces, *_ = _run_fetch(
            {**self._BASE, "provinces_cn": ["江苏", "上海", "广东", "山东", "蒙西"]},
            captured,
        )
        assert "ANY" in captured["sql"]
        assert captured["params"][2] == ["江苏", "上海", "广东", "山东", "蒙西"]
        assert provinces == ["江苏", "上海", "广东", "山东", "蒙西"]

    def test_comma_joined_string_splits_and_uses_any(self):
        captured = {}
        df, provinces, *_ = _run_fetch(
            {**self._BASE, "province_cn": "江苏,上海"}, captured,
        )
        assert "ANY" in captured["sql"]
        assert captured["params"][2] == ["江苏", "上海"]
        assert provinces == ["江苏", "上海"]

    def test_single_province_keeps_equality_filter(self):
        captured = {}
        df, provinces, *_ = _run_fetch(
            {**self._BASE, "province_cn": "山东"}, captured,
        )
        assert "= %s" in captured["sql"]
        assert "ANY" not in captured["sql"]
        assert captured["params"][2] == "山东"
        assert provinces == ["山东"]

    def test_no_province_queries_all(self):
        captured = {}
        df, provinces, *_ = _run_fetch(
            {**self._BASE, "province_cn": None}, captured,
        )
        assert "province_cn =" not in captured["sql"]
        assert provinces is None
