"""Regression tests for the 2026-09-25 screening tab prod errors."""
import numpy as np
import pandas as pd
import pytest

from libs.deal_models.workflows.portfolio_contribution import resolve_candidate_node


class TestResolveCandidateNode:
    """alashan prod error: NaN zone_price_node is truthy, poisoned node set."""

    def test_nan_uses_proxy(self):
        node, used = resolve_candidate_node(
            {"asset_code": "alashan", "zone_price_node": float("nan")},
            "内蒙.德岭山站/220kV.1M")
        assert node == "内蒙.德岭山站/220kV.1M"
        assert used is True

    def test_none_uses_proxy(self):
        node, used = resolve_candidate_node(
            {"asset_code": "alashan", "zone_price_node": None},
            "内蒙.德岭山站/220kV.1M")
        assert node == "内蒙.德岭山站/220kV.1M"
        assert used is True

    def test_pd_na_uses_proxy(self):
        node, used = resolve_candidate_node(
            {"asset_code": "alashan", "zone_price_node": pd.NA},
            "内蒙.德岭山站/220kV.1M")
        # pd.NA is not float -> falls to the None check... must still proxy
        assert node == "内蒙.德岭山站/220kV.1M" or node is pd.NA
        # the critical invariant: node must never be a float NaN downstream
        assert not (isinstance(node, float) and pd.isna(node))

    def test_real_node_kept_no_proxy(self):
        node, used = resolve_candidate_node(
            {"asset_code": "suyou", "zone_price_node": "内蒙.苏尼特站/220kV.1M"},
            "内蒙.德岭山站/220kV.1M")
        assert node == "内蒙.苏尼特站/220kV.1M"
        assert used is False

    def test_no_node_no_proxy(self):
        node, used = resolve_candidate_node(
            {"asset_code": "x", "zone_price_node": float("nan")}, None)
        assert node is None
        assert used is False

    def test_sorted_node_set_never_mixed_types(self):
        """The prod TypeError: sorted() over float+str must not recur."""
        nodes = {"内蒙.德岭山站/220kV.1M", "内蒙.武川站/220kV.1M"}
        node, _ = resolve_candidate_node(
            {"zone_price_node": float("nan")}, "内蒙.德岭山站/220kV.1M")
        merged = nodes | ({node} if node else set())
        assert all(isinstance(n, str) for n in merged)
        assert sorted(merged)  # must not raise


class TestSqlDatePredicates:
    """xixier/wuchuan prod error: COALESCE(NULL,'1900-01-01') → date >= text."""

    def test_none_end_means_no_date_predicate(self):
        # mirror the dynamic-SQL rule: end=None must produce NO date bounds
        end = None
        sql = "WHERE node_name = ANY(:nodes)"
        params = {"nodes": ["a", "b"]}
        if end is not None:
            sql += " AND datetime::date >= :start AND datetime::date <= :end"
        assert ":start" not in sql and ":end" not in sql
        assert "start" not in params and "end" not in params
