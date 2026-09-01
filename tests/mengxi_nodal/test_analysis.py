"""Tests for services.mengxi_nodal.analysis — pure nodal-analysis functions."""
from datetime import date

import numpy as np
import pytest

from services.mengxi_nodal.analysis import (
    assign_asset_clusters,
    cluster_day_prices,
    detect_split_days,
    price_match_fraction,
)
from services.mengxi_nodal.zones import CURRENT_ASSETS, ZONES


class TestPriceMatchFraction:
    def test_identical_vectors_score_one(self):
        v = np.array([100.0, -50.0, 300.25] * 32)
        assert price_match_fraction(v, v.copy()) == pytest.approx(1.0)

    def test_counts_matching_slots_within_tolerance(self):
        a = np.full(96, 100.0)
        b = np.full(96, 100.0)
        b[10] = 105.0   # beyond tolerance
        b[11] = 100.004  # within tolerance
        frac = price_match_fraction(a, b, tol=0.01)
        assert frac == pytest.approx(95 / 96)

    def test_nan_slots_are_excluded_not_mismatched(self):
        a = np.full(96, 100.0)
        b = np.full(96, 100.0)
        a[0] = np.nan
        b[1] = np.nan
        frac = price_match_fraction(a, b, tol=0.01)
        assert frac == pytest.approx(94 / 94)


class TestDetectSplitDays:
    def test_flags_only_divergent_days(self):
        days = [date(2026, 5, 15), date(2026, 5, 20)]
        asset = {days[0]: np.full(96, 100.0), days[1]: np.full(96, 120.0)}
        parent = {d: np.full(96, 100.0) for d in days}
        splits = detect_split_days(asset, parent, tol=0.01, min_match=0.99)
        assert splits == [date(2026, 5, 20)]

    def test_missing_parent_day_is_skipped(self):
        d = date(2026, 5, 15)
        asset = {d: np.full(96, 120.0)}
        assert detect_split_days(asset, {}, tol=0.01, min_match=0.99) == []


class TestClusterDayPrices:
    def test_groups_identical_vectors_and_sorts_by_size(self):
        matrix = {
            "n1": np.full(96, 100.0),
            "n2": np.full(96, 100.0),
            "n3": np.full(96, 200.0),
            "n4": np.full(96, 100.0),
            "n5": np.full(96, 300.0),
            "n6": np.full(96, 300.0),
        }
        clusters = cluster_day_prices(matrix, tol=0.01)
        sizes = [len(c) for c in clusters]
        assert sizes == [3, 2, 1]
        assert set(clusters[0]) == {"n1", "n2", "n4"}

    def test_near_identical_vectors_cluster_together(self):
        a = np.full(96, 100.0)
        b = np.full(96, 100.0)
        b[50] = 100.004
        matrix = {"a": a, "b": b, "c": np.full(96, 200.0)}
        clusters = cluster_day_prices(matrix, tol=0.01)
        assert [len(c) for c in clusters] == [2, 1]


class TestAssignAssetClusters:
    def test_asset_assigned_to_cluster_with_matching_vector(self):
        matrix = {
            "n1": np.full(96, 100.0),
            "n2": np.full(96, 100.0),
            "n3": np.full(96, 200.0),
        }
        assets = {"bess_a": np.full(96, 200.0), "bess_b": np.full(96, 100.0)}
        result = assign_asset_clusters(matrix, assets, tol=0.01)
        assert result["bess_a"] == ["n3"]
        assert set(result["bess_b"]) == {"n1", "n2"}

    def test_unmatched_asset_gets_empty(self):
        matrix = {"n1": np.full(96, 100.0)}
        result = assign_asset_clusters(matrix, {"bess_a": np.full(96, 999.0)}, tol=0.01)
        assert result["bess_a"] == []


class TestZonesConfig:
    def test_all_six_current_assets_present_with_substation(self):
        assert len(CURRENT_ASSETS) == 6
        for a in CURRENT_ASSETS:
            assert a["substation"], a

    def test_siziwangqi_has_no_own_node_others_do(self):
        by_code = {a["asset_code"]: a for a in CURRENT_ASSETS}
        assert by_code["siziwangqi"]["own_node"] is None
        for code in ("suyou", "hangjinqi", "gushanliang", "bameng", "wulate"):
            assert by_code[code]["own_node"], code

    def test_every_zone_references_known_asset(self):
        codes = {a["asset_code"] for a in CURRENT_ASSETS}
        for z in ZONES:
            for ref in z["our_assets"]:
                assert ref in codes, (z, ref)
