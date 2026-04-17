"""
libs/decision_models/tests/test_catalogue.py

Smoke tests for the catalogue page data layer.

Does NOT import Streamlit — tests only the pure-Python functions:
    load_catalogue(), apply_filters(), fmt()

These tests verify that the catalogue page can build its data without error,
and that every model exposes the fields the page depends on.

Run:
    cd bess-platform
    pytest libs/decision_models/tests/test_catalogue.py -v
"""
from __future__ import annotations

import pytest

# Trigger registrations (same pattern as other model test files)
import libs.decision_models.bess_dispatch_optimization           # noqa: F401
import libs.decision_models.bess_dispatch_simulation_multiday    # noqa: F401
import libs.decision_models.price_forecast_dayahead              # noqa: F401
import libs.decision_models.revenue_scenario_engine              # noqa: F401

from libs.decision_models.adapters.app.catalogue_page import (
    apply_filters,
    fmt,
    load_catalogue,
)
from libs.decision_models.model_spec import REQUIRED_METADATA_KEYS

_EXPECTED_MODEL_NAMES = {
    "bess_dispatch_optimization",
    "bess_dispatch_simulation_multiday",
    "price_forecast_dayahead",
    "revenue_scenario_engine",
}

# Fields the card renderer reads directly — these must all be present
_CARD_FIELDS = [
    "category",
    "scope",
    "granularity",
    "horizon",
    "deterministic",
    "model_family",
    "market",
    "asset_type",
    "status",
    "owner",
    "source_of_truth_module",
    "source_of_truth_functions",
    "fallback_behavior",
    "limitations",
    "assumptions",
]


# ---------------------------------------------------------------------------
# load_catalogue()
# ---------------------------------------------------------------------------

class TestLoadCatalogue:
    def test_returns_list(self):
        models = load_catalogue()
        assert isinstance(models, list)

    def test_all_expected_models_present(self):
        models = load_catalogue()
        names = {m["name"] for m in models}
        assert _EXPECTED_MODEL_NAMES <= names, (
            f"Missing models in catalogue: {_EXPECTED_MODEL_NAMES - names}"
        )

    def test_each_entry_has_describe_model_shape(self):
        """Each entry must have the fields describe_model() returns."""
        models = load_catalogue()
        required_top_keys = {"name", "version", "key", "description", "tags",
                             "has_run_fn", "has_input_schema", "has_output_schema",
                             "metadata"}
        for m in models:
            missing = required_top_keys - set(m.keys())
            assert not missing, f"Model {m.get('name')!r} entry missing keys: {missing}"

    def test_each_metadata_has_required_contract_keys(self):
        """All REQUIRED_METADATA_KEYS must be present in every model's metadata."""
        models = load_catalogue()
        for m in models:
            missing = REQUIRED_METADATA_KEYS - set(m["metadata"].keys())
            assert not missing, (
                f"Model {m['name']!r} missing required metadata keys: {sorted(missing)}"
            )

    def test_card_fields_present_in_all_models(self):
        """Every field the card renderer reads must exist in each model's metadata."""
        models = load_catalogue()
        for m in models:
            for field in _CARD_FIELDS:
                assert field in m["metadata"], (
                    f"Model {m['name']!r} missing card field: {field!r}"
                )

    def test_sorted_by_name(self):
        models = load_catalogue()
        names = [m["name"] for m in models]
        assert names == sorted(names), "load_catalogue() must return models sorted by name"

    def test_idempotent(self):
        """Calling load_catalogue() twice returns the same data."""
        models1 = load_catalogue()
        models2 = load_catalogue()
        assert [m["name"] for m in models1] == [m["name"] for m in models2]

    def test_all_have_run_fn(self):
        models = load_catalogue()
        for m in models:
            assert m["has_run_fn"] is True, f"Model {m['name']!r} has no run_fn"

    def test_limitations_are_nonempty_lists(self):
        models = load_catalogue()
        for m in models:
            lims = m["metadata"].get("limitations")
            assert isinstance(lims, list) and len(lims) > 0, (
                f"Model {m['name']!r} limitations must be a non-empty list"
            )

    def test_source_of_truth_functions_are_nonempty_lists(self):
        models = load_catalogue()
        for m in models:
            fns = m["metadata"].get("source_of_truth_functions")
            assert isinstance(fns, list) and len(fns) > 0, (
                f"Model {m['name']!r} source_of_truth_functions must be a non-empty list"
            )


# ---------------------------------------------------------------------------
# apply_filters()
# ---------------------------------------------------------------------------

class TestApplyFilters:
    def setup_method(self):
        self.models = load_catalogue()

    def test_no_filters_returns_all(self):
        result = apply_filters(self.models, [], [], [])
        assert len(result) == len(self.models)

    def test_filter_by_category_optimization(self):
        result = apply_filters(self.models, ["optimization"], [], [])
        assert all(m["metadata"]["category"] == "optimization" for m in result)
        assert len(result) >= 1

    def test_filter_by_category_analytics(self):
        result = apply_filters(self.models, ["analytics"], [], [])
        assert all(m["metadata"]["category"] == "analytics" for m in result)
        assert len(result) >= 1

    def test_filter_by_status_production(self):
        result = apply_filters(self.models, [], [], ["production"])
        assert all(m["metadata"]["status"] == "production" for m in result)
        assert len(result) == len(self.models)  # all are production

    def test_filter_by_market_mengxi(self):
        result = apply_filters(self.models, [], ["mengxi"], [])
        assert all(m["metadata"].get("market") == "mengxi" for m in result)
        assert len(result) == 1  # only revenue_scenario_engine

    def test_filter_by_market_dash_matches_none(self):
        result = apply_filters(self.models, [], ["—"], [])
        assert all(m["metadata"].get("market") is None for m in result)

    def test_filter_combined_returns_subset(self):
        result = apply_filters(self.models, ["optimization", "simulation"], [], ["production"])
        assert len(result) <= len(self.models)
        for m in result:
            assert m["metadata"]["category"] in ("optimization", "simulation")
            assert m["metadata"]["status"] == "production"

    def test_impossible_filter_returns_empty(self):
        result = apply_filters(self.models, ["optimization"], ["mengxi"], [])
        # revenue_scenario_engine is mengxi but analytics, not optimization
        assert len(result) == 0

    def test_filter_does_not_mutate_original(self):
        original_len = len(self.models)
        apply_filters(self.models, ["optimization"], [], [])
        assert len(self.models) == original_len


# ---------------------------------------------------------------------------
# fmt()
# ---------------------------------------------------------------------------

class TestFmt:
    def test_none_returns_dash(self):
        assert fmt(None) == "—"

    def test_empty_string_returns_dash(self):
        assert fmt("") == "—"

    def test_bool_true(self):
        assert fmt(True) == "yes"

    def test_bool_false(self):
        assert fmt(False) == "no"

    def test_list_joined(self):
        assert fmt(["a", "b", "c"]) == "a, b, c"

    def test_empty_list_via_none_label(self):
        # fmt([]) returns "" joined which is ""
        assert fmt([]) == ""

    def test_string_passthrough(self):
        assert fmt("production") == "production"

    def test_int_passthrough(self):
        assert fmt(42) == "42"

    def test_custom_none_label(self):
        assert fmt(None, none_label="N/A") == "N/A"
