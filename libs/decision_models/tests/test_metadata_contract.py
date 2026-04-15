"""
libs/decision_models/tests/test_metadata_contract.py

Cross-model metadata contract tests.

These tests are the enforcement layer for the metadata standard defined in
libs/decision_models/model_spec.py::REQUIRED_METADATA_KEYS.

Every model registered in the library must:
  1. Expose all keys in REQUIRED_METADATA_KEYS
  2. Have JSON-serialisable metadata values
  3. Be introspectable via registry.describe_model() and registry.summarize()

When you add a new model to the library, these tests catch it automatically
on next test run — no per-model test file update needed.

Run:
    cd bess-platform
    pytest libs/decision_models/tests/test_metadata_contract.py -v
"""
from __future__ import annotations

import json

import pytest

# Trigger registration of all known models before running tests
import libs.decision_models.bess_dispatch_optimization           # noqa: F401
import libs.decision_models.bess_dispatch_simulation_multiday    # noqa: F401
import libs.decision_models.price_forecast_dayahead              # noqa: F401
import libs.decision_models.revenue_scenario_engine              # noqa: F401

from libs.decision_models.model_spec import REQUIRED_METADATA_KEYS
from libs.decision_models.registry import registry


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

ALL_MODEL_NAMES = [
    "bess_dispatch_optimization",
    "bess_dispatch_simulation_multiday",
    "price_forecast_dayahead",
    "revenue_scenario_engine",
]


def _get_all_specs():
    return [registry.get(name) for name in ALL_MODEL_NAMES]


# ---------------------------------------------------------------------------
# 1. REQUIRED_METADATA_KEYS contract
# ---------------------------------------------------------------------------

class TestRequiredMetadataKeys:
    """
    Parametrised over all registered models — each model must satisfy the
    contract independently. New models that import this test suite automatically
    become part of the compliance check.
    """

    @pytest.mark.parametrize("model_name", ALL_MODEL_NAMES)
    def test_all_required_keys_present(self, model_name):
        spec = registry.get(model_name)
        missing = REQUIRED_METADATA_KEYS - set(spec.metadata.keys())
        assert not missing, (
            f"Model {model_name!r} is missing required metadata keys: {sorted(missing)}\n"
            f"Present keys: {sorted(spec.metadata.keys())}"
        )

    @pytest.mark.parametrize("model_name", ALL_MODEL_NAMES)
    def test_metadata_json_serialisable(self, model_name):
        """All metadata must be JSON-serialisable (no numpy arrays, datetime objects, etc.)."""
        spec = registry.get(model_name)
        try:
            json.dumps(spec.metadata, default=str)
        except (TypeError, ValueError) as exc:
            pytest.fail(
                f"Model {model_name!r} metadata is not JSON-serialisable: {exc}"
            )

    @pytest.mark.parametrize("model_name", ALL_MODEL_NAMES)
    def test_category_is_valid(self, model_name):
        spec = registry.get(model_name)
        valid = {"optimization", "simulation", "forecast", "analytics"}
        cat = spec.metadata.get("category")
        assert cat in valid, (
            f"Model {model_name!r}: category={cat!r} not in {valid}"
        )

    @pytest.mark.parametrize("model_name", ALL_MODEL_NAMES)
    def test_granularity_is_valid(self, model_name):
        spec = registry.get(model_name)
        valid = {"hourly", "15min", "daily"}
        g = spec.metadata.get("granularity")
        assert g in valid, (
            f"Model {model_name!r}: granularity={g!r} not in {valid}"
        )

    @pytest.mark.parametrize("model_name", ALL_MODEL_NAMES)
    def test_status_is_valid(self, model_name):
        spec = registry.get(model_name)
        valid = {"production", "experimental"}
        s = spec.metadata.get("status")
        assert s in valid, (
            f"Model {model_name!r}: status={s!r} not in {valid}"
        )

    @pytest.mark.parametrize("model_name", ALL_MODEL_NAMES)
    def test_deterministic_is_bool(self, model_name):
        spec = registry.get(model_name)
        d = spec.metadata.get("deterministic")
        assert isinstance(d, bool), (
            f"Model {model_name!r}: deterministic={d!r} must be bool"
        )

    @pytest.mark.parametrize("model_name", ALL_MODEL_NAMES)
    def test_limitations_is_nonempty_list(self, model_name):
        spec = registry.get(model_name)
        lims = spec.metadata.get("limitations")
        assert isinstance(lims, list) and len(lims) > 0, (
            f"Model {model_name!r}: limitations must be a non-empty list, got {type(lims)}"
        )

    @pytest.mark.parametrize("model_name", ALL_MODEL_NAMES)
    def test_limitations_are_strings(self, model_name):
        spec = registry.get(model_name)
        for item in spec.metadata.get("limitations", []):
            assert isinstance(item, str), (
                f"Model {model_name!r}: all limitation entries must be str, got {type(item)!r}"
            )

    @pytest.mark.parametrize("model_name", ALL_MODEL_NAMES)
    def test_source_of_truth_module_is_string(self, model_name):
        spec = registry.get(model_name)
        s = spec.metadata.get("source_of_truth_module")
        assert isinstance(s, str) and s, (
            f"Model {model_name!r}: source_of_truth_module must be a non-empty string"
        )

    @pytest.mark.parametrize("model_name", ALL_MODEL_NAMES)
    def test_source_of_truth_functions_is_list(self, model_name):
        spec = registry.get(model_name)
        fns = spec.metadata.get("source_of_truth_functions")
        assert isinstance(fns, list) and len(fns) > 0, (
            f"Model {model_name!r}: source_of_truth_functions must be a non-empty list"
        )

    @pytest.mark.parametrize("model_name", ALL_MODEL_NAMES)
    def test_owner_is_string(self, model_name):
        spec = registry.get(model_name)
        o = spec.metadata.get("owner")
        assert isinstance(o, str) and o, (
            f"Model {model_name!r}: owner must be a non-empty string"
        )

    @pytest.mark.parametrize("model_name", ALL_MODEL_NAMES)
    def test_assumptions_present(self, model_name):
        spec = registry.get(model_name)
        a = spec.metadata.get("assumptions")
        assert a is not None, (
            f"Model {model_name!r}: assumptions must not be None (use dict or list)"
        )


# ---------------------------------------------------------------------------
# 2. Registry introspection
# ---------------------------------------------------------------------------

class TestRegistryIntrospection:
    def test_get_model_metadata_returns_dict(self):
        md = registry.get_model_metadata("bess_dispatch_optimization")
        assert isinstance(md, dict)

    def test_get_model_metadata_has_required_keys(self):
        md = registry.get_model_metadata("bess_dispatch_optimization")
        missing = REQUIRED_METADATA_KEYS - set(md.keys())
        assert not missing

    def test_describe_model_required_fields(self):
        desc = registry.describe_model("bess_dispatch_optimization")
        for field in ("name", "version", "key", "description", "tags",
                      "has_run_fn", "has_input_schema", "has_output_schema", "metadata"):
            assert field in desc, f"describe_model missing field: {field!r}"

    def test_describe_model_correct_values(self):
        desc = registry.describe_model("bess_dispatch_optimization")
        assert desc["name"] == "bess_dispatch_optimization"
        assert desc["has_run_fn"] is True
        assert desc["has_input_schema"] is True
        assert desc["has_output_schema"] is True

    def test_describe_model_json_serialisable(self):
        desc = registry.describe_model("revenue_scenario_engine")
        try:
            json.dumps(desc, default=str)
        except (TypeError, ValueError) as exc:
            pytest.fail(f"describe_model output not JSON-serialisable: {exc}")

    def test_summarize_returns_all_models(self):
        summary = registry.summarize()
        names = {d["name"] for d in summary}
        for name in ALL_MODEL_NAMES:
            assert name in names, f"summarize() missing model {name!r}"

    def test_summarize_sorted_by_name(self):
        summary = registry.summarize()
        names = [d["name"] for d in summary]
        assert names == sorted(names), "summarize() should be sorted by name"

    def test_summarize_json_serialisable(self):
        summary = registry.summarize()
        try:
            json.dumps(summary, default=str)
        except (TypeError, ValueError) as exc:
            pytest.fail(f"summarize() output not JSON-serialisable: {exc}")

    def test_get_model_metadata_unknown_raises(self):
        with pytest.raises(KeyError):
            registry.get_model_metadata("no_such_model")

    def test_describe_model_unknown_raises(self):
        with pytest.raises(KeyError):
            registry.describe_model("no_such_model")


# ---------------------------------------------------------------------------
# 3. Cross-model consistency checks
# ---------------------------------------------------------------------------

class TestCrossModelConsistency:
    def test_all_models_have_run_fn(self):
        for spec in _get_all_specs():
            assert spec.run_fn is not None, \
                f"Model {spec.name!r} has no run_fn"

    def test_all_models_have_input_schema(self):
        for spec in _get_all_specs():
            assert spec.input_schema is not None, \
                f"Model {spec.name!r} has no input_schema"

    def test_all_models_have_output_schema(self):
        for spec in _get_all_specs():
            assert spec.output_schema is not None, \
                f"Model {spec.name!r} has no output_schema"

    def test_all_models_have_tags(self):
        for spec in _get_all_specs():
            assert spec.tags, f"Model {spec.name!r} has empty tags list"

    def test_version_semver_format(self):
        """All versions must be parseable semver-compatible strings (x.y.z)."""
        for spec in _get_all_specs():
            parts = spec.version.split(".")
            assert len(parts) == 3, \
                f"Model {spec.name!r} version {spec.version!r} is not x.y.z format"
            for part in parts:
                # Each segment must start with a digit
                assert part and part[0].isdigit(), \
                    f"Model {spec.name!r} version segment {part!r} must start with a digit"

    def test_all_descriptions_nonempty(self):
        for spec in _get_all_specs():
            assert spec.description.strip(), \
                f"Model {spec.name!r} has empty description"

    def test_no_duplicate_keys_in_registry(self):
        """Each name@version key must be unique in the singleton registry."""
        keys = [spec.key for spec in registry.list_models()]
        assert len(keys) == len(set(keys)), f"Duplicate registry keys found: {keys}"
