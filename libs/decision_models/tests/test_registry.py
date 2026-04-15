"""
libs/decision_models/tests/test_registry.py

Unit tests for ModelSpec and ModelRegistry.
Run with: pytest libs/decision_models/tests/
"""
import pytest

from libs.decision_models.model_spec import ModelSpec
from libs.decision_models.registry import ModelRegistry


def make_spec(name="test_model", version="1.0.0"):
    return ModelSpec(name=name, version=version, run_fn=lambda **kw: {})


class TestModelSpec:
    def test_key(self):
        spec = make_spec("foo", "2.0.0")
        assert spec.key == "foo@2.0.0"

    def test_empty_name_raises(self):
        with pytest.raises(ValueError):
            ModelSpec(name="", version="1.0.0")

    def test_empty_version_raises(self):
        with pytest.raises(ValueError):
            ModelSpec(name="foo", version="")


class TestModelRegistry:
    def test_register_and_get(self):
        reg = ModelRegistry()
        spec = make_spec()
        reg.register(spec)
        assert reg.get("test_model") is spec

    def test_duplicate_raises(self):
        reg = ModelRegistry()
        spec = make_spec()
        reg.register(spec)
        with pytest.raises(ValueError):
            reg.register(make_spec())

    def test_get_by_version(self):
        reg = ModelRegistry()
        s1 = make_spec(version="1.0.0")
        s2 = make_spec(version="2.0.0")
        reg.register(s1)
        reg.register(s2)
        assert reg.get("test_model", version="1.0.0") is s1
        assert reg.get("test_model") is s2  # latest

    def test_get_missing_raises(self):
        reg = ModelRegistry()
        with pytest.raises(KeyError):
            reg.get("nonexistent")

    def test_list_models(self):
        reg = ModelRegistry()
        reg.register(make_spec("a"))
        reg.register(make_spec("b"))
        assert len(reg.list_models()) == 2

    def test_deregister(self):
        reg = ModelRegistry()
        reg.register(make_spec())
        reg.deregister("test_model", "1.0.0")
        with pytest.raises(KeyError):
            reg.get("test_model")
