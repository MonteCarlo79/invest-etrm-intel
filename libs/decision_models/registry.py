"""
libs/decision_models/registry.py

In-process registry for ModelSpec instances.
Models self-register at import time via `registry.register(spec)`.
Runners and adapters look up models by name (latest version) or name@version.

Introspection helpers
---------------------
registry.get_model_metadata(name)   -> metadata dict for one model
registry.describe_model(name)       -> full JSON-serialisable descriptor dict
registry.summarize()                -> list of descriptor dicts for all models
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from libs.decision_models.model_spec import ModelSpec


class ModelRegistry:
    """Thread-safe in-process registry of ModelSpec instances."""

    def __init__(self) -> None:
        self._store: Dict[str, ModelSpec] = {}  # key -> ModelSpec

    def register(self, spec: ModelSpec) -> ModelSpec:
        """Register a model. Raises if the same key is already registered."""
        if spec.key in self._store:
            raise ValueError(
                f"Model {spec.key!r} is already registered. "
                "Use a different version string or deregister first."
            )
        self._store[spec.key] = spec
        return spec

    def get(self, name: str, version: Optional[str] = None) -> ModelSpec:
        """
        Return a registered ModelSpec.

        If version is omitted, returns the latest registered version
        (lexicographic sort on version string — use semver-compatible strings).
        """
        if version is not None:
            key = f"{name}@{version}"
            if key not in self._store:
                raise KeyError(f"Model {key!r} not found in registry")
            return self._store[key]

        candidates = [s for s in self._store.values() if s.name == name]
        if not candidates:
            raise KeyError(f"No model named {name!r} found in registry")
        return sorted(candidates, key=lambda s: s.version)[-1]

    def list_models(self) -> List[ModelSpec]:
        return list(self._store.values())

    def get_model_metadata(self, name: str, version: Optional[str] = None) -> Dict[str, Any]:
        """Return the metadata dict for a registered model."""
        return self.get(name, version=version).metadata

    def describe_model(self, name: str, version: Optional[str] = None) -> Dict[str, Any]:
        """
        Return a JSON-serialisable descriptor dict for a registered model.

        Suitable for agent introspection, UI display, or logging.
        Does NOT include run_fn, input_schema, or output_schema (not serialisable).
        """
        spec = self.get(name, version=version)
        return {
            "name": spec.name,
            "version": spec.version,
            "key": spec.key,
            "description": spec.description,
            "tags": spec.tags,
            "has_run_fn": spec.run_fn is not None,
            "has_input_schema": spec.input_schema is not None,
            "has_output_schema": spec.output_schema is not None,
            "metadata": spec.metadata,
        }

    def summarize(self) -> List[Dict[str, Any]]:
        """
        Return a list of describe_model() dicts for all registered models,
        sorted by name then version.

        Useful for building model catalogues, agent system prompts, or UI dropdowns.
        """
        specs = sorted(self._store.values(), key=lambda s: (s.name, s.version))
        return [self.describe_model(s.name, s.version) for s in specs]

    def deregister(self, name: str, version: str) -> None:
        key = f"{name}@{version}"
        self._store.pop(key, None)

    def __contains__(self, key: str) -> bool:
        return key in self._store

    def __repr__(self) -> str:
        keys = list(self._store.keys())
        return f"ModelRegistry({keys})"


# Module-level singleton used across the platform
registry = ModelRegistry()
