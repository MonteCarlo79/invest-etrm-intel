"""
libs/decision_models/registry.py

In-process registry for ModelSpec instances.
Models self-register at import time via `registry.register(spec)`.
Runners and adapters look up models by name (latest version) or name@version.
"""
from __future__ import annotations

from typing import Dict, List, Optional

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
