"""
libs/decision_models/model_spec.py

ModelSpec is the single descriptor for every decision model in this library.
It carries identity, version, I/O schema pointers, and capability tags — enough
for the registry to index it and runners/adapters to invoke it safely.

REQUIRED_METADATA_KEYS defines the metadata contract that all registered model
assets must satisfy. Compliance is verified by tests/test_metadata_contract.py.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Dict, FrozenSet, List, Optional, Type


# ---------------------------------------------------------------------------
# Metadata contract
#
# Every model registered in the library must include these keys in its
# ModelSpec.metadata dict. Values may be None where the concept does not
# apply (e.g. market=None for market-agnostic models), but the key must
# be present so apps and agents can introspect without guard clauses.
#
# Key glossary
# ------------
# category             : "optimization" | "simulation" | "forecast" | "analytics"
# scope                : coarse-grain input scope, e.g. "single_day", "multi_day",
#                        "province_level", "asset_level"
# market               : target market context, e.g. "mengxi", or None if agnostic
# asset_type           : "bess" | "wind" | "solar" | ...
# granularity          : time granularity of inputs/outputs: "hourly" | "15min" | "daily"
# horizon              : time horizon: "single_day" | "multi_day" | "day_ahead" | "historical"
# deterministic        : bool — True if same inputs always produce the same output
# model_family         : implementation family: "lp_milp" | "ols" | "identity" | "rule_based"
# source_of_truth_module   : file path relative to repo root (str)
# source_of_truth_functions: list of function names that implement the core logic (list[str])
# assumptions          : machine-readable dict or list of assumptions
# limitations          : list of known limitation strings
# fallback_behavior    : description of any fallback, or None
# status               : "production" | "experimental"
# owner                : team / system owner (str)
# ---------------------------------------------------------------------------
REQUIRED_METADATA_KEYS: FrozenSet[str] = frozenset({
    "category",
    "scope",
    "market",
    "asset_type",
    "granularity",
    "horizon",
    "deterministic",
    "model_family",
    "source_of_truth_module",
    "source_of_truth_functions",
    "assumptions",
    "limitations",
    "fallback_behavior",
    "status",
    "owner",
})


@dataclass
class ModelSpec:
    """Descriptor for a reusable decision model."""

    # --- Identity ---
    name: str                          # stable slug, e.g. "bess_dispatch_optimization"
    version: str                       # semver string, e.g. "1.0.0"
    description: str = ""

    # --- I/O contracts ---
    # Either a Pydantic model class or a TypedDict / dataclass accepted as input
    input_schema: Optional[Type] = None
    output_schema: Optional[Type] = None

    # --- Entrypoint ---
    # Callable(input_dict) -> output_dict; populated by the model module at import time
    run_fn: Optional[Callable[..., Any]] = None

    # --- Metadata ---
    tags: List[str] = field(default_factory=list)
    # See REQUIRED_METADATA_KEYS for the full standard. Example:
    # {"category": "optimization", "asset_type": "bess", "source_of_truth_module": "services/bess_map/optimisation_engine.py", ...}
    metadata: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.name:
            raise ValueError("ModelSpec.name must not be empty")
        if not self.version:
            raise ValueError("ModelSpec.version must not be empty")

    @property
    def key(self) -> str:
        """Unique registry key: name@version."""
        return f"{self.name}@{self.version}"

    def __repr__(self) -> str:
        return f"ModelSpec(name={self.name!r}, version={self.version!r}, tags={self.tags})"
