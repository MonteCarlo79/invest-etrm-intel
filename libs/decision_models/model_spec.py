"""
libs/decision_models/model_spec.py

ModelSpec is the single descriptor for every decision model in this library.
It carries identity, version, I/O schema pointers, and capability tags — enough
for the registry to index it and runners/adapters to invoke it safely.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Type


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
    # e.g. {"asset_type": "bess", "market": "mengxi", "source_module": "services/bess_map/storage_optimisation.py"}
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
