"""libs/deal_models/registry.py — Registry for DealStructureSpec instances."""
from __future__ import annotations
from dataclasses import dataclass
from typing import Callable, Dict, List, Type
from pydantic import BaseModel

_REGISTRY: Dict[str, "DealStructureSpec"] = {}


@dataclass
class DealStructureSpec:
    name: str
    description: str
    payoff_fn: Callable   # (revenue_paths: np.ndarray, params: BaseModel) -> np.ndarray
    params_schema: Type[BaseModel]


def register(spec: DealStructureSpec) -> DealStructureSpec:
    if spec.name in _REGISTRY:
        raise ValueError(f"{spec.name!r} is already registered. Use a different name.")
    _REGISTRY[spec.name] = spec
    return spec


def get(name: str) -> DealStructureSpec:
    if name not in _REGISTRY:
        raise KeyError(f"{name!r} not in registry. Available: {list(_REGISTRY)}")
    return _REGISTRY[name]


def list_structures() -> List[str]:
    return list(_REGISTRY.keys())
