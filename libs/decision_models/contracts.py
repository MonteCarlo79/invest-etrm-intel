"""
libs/decision_models/contracts.py

Shared input/output contract helpers.

Conventions:
- Each model defines its own Input/Output TypedDicts (or dataclasses) in
  libs/decision_models/schemas/<model_name>.py
- This module provides base types and a lightweight validation helper so
  runners and adapters can enforce contracts without pulling in Pydantic
  as a hard dependency at the core layer.
"""
from __future__ import annotations

from typing import Any, Dict, Optional, Type


class ContractViolation(ValueError):
    """Raised when model input/output does not satisfy the declared schema."""


def validate_input(data: Dict[str, Any], schema: Optional[Type]) -> Dict[str, Any]:
    """
    Validate `data` against `schema`.

    If schema is a Pydantic BaseModel subclass, parse and re-dump as dict.
    If schema is a plain dict/TypedDict, no runtime validation (trust the caller).
    If schema is None, pass through unchanged.

    Returns a (possibly coerced) dict ready for the model's run_fn.
    """
    if schema is None:
        return data

    # Pydantic v1 / v2 path
    if _is_pydantic(schema):
        try:
            parsed = schema(**data)
            return _pydantic_dump(parsed)
        except Exception as exc:
            raise ContractViolation(f"Input validation failed: {exc}") from exc

    # Dataclass path
    if _is_dataclass(schema):
        try:
            instance = schema(**data)
            return instance.__dict__.copy()
        except Exception as exc:
            raise ContractViolation(f"Input validation failed: {exc}") from exc

    return data


def validate_output(data: Dict[str, Any], schema: Optional[Type]) -> Dict[str, Any]:
    """Mirror of validate_input for output contracts."""
    return validate_input(data, schema)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _is_pydantic(cls: Type) -> bool:
    try:
        from pydantic import BaseModel
        return issubclass(cls, BaseModel)
    except ImportError:
        return False


def _pydantic_dump(instance: Any) -> Dict[str, Any]:
    # pydantic v2
    if hasattr(instance, "model_dump"):
        return instance.model_dump()
    # pydantic v1
    return instance.dict()


def _is_dataclass(cls: Type) -> bool:
    import dataclasses
    return dataclasses.is_dataclass(cls)
