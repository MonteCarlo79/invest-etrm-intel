"""
libs/decision_models/runners/local.py

Synchronous in-process runner.  Used by Streamlit pages, CLI scripts,
and unit tests — anywhere you want to call a registered model directly.
"""
from __future__ import annotations

from typing import Any, Dict, Optional

from libs.decision_models.contracts import validate_input, validate_output
from libs.decision_models.registry import registry


def run(
    model_name: str,
    inputs: Dict[str, Any],
    version: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Look up `model_name` in the registry, validate inputs, run, validate outputs.

    Args:
        model_name: registered model slug
        inputs:     raw input dict (will be validated against spec.input_schema)
        version:    optional pinned version; defaults to latest

    Returns:
        output dict validated against spec.output_schema
    """
    spec = registry.get(model_name, version=version)

    if spec.run_fn is None:
        raise RuntimeError(
            f"Model {spec.key!r} has no run_fn registered. "
            "Ensure the model module has been imported."
        )

    validated_inputs = validate_input(inputs, spec.input_schema)
    raw_outputs = spec.run_fn(**validated_inputs)

    if not isinstance(raw_outputs, dict):
        raw_outputs = {"result": raw_outputs}

    return validate_output(raw_outputs, spec.output_schema)
