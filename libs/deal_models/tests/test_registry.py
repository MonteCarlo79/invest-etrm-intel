from __future__ import annotations
import pytest
import numpy as np


def test_register_and_get():
    from libs.deal_models.registry import DealStructureSpec, register, get, list_structures
    from pydantic import BaseModel

    class P(BaseModel):
        x: float

    spec = DealStructureSpec(
        name="_test_struct",
        description="test",
        payoff_fn=lambda rev, p: rev - p.x,
        params_schema=P,
    )
    register(spec)
    assert get("_test_struct").name == "_test_struct"
    assert "_test_struct" in list_structures()


def test_get_unknown_raises():
    from libs.deal_models.registry import get
    with pytest.raises(KeyError):
        get("__nonexistent__")


def test_register_duplicate_raises():
    from libs.deal_models.registry import DealStructureSpec, register
    from pydantic import BaseModel

    class P(BaseModel):
        y: float

    spec = DealStructureSpec(name="_dup_test", description="d", payoff_fn=lambda r, p: r, params_schema=P)
    register(spec)
    with pytest.raises(ValueError, match="_dup_test"):
        register(spec)
