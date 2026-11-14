import ast
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]

EXPECTED = {
    "apps/asset_risk/app.py": [
        "Asset Config", "Settlement", "Realised P&L", "Agent",
        "Dispatch Diagnostics", "收益瀑布 P&L Waterfall",
    ],
    "apps/retail_risk/app.py": [
        "CRM", "Settlement", "Realised P&L", "Reconciliation",
        "Data Upload", "Agent",
    ],
}


def _tab_labels(src: str):
    for node in ast.walk(ast.parse(src)):
        if (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
                and node.func.attr == "tabs"):
            return [e.value for e in node.args[0].elts]
    raise AssertionError("no st.tabs call")


@pytest.mark.parametrize("rel", sorted(EXPECTED))
def test_tab_labels(rel):
    assert _tab_labels((ROOT / rel).read_text()) == EXPECTED[rel]


@pytest.mark.parametrize("rel", sorted(EXPECTED))
def test_no_position_or_var_refs(rel):
    src = (ROOT / rel).read_text()
    assert "tab_positions" not in src and "tab_var" not in src
