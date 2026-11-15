import ast
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]

ASSET_LABELS = [
    "Asset Config", "Settlement", "Realised P&L", "Agent",
    "Dispatch Diagnostics", "收益瀑布 P&L Waterfall",
]

EXPECTED = {
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


def _asset_tree():
    return ast.parse((ROOT / "apps/asset_risk/app.py").read_text())


def test_asset_risk_nav_labels():
    tree = _asset_tree()
    labels = None
    for node in tree.body:
        if (isinstance(node, ast.Assign) and len(node.targets) == 1
                and getattr(node.targets[0], "id", None) == "LABELS"):
            labels = [e.value for e in node.value.elts]
    assert labels == ASSET_LABELS


def test_asset_risk_lazy_pages():
    tree = _asset_tree()
    calls = [n.func.attr for n in ast.walk(tree)
             if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)
             and n.func.attr in ("radio", "tabs")]
    assert calls == ["radio"]
    # every render_* import must sit inside an `if page == ...` block
    guarded = set()
    for n in ast.walk(tree):
        if (isinstance(n, ast.If) and isinstance(n.test, ast.Compare)
                and isinstance(n.test.left, ast.Name) and n.test.left.id == "page"):
            for sub in ast.walk(n):
                guarded.add(id(sub))
    imports = [n for n in ast.walk(tree) if isinstance(n, ast.ImportFrom)
               and any(a.name.startswith("render_") for a in n.names)]
    assert len(imports) == 6
    assert all(id(i) in guarded for i in imports)


@pytest.mark.parametrize("rel", sorted(EXPECTED))
def test_no_position_or_var_refs(rel):
    src = (ROOT / rel).read_text()
    assert "tab_positions" not in src and "tab_var" not in src
