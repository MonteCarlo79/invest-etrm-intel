"""Structure guard for apps/market_maker: lazy radio pages, no st.tabs."""
import ast
from pathlib import Path

APP_DIR = Path(__file__).resolve().parents[2] / "apps" / "market_maker"
LABELS = ["Positions", "Exposure & MtM", "VaR & Greeks", "Curves", "Risk Agent"]


def _tree(name="app.py"):
    return ast.parse((APP_DIR / name).read_text(encoding="utf-8"))


def test_labels():
    labels = [ast.literal_eval(n.value) for n in ast.walk(_tree())
              if isinstance(n, ast.Assign) and any(getattr(t, "id", None) == "LABELS" for t in n.targets)]
    assert labels == [LABELS]


def _is_page_test(node):
    t = node.test
    return (isinstance(t, ast.Compare) and getattr(t.left, "id", None) == "page"
            and isinstance(t.ops[0], ast.Eq) and isinstance(t.comparators[0], ast.Constant))


def test_render_imports_inside_page_blocks():
    tree = _tree()
    parents = {}
    for p in ast.walk(tree):
        for c in ast.iter_child_nodes(p):
            parents[c] = p
    imports = [n for n in ast.walk(tree) if isinstance(n, ast.ImportFrom)
               and any(a.name.startswith("render_") for a in n.names)]
    assert len(imports) == len(LABELS)
    covered = set()
    for imp in imports:
        p = parents.get(imp)
        while p is not None and not (isinstance(p, ast.If) and _is_page_test(p)):
            p = parents.get(p)
        assert p is not None, f"render import at line {imp.lineno} not inside an `if page ==` block"
        covered.add(p.test.comparators[0].value)
    assert covered == set(LABELS)


def test_no_st_tabs():
    for f in APP_DIR.glob("*.py"):
        for n in ast.walk(ast.parse(f.read_text(encoding="utf-8"))):
            if isinstance(n, ast.Attribute) and n.attr == "tabs":
                raise AssertionError(f"st.tabs used in {f.name}:{n.lineno}")
