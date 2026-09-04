"""Static regression test: no module-level call may precede its def.

Streamlit scripts execute top-to-bottom on every rerun, so any function
called at module level (including inside `with tab_x:` blocks) must be
defined above its first call site. This test exists because the v60 IRR
work hoisted `_sof_df = load_sysopfee(_ENG_KEY)` above the
`def load_sysopfee` definition, crashing the whole app with a NameError
in production.
"""

import ast
from pathlib import Path

APP_PY = Path(__file__).resolve().parents[1] / "app.py"


def _module_level_calls_and_defs(tree: ast.Module):
    """Return (defs, first_calls) for module-level code.

    defs:        {name: lineno} for `def` statements directly at module level.
    first_calls: {name: min lineno} of Call nodes with a bare-Name func,
                 descending into statements (if/with/try/assign/expr) but
                 NOT into function/class bodies (those run at call time).
    """
    defs = {}
    first_calls = {}

    def visit(node):
        for child in ast.iter_child_nodes(node):
            if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                continue  # body executes later, not in module order
            if isinstance(child, ast.Call) and isinstance(child.func, ast.Name):
                name = child.func.id
                if name not in first_calls or child.lineno < first_calls[name]:
                    first_calls[name] = child.lineno
            visit(child)

    for stmt in tree.body:
        if isinstance(stmt, ast.FunctionDef):
            defs.setdefault(stmt.name, stmt.lineno)
        else:
            visit(stmt)
    return defs, first_calls


def test_no_module_level_call_before_def():
    tree = ast.parse(APP_PY.read_text(encoding="utf-8"), filename=str(APP_PY))
    defs, first_calls = _module_level_calls_and_defs(tree)

    violations = [
        f"{name}: called at line {first_calls[name]} but defined at line {defs[name]}"
        for name in sorted(set(defs) & set(first_calls))
        if defs[name] > first_calls[name]
    ]
    assert not violations, (
        "Module-level call(s) precede their def (Streamlit NameError):\n"
        + "\n".join(violations)
    )


# Shared DataFrames loaded once near the top of app.py and consumed by
# multiple tabs (geo map overlay, aux tab, IRR tab). Tab sections must never
# reuse these names for display frames — v60's hoist turned that shadowing
# into a cross-tab corruption (KeyError: 'province').
_SHARED_FRAMES = {"_sof_df", "_cc_df", "_fr_df"}


def test_shared_frames_never_reassigned():
    tree = ast.parse(APP_PY.read_text(encoding="utf-8"), filename=str(APP_PY))
    assignments = {name: [] for name in _SHARED_FRAMES}

    def visit(node):
        for child in ast.iter_child_nodes(node):
            if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                continue
            if isinstance(child, ast.Name) and isinstance(child.ctx, ast.Store):
                if child.id in assignments:
                    assignments[child.id].append(child.lineno)
            visit(child)

    for stmt in tree.body:
        if not isinstance(stmt, ast.FunctionDef):
            visit(stmt)

    violations = [
        f"{name}: assigned at lines {lines} (must be assigned exactly once)"
        for name, lines in sorted(assignments.items())
        if len(lines) > 1
    ]
    assert not violations, (
        "Shared DataFrame name(s) reassigned at module level:\n" + "\n".join(violations)
    )
