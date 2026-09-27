# tests/apps/test_ranking_exclusions.py
"""Guard the Province Ranking exclusion list (CJK entity names — a silent typo
would re-admit stale sub-provincial regions) and its wiring in the tab."""
import ast
from pathlib import Path

_APP = Path(__file__).resolve().parents[2] / "apps" / "bess-map" / "app.py"

_EXPECTED = {"海南礼记", "海南那悦", "豫南", "豫中东", "豫北", "豫西"}


def _exclusion_set():
    tree = ast.parse(_APP.read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign) and any(
            getattr(t, "id", "") == "_RANKING_EXCLUDE" for t in node.targets
        ):
            return ast.literal_eval(node.value)
    raise AssertionError("_RANKING_EXCLUDE assignment not found in app.py")


def test_ranking_exclude_entities_exact():
    assert _exclusion_set() == _EXPECTED


def test_exclusion_wired_in_tab():
    src = _APP.read_text(encoding="utf-8")
    assert 'rank_df[~rank_df["province"].isin(_RANKING_EXCLUDE)]' in src
    assert 'spread_df[~spread_df["province"].isin(_RANKING_EXCLUDE)]' in src


def test_days_label_uses_i18n_unit():
    src = _APP.read_text(encoding="utf-8")
    assert "_t('rank_days_unit')" in src
    assert '"rank_days_unit"' in src  # defined in both i18n dicts
