# tests/apps/test_capacity_stack_chart.py
"""Ranking-chart capacity/调频 stack wiring (app.py source checks — the
formula math itself is covered in tests/bess_map/test_capacity_stack.py)."""
from pathlib import Path

_APP = Path(__file__).resolve().parents[2] / "apps" / "bess-map" / "app.py"


def test_stack_imports_and_helpers():
    src = _APP.read_text(encoding="utf-8")
    assert "from services.bess_map.capacity_stack import (" in src
    for name in ("capacity_payment_per_mwh_yr", "capacity_stack_map",
                 "ancillary_per_mwh_yr", "load_capacity_rows", "load_ancillary_annual"):
        assert name in src
    assert "def _cap_for(prov: str, d: float)" in src
    assert "def _anc_for(prov: str, d: float)" in src


def test_stack_chart_structure():
    src = _APP.read_text(encoding="utf-8")
    assert 'barmode="stack"' in src
    assert 'facet_row="Duration"' in src
    assert "_load_installed_bess(_ENG_KEY)" in src
    # data-length labels preserved on the stacked chart
    assert "rank_days_unit" in src
    assert 'text="label"' in src


def test_i18n_keys_both_locales():
    src = _APP.read_text(encoding="utf-8")
    for key in ('"rank_stack_arb"', '"rank_stack_cap"', '"rank_stack_anc"'):
        assert src.count(key) >= 2, f"{key} missing from one locale dict"
    assert '"调频"' in src and '"容量电价"' in src


def test_table_stack_columns():
    src = _APP.read_text(encoding="utf-8")
    for col in ("CapPmt", "AncRev", "Total"):
        assert f'{{tag}} {col}' in src, f"table missing {col} column"
