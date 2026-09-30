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
    assert "make_subplots" in src
    assert 'barmode="overlay"' in src                     # explicit-base waterfall, not px stack
    assert 'categoryorder="array"' in src
    assert 'autorange="reversed"' in src                  # largest NET at top
    assert "_load_installed_bess(_ENG_KEY)" in src
    # data-length labels preserved on the deduction segment
    assert "rank_days_unit" in src
    assert 'textposition="outside"' in src


def test_standard_station_conversion():
    src = _APP.read_text(encoding="utf-8")
    assert "scale = 100.0 * d / 1e4" in src               # per-MWh → 万元 for 100MW station
    assert '"万元/年"' in src
    assert "100MW/200MWh" in src or "100 MW" in src
    assert "* 200 / 1e4" in src and "* 400 / 1e4" in src  # KPI conversion
    assert "100MW/400MWh" in src


def test_i18n_keys_both_locales():
    src = _APP.read_text(encoding="utf-8")
    for key in ('"rank_stack_arb"', '"rank_stack_cap"', '"rank_stack_anc"'):
        assert src.count(key) >= 2, f"{key} missing from one locale dict"
    assert '"调频"' in src and '"容量电价"' in src


def test_table_stack_columns():
    src = _APP.read_text(encoding="utf-8")
    for col in ("CapPmt", "AncRev", "Total"):
        assert f'{{tag}} {col}' in src, f"table missing {col} column"


def test_unified_model_wired():
    src = _APP.read_text(encoding="utf-8")
    # curated sources
    for fn in ("load_cap_comp_latest", "load_fr_pool", "load_sysopfee_latest",
               "fr_component", "sysopfee_annual_cost_per_mwh", "STACKABLE_STATUSES"):
        assert fn in src, f"{fn} not wired"
    # sysopfee drawn as a NEGATIVE segment eating from the gross end (overlay base)
    assert "xs.append(-v)" in src
    assert "def _net(rec):" in src and '- rec.get("sys", 0.0)' in src
    # net totals in table + KPI
    assert "- sys_s).values" in src or "- sys_s.values" in src
    assert 'assign(net=_net2)' in src and 'assign(net=_net4)' in src
    # legacy exclusions still enforced
    assert "not in STACKABLE_STATUSES" in src
    # i18n key both locales
    assert src.count('"rank_stack_sys"') >= 3
