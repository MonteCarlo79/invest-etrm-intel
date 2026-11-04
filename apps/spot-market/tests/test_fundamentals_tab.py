# apps/spot-market/tests/test_fundamentals_tab.py
"""Market Fundamentals tab rebuild (province_fundamentals_monthly) — tests.

Covers the task-6 brief acceptance points:
  (a) loaders handle empty results / DB failure without crashing;
  (b) module-level execution-order guard (v60 lesson) scoped to the
      fundamentals-tab region of app.py: no module-level call in that region
      may invoke a `def` that appears later in the file;
  pure data-shaping helpers (scope pick / annual / monthly / growth) so the
  口径 and yoy-unit rules are pinned down;
  AppTest smoke of fundamentals_tab.render with stubbed loaders (empty and
  populated), proving the visual layer renders from empty data without
  crashing.
"""
import ast
import os
import sys
import tempfile
from datetime import date
from pathlib import Path

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

import fundamentals_tab as ft  # noqa: E402

APP_PY = Path(__file__).resolve().parents[1] / "app.py"
REPO_ROOT = Path(__file__).resolve().parents[3]


def _row(metric, value, *, period="year", month=date(2025, 1, 1),
         unit="万千瓦", section="capacity"):
    return {"province": "安徽", "month": month, "period": period,
            "section": section, "metric": metric, "value": value,
            "unit": unit, "source": "fixture.xlsx", "created_at": None}


# ── pick_scope ────────────────────────────────────────────────────────────────

def test_pick_scope_prefers_full_then_falls_back():
    present = {"cap_thermal_full", "cap_wind_full", "cap_solar"}
    chosen, scope = ft.pick_scope(present, ft.CAP_BASE)
    assert chosen["thermal"] == "cap_thermal_full"   # 全口径 preferred
    assert chosen["wind"] == "cap_wind_full"
    assert chosen["solar"] == "cap_solar"            # 省调 fallback
    assert "nuclear" not in chosen                   # absent stem stays absent
    assert scope == "mixed"


def test_pick_scope_all_full_all_base_none():
    chosen, scope = ft.pick_scope({"cap_thermal_full", "cap_wind_full"}, ft.CAP_BASE)
    assert scope == "full"
    chosen, scope = ft.pick_scope({"cap_thermal", "cap_wind"}, ft.CAP_BASE)
    assert scope == "base"
    assert chosen == {"thermal": "cap_thermal", "wind": "cap_wind"}
    chosen, scope = ft.pick_scope(set(), ft.CAP_BASE)
    assert chosen == {} and scope == "none"


def test_pick_scope_hours_prefers_source_rows():
    # 年度利用小时: prefer hours_<stem> (交接表利用小时数 sheet, carries the yoy
    # rows) over hours_<stem>_full (全国发电量 sheet) — never the reverse.
    present = {"hours_thermal", "hours_thermal_full"}
    chosen, scope = ft.pick_scope(present, ft.HOURS_BASE, prefer_full=False)
    assert chosen["thermal"] == "hours_thermal"
    assert scope == "base"


# ── annual_values / monthly_series / hours_map ───────────────────────────────

def test_annual_values_keeps_only_year_rows_of_that_year():
    rows = [
        _row("cap_total", 100.0),
        _row("cap_total", 50.0, period="month", month=date(2025, 3, 1)),
        _row("cap_total", 200.0, month=date(2026, 1, 1)),
        _row("cap_wind", 30.0),
    ]
    ann = ft.annual_values(rows, 2025)
    assert ann["cap_total"]["value"] == 100.0
    assert ann["cap_wind"]["value"] == 30.0
    assert len(ann) == 2


def test_monthly_series_filters_year_metric_keeps_none():
    rows = [
        _row("gen_total", 10.0, period="month", month=date(2026, 1, 1)),
        _row("gen_total", None, period="month", month=date(2026, 2, 1)),
        _row("gen_total", 9.0, period="month", month=date(2025, 1, 1)),
        _row("gen_wind", 3.0, period="month", month=date(2026, 1, 1)),
        _row("gen_total", 999.0),                      # annual row: excluded
    ]
    s = ft.monthly_series(rows, 2026, "gen_total")
    assert s == {1: 10.0, 2: None}                     # None cell survives (reported-empty)


def test_hours_map_keys_by_year_and_month():
    rows = [
        _row("hours_wind_derived", 210.0, period="month", month=date(2026, 7, 1),
             unit="小时", section="util_hours"),
        _row("hours_wind", 1980.0, unit="小时", section="util_hours"),
    ]
    m = ft.hours_map(rows, "hours_wind_derived")
    assert m == {(2026, 7): 210.0}


# ── annual_growth ─────────────────────────────────────────────────────────────

def test_annual_growth_reads_only_pct_yoy_rows():
    rows = [
        _row("demand_total", 100.0, unit="亿千瓦时", section="demand"),
        _row("demand_total_yoy", 8.2, unit="%", section="demand"),
        _row("cap_total_full", 500.0),
        # jiaojie hours yoy is an HOUR DELTA, not % — must not be read as growth
        _row("cap_total_full_yoy", -389.0, unit="小时"),
    ]
    assert ft.annual_growth(rows, ["demand_total"], 2025) == 8.2
    assert ft.annual_growth(rows, ["cap_total_full", "cap_total"], 2025) is None


def test_annual_growth_computed_from_two_annual_rows():
    rows = [
        _row("cap_total_full", 550.0),
        _row("cap_total_full", 500.0, month=date(2024, 1, 1)),
    ]
    assert ft.annual_growth(rows, ["cap_total_full", "cap_total"], 2025) == pytest.approx(10.0)


def test_annual_growth_none_when_nothing_available():
    assert ft.annual_growth([], ["cap_total_full", "cap_total"], 2025) is None


# ── loaders: empty result / DB failure must not crash (brief step 1a) ────────

def test_load_series_empty_result_ok(monkeypatch):
    from services.province_fundamentals import store
    monkeypatch.setattr(store, "read_series", lambda *a, **k: [])
    assert ft.load_series("安徽", "capacity", "postgresql://unused") == []


def test_load_series_db_error_returns_empty(monkeypatch):
    from services.province_fundamentals import store

    def _boom(*a, **k):
        raise RuntimeError("db down")

    monkeypatch.setattr(store, "read_series", _boom)
    assert ft.load_series("山东", "generation", "postgresql://unused") == []


def test_load_provinces_db_error_returns_empty(monkeypatch):
    def _boom(*a, **k):
        raise RuntimeError("db down")

    monkeypatch.setattr(ft.psycopg2, "connect", _boom)
    assert ft.load_provinces("postgresql://unused") == []


# ── def-order guard (v60 lesson), scoped to the fundamentals-tab region ──────

def _tab_region(lines):
    start = next(i for i, l in enumerate(lines, 1)
                 if "Tab 8: Market Fundamentals" in l)
    end = next(i for i, l in enumerate(lines, 1)
               if "Tab 9: Agent" in l)
    return start, end


def test_no_module_level_call_before_def_in_fundamentals_region():
    """Streamlit runs app.py top-to-bottom; a bare-name call in the
    fundamentals tab region must not target a `def` defined later in the file."""
    src = APP_PY.read_text(encoding="utf-8")
    tree = ast.parse(src, filename=str(APP_PY))
    region_start, region_end = _tab_region(src.splitlines())

    defs, region_calls = {}, {}

    def visit(node):
        for child in ast.iter_child_nodes(node):
            if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                continue                      # bodies run at call time
            if isinstance(child, ast.Call) and isinstance(child.func, ast.Name):
                name = child.func.id
                if region_start <= child.lineno < region_end:
                    region_calls.setdefault(name, child.lineno)
            visit(child)

    for stmt in tree.body:
        if isinstance(stmt, ast.FunctionDef):
            defs.setdefault(stmt.name, stmt.lineno)
        else:
            visit(stmt)

    violations = [
        f"{name}: called at line {lineno} but defined at line {defs[name]}"
        for name, lineno in sorted(region_calls.items())
        if name in defs and defs[name] > lineno
    ]
    assert not violations, (
        "Module-level call(s) precede their def (Streamlit NameError):\n"
        + "\n".join(violations)
    )


# ── AppTest smoke: render with stubbed loaders ───────────────────────────────

_T_STUB = """
def _t(key, **kw):
    return key.format(**kw) if kw else key
"""

_HARNESS_HEAD = f"""
import sys, datetime
sys.path.insert(0, {str(APP_PY.parent)!r})
sys.path.insert(0, {str(REPO_ROOT)!r})
import fundamentals_tab as ft
"""


def _run_harness(body: str):
    from streamlit.testing.v1 import AppTest
    with tempfile.NamedTemporaryFile("w", suffix=".py", delete=False) as f:
        f.write(_HARNESS_HEAD + body)
    at = AppTest.from_file(f.name, default_timeout=60)
    at.run()
    return at


def test_render_empty_data_no_crash():
    at = _run_harness(
        _T_STUB + """
ft.load_provinces = lambda pg_url: []
ft.load_series = lambda *a, **k: []
ft.render(t=_t, is_zh=False, pg_url="postgresql://unused")
"""
    )
    assert not at.exception, f"render exception on empty data: {at.exception}"
    sels = [s for s in at.selectbox if s.label == "fund_province"]
    assert len(sels) == 1, "province selectbox missing"
    # AppTest reports formatted option labels: 全国 renders as "National" in EN mode
    assert {"National", "全国"} & set(sels[0].options), \
        f"全国 must always be a candidate: {sels[0].options}"


def test_render_populated_sections_no_crash():
    rows_repr = repr([
        # capacity: annual 全口径 + monthly 省调 (mixed scope across periods)
        _row("cap_total_full", 6820.0), _row("cap_wind_full", 1450.0),
        _row("cap_solar_full", 2310.0),
        _row("cap_total", 6831.49, period="month", month=date(2026, 1, 1)),
        _row("cap_wind", 1451.0, period="month", month=date(2026, 1, 1)),
        # util hours: annual + hour-delta yoy + derived monthly
        _row("hours_total", 3450.0, unit="小时", section="util_hours"),
        _row("hours_total_yoy", -389.0, unit="小时", section="util_hours"),
        _row("hours_wind_derived", 180.0, period="month", month=date(2026, 7, 1),
             unit="小时", section="util_hours"),
        # demand: annual total + sector split
        _row("demand_total", 3214.0, unit="亿千瓦时", section="demand"),
        _row("demand_p1", 45.0, unit="亿千瓦时", section="demand"),
        _row("demand_p2", 2050.0, unit="亿千瓦时", section="demand"),
        _row("demand_p3", 610.0, unit="亿千瓦时", section="demand"),
        _row("demand_residential", 509.0, unit="亿千瓦时", section="demand"),
    ])
    at = _run_harness(
        _T_STUB + f"""
ROWS = {rows_repr}
ft.load_provinces = lambda pg_url: ["安徽"]
ft.load_series = lambda *a, **k: ROWS
ft.render(t=_t, is_zh=False, pg_url="postgresql://unused")
"""
    )
    assert not at.exception, f"render exception on populated data: {at.exception}"
    subs = {s.value for s in at.subheader}
    for key in ("fund_s1_title", "fund_s2_title", "fund_s3_title", "fund_s4_title"):
        assert key in subs, f"section {key} missing: {subs}"
    assert len(at.metric) >= 6, f"annual metric cards missing: {len(at.metric)}"


# ── S5 省际对比 helpers (2026-11-04) ─────────────────────────────────────────

def test_common_month_is_min_of_province_max():
    rows = [
        {"province": "山东", "month": date(2026, 5, 1), "period": "month", "metric": "m"},
        {"province": "山东", "month": date(2026, 4, 1), "period": "month", "metric": "m"},
        {"province": "安徽", "month": date(2026, 7, 1), "period": "month", "metric": "m"},
        {"province": "安徽", "month": date(2025, 1, 1), "period": "year", "metric": "m"},
    ]
    assert ft.common_month(rows) == date(2026, 5, 1)  # min(山东 5月, 安徽 7月)


def test_common_month_empty_when_no_monthly():
    rows = [{"province": "安徽", "month": date(2025, 1, 1), "period": "year", "metric": "m"}]
    assert ft.common_month(rows) is None


def test_pivot_rows_grid_shape_and_missing():
    rows = [
        {"province": "山东", "metric": "cap_total_full", "value": 25000.0, "period": "month",
         "month": date(2026, 5, 1)},
        {"province": "山东", "metric": "cap_thermal_full", "value": 19000.0, "period": "month",
         "month": date(2026, 5, 1)},
        {"province": "安徽", "metric": "cap_total_full", "value": 7752.14, "period": "month",
         "month": date(2026, 5, 1)},
    ]
    provs, metrics, grid = ft.pivot_rows(rows)
    assert provs == ["安徽", "山东"] and metrics == ["cap_thermal_full", "cap_total_full"]
    assert grid["山东"]["cap_total_full"] == 25000.0
    assert grid["安徽"].get("cap_thermal_full") is None  # missing stays missing


def test_metrics_of_filters_absent_variants():
    pairs = [("cap_total_full", "cap_total"), ("cap_wind_full", "cap_wind")]
    assert ft.metrics_of(pairs, {"cap_total_full"}) == [("cap_total_full", "cap_total")]


def test_pick_first_prefers_full():
    assert ft.pick_first({"cap_total_full": 5.0, "cap_total": 4.0},
                         ("cap_total_full", "cap_total")) == 5.0
    assert ft.pick_first({"cap_total": 4.0}, ("cap_total_full", "cap_total")) == 4.0
    assert ft.pick_first({}, ("cap_total_full", "cap_total")) is None
