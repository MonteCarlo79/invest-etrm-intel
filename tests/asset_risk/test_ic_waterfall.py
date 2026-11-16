"""投决预期 stage: ic_leg, build_waterfall with ic, upload expander smoke, seed script."""
import math
from datetime import date

import pandas as pd
import pytest
import sqlalchemy
from openpyxl import Workbook
from sqlalchemy import event, text
from streamlit.testing.v1 import AppTest

from libs.asset_risk.ic_case import IC_COLUMNS, ic_month_value, load_ic
from libs.asset_risk.waterfall import _STAGES, build_waterfall, ic_leg, restrict_to_keys


def _ic_rows(code="wuchuan", cod=None):
    rows = []
    for y in range(1, 21):
        rows.append(dict(asset_code=code, op_year=y, soh=0.946, cycles_per_year=234.0, charge_price=177.73,
                         discharge_price=275.77, dod=0.9, rte=0.85, capcomp_rate=350.0, capcomp_years=10,
                         grid_fee=20.0, capacity_mw=500.0, energy_mwh=2000.0, ic_cod=cod,
                         case_label=None, source_file="t.xlsx"))
    return pd.DataFrame(rows, columns=IC_COLUMNS)


def _leg(asset, month, arb, cap=0.0, fee=0.0):
    return pd.DataFrame([{"asset": asset, "month": month, "arb_cny": arb, "cap_cny": cap,
                          "fee_cny": fee, "other_cny": 0.0, "dis_mwh": 1.0, "chg_mwh": 1.0}])


NAMES = {"武川": "wuchuan", "阿拉善": "alashan"}
COD = {"武川": date(2026, 7, 20), "阿拉善": None}


def test_ic_leg_matches_month_value_and_reports_missing():
    ic = _ic_rows()
    keys = [("武川", "2026-09"), ("武川", "2026-06"), ("阿拉善", "2026-09")]
    leg, missing = ic_leg(ic, keys, NAMES, COD)
    v = ic_month_value(ic, "wuchuan", date(2026, 9, 1), date(2026, 7, 20))
    assert list(leg["asset"]) == ["武川"] and leg.iloc[0]["month"] == "2026-09"
    assert leg.iloc[0]["arb_cny"] == pytest.approx(v["arbitrage_cny"])
    assert leg.iloc[0]["cap_cny"] == pytest.approx(v["capcomp_cny"]) and v["capcomp_cny"] > 0
    assert leg.iloc[0]["fee_cny"] == pytest.approx(-v["grid_fee_cny"]) and v["grid_fee_cny"] > 0
    # pre-cod month and asset with no IC rows are reported, never zero-filled
    assert set(missing) == {("武川", "2026-06"), ("阿拉善", "2026-09")}


def test_ic_leg_nat_commission_falls_back_to_ic_cod():
    ic = _ic_rows(cod=date(2026, 7, 20))
    leg, missing = ic_leg(ic, [("武川", "2026-09")], NAMES, {"武川": pd.NaT})
    assert len(leg) == 1 and not missing


def test_waterfall_unchanged_without_ic():
    args = (_leg("武川", "2026-09", 100), _leg("武川", "2026-09", 80),
            _leg("武川", "2026-09", 70), _leg("武川", "2026-09", 60, fee=-5))
    base = build_waterfall(*args)
    assert build_waterfall(*args, ic=None).equals(base)
    assert build_waterfall(*args, ic=pd.DataFrame()).equals(base)
    assert list(dict.fromkeys(base["stage"])) == _STAGES


def test_waterfall_ic_stage_first_and_bridge_closes():
    ic = _leg("武川", "2026-09", 90, cap=30, fee=-10)          # total 110
    bench = _leg("武川", "2026-09", 100, cap=20)               # total 120
    wf = build_waterfall(bench, _leg("武川", "2026-09", 80), _leg("武川", "2026-09", 70),
                         _leg("武川", "2026-09", 60), ic=ic)
    assert list(dict.fromkeys(wf["stage"]))[:3] == ["投决预期", "市场偏差", "投资标准"]
    tot = lambda s: wf[wf["stage"] == s]["cny"].sum()
    assert tot("投决预期") == 110
    assert tot("市场偏差") == pytest.approx(tot("投资标准") - tot("投决预期")) == 10
    for c in ("arb_cny", "cap_cny", "fee_cny", "other_cny"):
        g = lambda s: wf[(wf["stage"] == s) & (wf["component"] == c)]["cny"].sum()
        assert g("投决预期") + g("市场偏差") == pytest.approx(g("投资标准"))


def test_all_assets_passthrough_keeps_existing_legs_and_closes_bridge():
    from libs.asset_risk.waterfall import ic_passthrough
    ic_all = _ic_rows("wuchuan", cod=date(2026, 7, 20))
    keys = [("武川", "2026-09"), ("阿拉善", "2026-09")]
    leg, missing = ic_leg(ic_all, keys, NAMES, COD)
    assert missing == [("阿拉善", "2026-09")]
    bench = pd.concat([_leg("武川", "2026-09", 100, cap=10), _leg("阿拉善", "2026-09", 500, cap=7)])
    nom, clr, act = (bench.assign(arb_cny=bench["arb_cny"] * f) for f in (0.9, 0.8, 0.7))
    plain = build_waterfall(bench, nom, clr, act)
    full = ic_passthrough(leg, missing, bench)
    wf = build_waterfall(bench, nom, clr, act, ic=full)
    tot = lambda w, s, c=None: w[(w["stage"] == s) & ((w["component"] == c) if c else True)]["cny"].sum()
    for st_ in ("投资标准", "申报", "出清", "实际", "Δ策略与预测", "Δ出清校核", "Δ执行与费用"):
        assert tot(wf, st_) == pytest.approx(tot(plain, st_))      # existing legs unchanged
    # pass-through asset enters at its own 投资标准 and contributes 0 deviation
    pt = full[full["asset"] == "阿拉善"].iloc[0]
    assert (pt["arb_cny"], pt["cap_cny"], pt["fee_cny"]) == (500, 7, 0)
    only_pt = build_waterfall(bench[bench["asset"] == "阿拉善"], nom[nom["asset"] == "阿拉善"],
                              clr[clr["asset"] == "阿拉善"], act[act["asset"] == "阿拉善"],
                              ic=full[full["asset"] == "阿拉善"])
    assert tot(only_pt, "市场偏差") == 0
    for c in ("arb_cny", "cap_cny", "fee_cny", "other_cny"):       # bridge identity
        assert tot(wf, "投决预期", c) + tot(wf, "市场偏差", c) == pytest.approx(tot(wf, "投资标准", c))


# ---------------- upload expander smoke (sqlite + ATTACH) ----------------
SCRIPT = '''
import os, sqlalchemy
from sqlalchemy import event
from apps.asset_risk.tab_waterfall import render_ic_upload
eng = sqlalchemy.create_engine("sqlite:///" + os.environ["IC_MAIN_DB"])
@event.listens_for(eng, "connect")
def _attach(conn, _):
    conn.execute("ATTACH DATABASE '%s' AS marketdata" % os.environ["IC_MD_DB"])
render_ic_upload(eng)
'''


def test_upload_expander_renders(tmp_path, monkeypatch):
    monkeypatch.setenv("IC_MAIN_DB", str(tmp_path / "m.db"))
    monkeypatch.setenv("IC_MD_DB", str(tmp_path / "md.db"))
    at = AppTest.from_string(SCRIPT, default_timeout=60).run()
    assert not at.exception, [e.message for e in at.exception]
    assert any("投决边界上传" in e.label for e in at.expander)


# ---------------- seed script ----------------
def _wb(path):
    from tests.asset_risk.test_ic_case import _format_a_sheet
    wb = Workbook()
    ws = wb.active
    ws.title = "武川-IPMT 边界"
    _format_a_sheet(ws, 234, (177.73, 275.77), (85.21, 260.0), (61.72, 246.24))
    wb.save(path)


def test_seed_dry_run_then_apply(tmp_path, capsys):
    import importlib.util
    spec = importlib.util.spec_from_file_location(
        "seed_ic", "scripts/asset_risk/seed_ic.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    f = tmp_path / "ic.xlsx"
    _wb(f)
    main, md = tmp_path / "m.db", tmp_path / "md.db"
    eng = sqlalchemy.create_engine(f"sqlite:///{main}")

    @event.listens_for(eng, "connect")
    def _a(conn, _):
        conn.execute(f"ATTACH DATABASE '{md}' AS marketdata")

    assert mod.run([str(f)], None, apply=False) == 0               # no DB needed
    assert mod.run([str(f)], eng, apply=True) == 0
    df = load_ic(eng)
    assert set(df["asset_code"]) == {"wuchuan"} and len(df) == 20
    assert mod.run([str(f)], eng, apply=True) == 0                 # idempotent
    assert len(load_ic(eng)) == 20


def test_ic_scaled_by_chain_coverage_not_bench_days():
    from libs.asset_risk.waterfall import ic_passthrough, ic_scale_to_chain
    ic_all = _ic_rows("wuchuan", cod=date(2026, 7, 20))
    keys = [("武川", "2026-09"), ("武川", "2026-07"), ("阿拉善", "2026-09")]
    leg, missing = ic_leg(ic_all, keys, NAMES, COD)
    raw = leg.set_index("month")
    assert raw.loc["2026-09", "ic_days"] == 30 and raw.loc["2026-07", "ic_days"] == 12   # from cod 07-20
    # bench solved 0 days for 武川 09 — must NOT zero the IC leg any more
    bench = pd.DataFrame([
        {"asset": "武川", "month": "2026-09", "arb_cny": 0.0, "cap_cny": 0.0, "days_solved": 0},
        {"asset": "武川", "month": "2026-07", "arb_cny": 1.0, "cap_cny": 0.0, "days_solved": 31},
        {"asset": "阿拉善", "month": "2026-09", "arb_cny": 500.0, "cap_cny": 7.0, "days_solved": 10}])
    cov = {("武川", "2026-09"): 20, ("武川", "2026-07"): 31}     # 07: > ic_days -> capped
    out, scaled = ic_scale_to_chain(leg, cov)
    assert scaled == [("武川", "2026-09")]
    s = out.set_index("month")
    for c in ("arb_cny", "cap_cny", "fee_cny"):
        assert s.loc["2026-09", c] == pytest.approx(raw.loc["2026-09", c] * 20 / 30)
        assert s.loc["2026-09", c] != 0 or raw.loc["2026-09", c] == 0
        assert s.loc["2026-07", c] == pytest.approx(raw.loc["2026-07", c])        # capped at 1
    full = ic_passthrough(out, missing, bench)
    pt = full[full["asset"] == "阿拉善"].iloc[0]
    assert (pt["arb_cny"], pt["cap_cny"], pt["fee_cny"]) == (500, 7, 0)            # pass-through unscaled
    # no chain coverage for an asset-month -> unscaled; empty coverage -> unchanged
    same, none = ic_scale_to_chain(leg, {("其他", "2026-09"): 3})
    assert none == [] and same.equals(leg)
    same, none = ic_scale_to_chain(leg, {})
    assert none == [] and same.equals(leg)


def test_chain_coverage_days_counts_nominated_days_from_cod():
    from libs.asset_risk.waterfall import chain_coverage_days
    ts = pd.to_datetime(["2026-09-01 00:15", "2026-09-01 00:30", "2026-09-02 00:15",
                         "2026-09-03 00:15", "2026-09-04 00:15"])
    chain = pd.DataFrame({"asset": "武川", "month": "2026-09", "ts": ts,
                          "nominated_mw": [1.0, 2.0, None, 3.0, 4.0]})
    assert chain_coverage_days(chain, {})[("武川", "2026-09")] == 3          # 09-02 has no 申报
    assert chain_coverage_days(chain, {"武川": date(2026, 9, 3)})[("武川", "2026-09")] == 2
    assert chain_coverage_days(chain.iloc[0:0]) == {}


def test_bench_coverage_gaps_flags_below_90pct():
    from libs.asset_risk.waterfall import bench_coverage_gaps
    bench = pd.DataFrame([
        {"asset": "A", "month": "2026-09", "days_solved": 0},
        {"asset": "A", "month": "2026-11", "days_solved": 1},
        {"asset": "B", "month": "2026-09", "days_solved": 27},     # 27 >= 0.9*30 ok
        {"asset": "B", "month": "2026-10", "days_solved": 26},     # 26 < 27 flagged
        {"asset": "C", "month": "2026-09", "days_solved": 0}])     # no chain data: not flagged
    cov = {("A", "2026-09"): 30, ("A", "2026-11"): 11, ("B", "2026-09"): 30, ("B", "2026-10"): 30}
    assert bench_coverage_gaps(bench, cov) == [
        ("A", "2026-09", 0, 30), ("A", "2026-11", 1, 11), ("B", "2026-10", 26, 30)]
    assert bench_coverage_gaps(bench, {}) == []


def test_review_overlay_skips_actual_without_bills(monkeypatch):
    import plotly.graph_objects as go
    import apps.asset_risk.tab_waterfall as tw
    rv = pd.DataFrame([("suyou", date(2026, 9, 1), m, 100.0, "CNY")
                       for m in ("strategy_revenue_cny", "executed_revenue_cny")],
                      columns=["asset_code", "month", "metric", "value", "unit"])
    monkeypatch.setattr(tw.review_store, "load_review", lambda *a, **k: rv)
    assets = pd.DataFrame({"name": ["苏右"]})
    nom = _leg("苏右", "2026-09", 90)
    act = _leg("苏右", "2026-09", 0.0)
    fig = go.Figure()
    recon_df, _ = tw._review_overlay(None, fig, assets, ["2026-09"], {"苏右": "suyou"}, nom, nom, None)
    assert list(recon_df["stage"]) == ["申报"] and list(fig.data[0].x) == ["申报"]
    recon_df, _ = tw._review_overlay(None, go.Figure(), assets, ["2026-09"], {"苏右": "suyou"}, nom, nom, act)
    assert "实际" in set(recon_df["stage"])
