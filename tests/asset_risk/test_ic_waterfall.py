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


def test_all_assets_sum_only_assets_with_ic():
    ic_all = _ic_rows("wuchuan", cod=date(2026, 7, 20))
    keys = [("武川", "2026-09"), ("阿拉善", "2026-09")]
    leg, missing = ic_leg(ic_all, keys, NAMES, COD)
    assert missing == [("阿拉善", "2026-09")]
    bench = pd.concat([_leg("武川", "2026-09", 100), _leg("阿拉善", "2026-09", 500)])
    bench = restrict_to_keys(bench, list(zip(leg["asset"], leg["month"])))
    wf = build_waterfall(bench, bench, bench, bench, ic=leg)
    assert wf[wf["stage"] == "投资标准"]["cny"].sum() == 100          # 阿拉善 excluded
    assert wf[wf["stage"] == "投决预期"]["cny"].sum() == pytest.approx(
        leg[["arb_cny", "cap_cny", "fee_cny"]].sum().sum())


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
