"""Tests for nomination_curves — the 交易员申报计划 (D-1 nomination) parser and
the bess_nomination_96 read/write path. No RDS: workbook fixtures built in
tmp_path, SQL on a sqlite-ATTACH harness (same pattern as dispatch_zones /
market_impact tests).
"""
from datetime import date
from pathlib import Path

import pandas as pd
import pytest
from openpyxl import Workbook
from sqlalchemy import create_engine

from services.mengxi_nodal.nomination_curves import (
    _DDL,
    dedupe_rows,
    ensure_table,
    get_nominations,
    load_curves,
    parse_nomination_curves,
)

_HEADER = ["时间", "SOC（%）", "交易员申报计划 (MW)", "日前出清 (MW)",
           "实时调度出清 (MW)", "实际执行功率 (MW)"]


def _make_workbook(path, sheets: dict[str, list], header=None):
    """One day-sheet per name; each sheet = header row + rows of
    [timestamp, soc, nominated, da, rt, actual] (only the first three
    columns matter to the parser)."""
    wb = Workbook()
    wb.remove(wb.active)
    agg = wb.create_sheet("电力交易调度计划")  # aggregate sheet — not a day
    agg.append(header or _HEADER)
    for name, rows in sheets.items():
        ws = wb.create_sheet(name)
        ws.append(header or _HEADER)
        for r in rows:
            ws.append(r)
    wb.save(path)


def _day_rows(day: str, nominated: list) -> list:
    """96-row day sheet body: slot i at (slot-1)·15min with the given
    nomination (None allowed)."""
    base = pd.Timestamp(day)
    return [[str(base + pd.Timedelta(minutes=15 * i)), 0.5, v, 0, 0, 0]
            for i, v in enumerate(nominated)]


def test_ddl_shape():
    assert "marketdata.bess_nomination_96" in _DDL
    for col in ("asset_code", "data_date", "slot", "nominated_mw",
                "source_file"):
        assert col in _DDL
    assert "PRIMARY KEY (asset_code, data_date, slot)" in _DDL
    assert "DEFAULT CURRENT_TIMESTAMP" in _DDL  # SQLite-harness compatible


def test_parse_day_sheets(tmp_path):
    f = tmp_path / "【5】220kV裕昭沙子坝储能电力交易调度计划表9月.xlsx"
    nom1 = [0.0, -160.0, -400.0] + [0.0] * 93
    nom2 = [10.0] + [0.0] * 94 + [None]      # last slot un-nominated
    _make_workbook(f, {"09.01": _day_rows("2026-09-01", nom1),
                       "09.02": _day_rows("2026-09-02", nom2)})
    rows = parse_nomination_curves(f, "gushanliang")
    by = {(r["data_date"], r["slot"]): r["nominated_mw"] for r in rows}
    # All 96 slots of 09.01 (zeros kept — 0 MW is a real nomination) and
    # 95 of 09.02 (the None slot dropped).
    assert sum(1 for r in rows if r["data_date"] == date(2026, 9, 1)) == 96
    assert sum(1 for r in rows if r["data_date"] == date(2026, 9, 2)) == 95
    assert by[(date(2026, 9, 1), 2)] == -160.0   # row 3 -> slot 2
    assert by[(date(2026, 9, 1), 3)] == -400.0   # negative = charge
    assert by[(date(2026, 9, 2), 1)] == 10.0
    assert all(r["asset_code"] == "gushanliang" for r in rows)
    assert all(r["source_file"] == f.name for r in rows)


def test_parse_skips_sheet_without_nomination_column(tmp_path, capsys):
    f = tmp_path / "10月.xlsx"
    _make_workbook(f, {"10.01": _day_rows("2026-10-01", [1.0] * 96)},
                   header=["时间", "SOC（%）", "日前出清 (MW)"])
    rows = parse_nomination_curves(f, "gushanliang")
    assert rows == []
    assert "no 申报 column" in capsys.readouterr().out


def test_dedupe_prefers_nearest_covering_version(capsys):
    """bameng's 11.02 re-save shows 902 MW in July tabs (template pollution);
    July's own monthly file must win July slots. Identical-value duplicates
    dedupe silently; conflicting ones print."""
    rows = [
        {"asset_code": "bameng", "data_date": date(2026, 7, 5), "slot": 27,
         "nominated_mw": 0.0, "source_file": "…调度计划表07.31.xlsx"},
        {"asset_code": "bameng", "data_date": date(2026, 7, 5), "slot": 27,
         "nominated_mw": 902.0, "source_file": "…调度计划表11.02.xlsx"},
        {"asset_code": "bameng", "data_date": date(2026, 7, 5), "slot": 27,
         "nominated_mw": 5.0, "source_file": "…调度计划表7月(31).xlsx"},
        # identical duplicate — silent, nearest-covering still wins
        {"asset_code": "suyou", "data_date": date(2026, 9, 10), "slot": 3,
         "nominated_mw": -50.0, "source_file": "…9月.xlsx"},
        {"asset_code": "suyou", "data_date": date(2026, 9, 10), "slot": 3,
         "nominated_mw": -50.0, "source_file": "…09.30.xlsx"},
    ]
    out = dedupe_rows(rows)
    assert len(out) == 2
    by = {(r["asset_code"], r["data_date"], r["slot"]): r for r in out}
    # 7月(31) -> 2026-07-31, same as 07.31; tie broken toward the monthly
    # name? No: both name-dates are 07-31 — first-seen among equals is the
    # min() pick; the 902 MW row must lose regardless.
    assert by[("bameng", date(2026, 7, 5), 27)]["nominated_mw"] != 902.0
    assert by[("suyou", date(2026, 9, 10), 3)]["nominated_mw"] == -50.0
    noise = capsys.readouterr().out
    assert "conflicting" in noise and "902.0" in noise
    assert "suyou" not in noise


def test_dedupe_falls_back_to_latest_prior():
    rows = [
        {"asset_code": "suyou", "data_date": date(2026, 11, 1), "slot": 1,
         "nominated_mw": 1.0, "source_file": "…10.31.xlsx"},
        {"asset_code": "suyou", "data_date": date(2026, 11, 1), "slot": 1,
         "nominated_mw": 2.0, "source_file": "…9月.xlsx"},
    ]
    [winner] = dedupe_rows(rows)
    assert winner["nominated_mw"] == 1.0  # 10.31 is nearer than 09-30


def test_parse_stale_a2_workbook_dates_by_tab(tmp_path, capsys):
    """bameng pattern: the operator copies a template forward without
    updating the A2 timestamp — one A2 date serves many tab dates. The tab
    name must become the date authority (A2 still supplies the year)."""
    f = tmp_path / "计划表.xlsx"
    sheets = {}
    for i, tab in enumerate(["07.20", "07.21", "07.22", "07.23", "07.24"]):
        rows = _day_rows("2026-07-04", [float(i)] + [0.0] * 95)  # stale A2
        sheets[tab] = rows
    _make_workbook(f, sheets)
    rows = parse_nomination_curves(f, "bameng")
    out = capsys.readouterr().out
    assert "stale A2" in out
    by = {(r["data_date"], r["slot"]): r["nominated_mw"] for r in rows}
    for i, day in enumerate(range(20, 25)):
        assert by[(date(2026, 7, day), 1)] == float(i)


@pytest.fixture()
def sqlite_engine():
    eng = create_engine("sqlite:///:memory:")
    with eng.begin() as conn:
        conn_exec = conn.execute
        from sqlalchemy import text
        conn_exec(text("ATTACH DATABASE ':memory:' AS marketdata"))
    ensure_table(eng)
    return eng


def test_load_and_get_roundtrip(sqlite_engine, tmp_path):
    f = tmp_path / "9月.xlsx"
    nom = [0.0, -160.0] + [0.0] * 94
    _make_workbook(f, {"09.01": _day_rows("2026-09-01", nom)})
    n = load_curves(sqlite_engine, f, "gushanliang")
    assert n == 96
    n2 = load_curves(sqlite_engine, f, "gushanliang")  # idempotent upsert
    assert n2 == 96
    df = get_nominations(sqlite_engine, "gushanliang",
                         date(2026, 9, 1), date(2026, 9, 2))
    assert len(df) == 96
    assert df["interval_start"].iloc[0] == pd.Timestamp("2026-09-01 00:00:00")
    assert df["interval_start"].iloc[1] == pd.Timestamp("2026-09-01 00:15:00")
    assert df["nominated_mw"].iloc[1] == -160.0
    # Window excludes outside days / other assets.
    assert get_nominations(sqlite_engine, "gushanliang",
                           date(2026, 9, 2), date(2026, 9, 3)).empty
    assert get_nominations(sqlite_engine, "suyou",
                           date(2026, 9, 1), date(2026, 9, 2)).empty


# ── legacy .xls path (wulate operator sends .xls since July 2026) ────────────

_REPO = Path(__file__).resolve().parents[2]
WULATE_XLS = (_REPO / "data" / "raw" / "nomination" / "乌拉特中旗"
              / "远景乌拉特储能电站电力交易调度计划表10月.xls")


@pytest.mark.skipif(not WULATE_XLS.exists(), reason="wulate .xls fixture absent")
def test_parse_legacy_xls_workbook():
    rows = parse_nomination_curves(WULATE_XLS, "wulate")
    days = {r["data_date"] for r in rows}
    assert min(days) == date(2026, 10, 1)
    assert max(days) == date(2026, 10, 13)     # the October file stops at 10.13
    assert len(days) == 13
    by = {(r["data_date"], r["slot"]): r["nominated_mw"] for r in rows}
    assert by[(date(2026, 10, 13), 1)] == 32.0  # sheet 10.13 row 2 (screenshot-verified)
    assert all(1 <= r["slot"] <= 96 for r in rows)
    assert all(r["source_file"] == WULATE_XLS.name for r in rows)


def test_parse_short_workbook_stale_a2_dates_by_tab(tmp_path, capsys):
    """bameng's 11.02 file: only 3 sheets (below the bulk stale rule's
    threshold) with July A2s on November tabs — the per-sheet drift guard
    must date by tab regardless."""
    f = tmp_path / "景怡查干哈达储能电站电力交易调度计划表11.02.xlsx"
    sheets = {}
    for i, tab in enumerate(["10.31", "11.01", "11.02"]):
        sheets[tab] = _day_rows("2026-07-05", [float(i)] + [0.0] * 95)  # stale A2
    _make_workbook(f, sheets)
    rows = parse_nomination_curves(f, "bameng")
    out = capsys.readouterr().out
    assert "drift bound" in out
    by = {(r["data_date"], r["slot"]): r["nominated_mw"] for r in rows}
    assert by[(date(2026, 10, 31), 1)] == 0.0
    assert by[(date(2026, 11, 1), 1)] == 1.0
    assert by[(date(2026, 11, 2), 1)] == 2.0
    assert all(r["data_date"].month >= 10 for r in rows)


# ── 2026-11-11: multi-unit workbooks — every matching column summed ─────────

_WUCHUAN_HEADER = ["时间", "SOC（%）一期", "SOC（%）二期",
                   "交易员申报计划 (MW)一期", "日前出清 (MW)一期", "实时调度出清 (MW)一期",
                   "实际执行功率 (MW)一期",
                   "交易员申报计划 (MW)二期", "日前出清 (MW)二期", "实时调度出清 (MW)二期",
                   "实际执行功率 (MW)二期", "出清偏差(MW)一期", "AGC下发功率(MW)一期"]


def test_two_account_workbook_sums_both_accounts(tmp_path):
    # 武川 = 旭纯哈乐 (一期) + 旭纯哈乐二 (二期); 二期 实际执行 sits in col K,
    # past the old B..I scan window.
    from services.mengxi_nodal.nomination_curves import parse_actual_curves
    base = pd.Timestamp("2026-09-23")
    body = [[str(base + pd.Timedelta(minutes=15 * i)), 40, 41,
             248.0, 248.0, 248.0, 248.8, 240.0, 240.0, 240.0, 239.5, 0.8, None]
            for i in range(96)]
    body[5][7] = None                     # 二期 nomination missing in slot 6
    f = tmp_path / "220kV旭纯哈乐储能电站电站电力交易调度计划表9月.xlsx"
    _make_workbook(f, {"09.23": body}, header=_WUCHUAN_HEADER)
    nom = {r["slot"]: r["nominated_mw"] for r in parse_nomination_curves(f, "wuchuan")}
    act = {r["slot"]: r["actual_mw"] for r in parse_actual_curves(f, "wuchuan")}
    assert nom[1] == 488.0
    assert 6 not in nom and len(nom) == 95  # partial sum never written
    assert act[1] == pytest.approx(488.3)


def test_unit_group_columns_are_summed(tmp_path):
    # 阿拉善 splits 申报 by unit group: "3、5" + "1、2、4、6" = site (= DA cleared).
    header = ["时间", "SOC（%）", "交易员申报计划 3、5(MW)", "交易员申报计划 1、2、4、6(MW)",
              "日前出清 (MW)", "实时调度出清 (MW)", "实际执行功率 (MW)"]
    base = pd.Timestamp("2026-10-04")
    body = [[str(base + pd.Timedelta(minutes=15 * i)), 60, 90, 212, 302, 302, 303.7]
            for i in range(96)]
    f = tmp_path / "景敖乌珠日储能电站电力交易调度计划表10月.xlsx"
    _make_workbook(f, {"10.04": body}, header=header)
    rows = parse_nomination_curves(f, "alashan")
    assert len(rows) == 96 and all(r["nominated_mw"] == 302.0 for r in rows)


def test_new_stations_shipped_without_zone_parse():
    from services.wecom_nominations import run_daily
    assert run_daily.FOLDERS["武川"] == "wuchuan"
    assert run_daily.FOLDERS["阿拉善"] == "alashan"
    assert {"wuchuan", "alashan"} <= run_daily.NO_ZONE_PARSE
    assert not ({"wuchuan", "alashan"} & run_daily.NO_ZONE_FILES)  # no consensus rows
