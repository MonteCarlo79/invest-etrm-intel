"""Tests for the 蒙西 dispatch-zone pipeline (plan task 12). No RDS:
- parser tests read the real 谷山梁 September nomination workbook from data/raw;
- DB tests run on a sqlite-ATTACH harness (same shape as tests/asset_risk);
- engine tests are tiny synthetic LPs.
"""
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
from sqlalchemy import create_engine, text

from services.mengxi_nodal.dispatch_zones import (
    _DDL,
    ensure_table,
    get_zones,
    load_zones,
    parse_nomination_zones,
    zones_to_masks,
)
from services.bess_map.optimisation_engine import (
    compute_dispatch_from_15min_prices,
    optimise_window,
)

_REPO = Path(__file__).resolve().parents[2]
NOM_FILE = (_REPO / "data" / "raw" / "nomination" / "谷山梁"
            / "【5】220kV裕昭沙子坝储能电力交易调度计划表9月.xlsx")


# ---------- parser (real file) ----------

@pytest.fixture(scope="module")
def sep_rows():
    return parse_nomination_zones(NOM_FILE, "gushanliang")


def test_parse_covers_all_30_days_96_slots(sep_rows):
    assert len(sep_rows) == 30 * 96
    days = {r["data_date"] for r in sep_rows}
    assert days == {date(2026, 9, d) for d in range(1, 31)}
    assert all(r["asset_code"] == "gushanliang" for r in sep_rows)
    assert all(r["source_file"] == NOM_FILE.name for r in sep_rows)
    assert {r["slot"] for r in sep_rows if r["data_date"] == date(2026, 9, 1)} == set(range(1, 97))


def test_parse_single_digit_sheet_name(sep_rows):
    # '9.13' (not '09.13') must still land on 2026-09-13.
    assert sum(1 for r in sep_rows if r["data_date"] == date(2026, 9, 13)) == 96


def test_parse_0902_decode_exact(sep_rows):
    """User-validated 2026-09-02 decode: green 00:00-09:30 + 16:30-18:45 +
    20:30-24:00 (61 slots), yellow 09:30-16:30 (28), red 18:45-20:30 (7).
    Slot s covers (s-1)*15min → s*15min."""
    z = {r["slot"]: r["zone"] for r in sep_rows if r["data_date"] == date(2026, 9, 2)}
    assert len(z) == 96
    for s in range(1, 39):
        assert z[s] == "green", s          # 00:00–09:30
    for s in range(39, 67):
        assert z[s] == "yellow", s         # 09:30–16:30
    for s in range(67, 76):
        assert z[s] == "green", s          # 16:30–18:45
    for s in range(76, 83):
        assert z[s] == "red", s            # 18:45–20:30
    for s in range(83, 97):
        assert z[s] == "green", s          # 20:30–24:00
    counts = pd.Series(list(z.values())).value_counts()
    assert counts["green"] == 61 and counts["yellow"] == 28 and counts["red"] == 7


# ---------- zone → mask mapping ----------

def test_zones_to_masks():
    zones = ["green"] * 96
    zones[75] = "red"      # slot 76
    zones[38] = "yellow"   # slot 39
    no_charge, no_discharge = zones_to_masks(zones)
    assert no_charge == {76}
    assert no_discharge == {39}


def test_zones_to_masks_all_free():
    assert zones_to_masks(["green"] * 96) == (set(), set())


# ---------- engine hard constraints (synthetic LPs) ----------

@pytest.fixture()
def lp_solver(monkeypatch):
    """Swap the engine's CBC for HiGHS: pulp's bundled CBC is an x86_64 binary
    and cannot execute on this arm64 Mac (no Rosetta). The constraints under
    test are solver-agnostic; production Fargate (amd64) keeps CBC."""
    import pulp
    monkeypatch.setattr(
        pulp, "PULP_CBC_CMD",
        lambda **kw: pulp.HiGHS(msg=False, timeLimit=kw.get("timeLimit")))


def _solve8(**kw):
    prices = np.array([10, 10, 10, 10, 100, 100, 100, 100], dtype=float)
    return optimise_window(prices, power_mw=1.0, duration_h=2.0,
                           roundtrip_eff=1.0, dt=1.0, **kw)


def test_optimise_window_unconstrained_baseline(lp_solver):
    res = _solve8()
    assert res.status == "Optimal"
    assert res.profit == pytest.approx(2 * (100 - 10))  # 2 MWh through the spread


def test_optimise_window_no_charge_blocks_cheap_slots(lp_solver):
    res = _solve8(no_charge_slots={0, 1, 2, 3})
    assert res.status == "Optimal"
    assert np.allclose(res.charge_mw, 0)
    assert np.allclose(res.discharge_mw, 0)
    assert res.profit == pytest.approx(0.0)


def test_optimise_window_no_discharge_blocks_expensive_slots(lp_solver):
    res = _solve8(no_discharge_slots={4, 5, 6, 7})
    assert res.status == "Optimal"
    assert np.allclose(res.discharge_mw, 0)
    assert res.profit == pytest.approx(0.0)


def test_optimise_window_partial_mask_shifts_dispatch(lp_solver):
    # Masking 2 of 4 cheap slots just moves the charge; profit unchanged.
    res = _solve8(no_charge_slots={0, 1})
    assert res.charge_mw[0] == 0 and res.charge_mw[1] == 0
    assert res.profit == pytest.approx(180.0)


def _day_series():
    prices = np.array([10.0] * 48 + [100.0] * 48)
    idx = pd.date_range("2026-09-02", periods=96, freq="15min")
    return pd.Series(prices, index=idx)


def test_15min_wrapper_masks_are_one_based(lp_solver):
    # Full no-charge mask over the cheap half (slots 1..48) → no cycle at all.
    disp, profit = compute_dispatch_from_15min_prices(
        _day_series(), power_mw=1.0, duration_h=2.0, roundtrip_eff=1.0,
        no_charge_slots=set(range(1, 49)))
    assert np.allclose(disp["charge_mw"], 0)
    assert np.allclose(disp["discharge_mw"], 0)
    assert float(profit.sum()) == pytest.approx(0.0)


def test_15min_wrapper_mask_boundaries(lp_solver):
    # {1} must hit the FIRST slot only; {96} the LAST. Off-by-one would leak.
    disp, _ = compute_dispatch_from_15min_prices(
        _day_series(), power_mw=1.0, duration_h=2.0, roundtrip_eff=1.0,
        no_charge_slots={1}, no_discharge_slots={96})
    assert disp["charge_mw"].iloc[0] == 0
    assert disp["discharge_mw"].iloc[95] == 0
    # dispatch shifts, day profit preserved (8 slots needed, 47/48 available)
    ch_mwh = disp["charge_mw"].sum() * 0.25
    dis_mwh = disp["discharge_mw"].sum() * 0.25
    assert ch_mwh == pytest.approx(2.0) and dis_mwh == pytest.approx(2.0)


# ---------- loader + staleness (sqlite harness) ----------

@pytest.fixture()
def sqlite_engine():
    eng = create_engine("sqlite:///:memory:")
    with eng.connect() as conn:
        conn.execute(text("ATTACH DATABASE ':memory:' AS marketdata"))
    ensure_table(eng)
    return eng


def test_ddl_shape():
    assert "marketdata.bess_dispatch_zones" in _DDL
    for col in ("asset_code", "data_date", "slot", "zone", "source_file"):
        assert col in _DDL
    assert "PRIMARY KEY" in _DDL


def test_load_and_get_zones_exact_day(sqlite_engine):
    n = load_zones(sqlite_engine, NOM_FILE, "gushanliang")
    assert n == 30 * 96
    zones, stale = get_zones(sqlite_engine, "gushanliang", date(2026, 9, 2))
    assert not stale
    assert len(zones) == 96
    assert zones[75] == "red" and zones[38] == "yellow" and zones[0] == "green"


def test_load_is_idempotent_upsert(sqlite_engine):
    load_zones(sqlite_engine, NOM_FILE, "gushanliang")
    load_zones(sqlite_engine, NOM_FILE, "gushanliang")
    with sqlite_engine.connect() as conn:
        cnt = conn.execute(text(
            "SELECT COUNT(*) FROM marketdata.bess_dispatch_zones")).scalar()
    assert cnt == 30 * 96


def test_get_zones_stale_fallback_returns_latest(sqlite_engine):
    load_zones(sqlite_engine, NOM_FILE, "gushanliang")
    zones, stale = get_zones(sqlite_engine, "gushanliang", date(2026, 10, 5))
    assert stale
    latest, stale_latest = get_zones(sqlite_engine, "gushanliang", date(2026, 9, 30))
    assert not stale_latest
    assert zones == latest


def test_get_zones_no_data(sqlite_engine):
    load_zones(sqlite_engine, NOM_FILE, "gushanliang")
    # Before the first loaded day → nothing to fall back to.
    assert get_zones(sqlite_engine, "gushanliang", date(2026, 8, 1)) == ([], False)
    # Unknown station → unconstrained signal.
    assert get_zones(sqlite_engine, "wulate", date(2026, 9, 2)) == ([], False)


# ── fleet consensus (bameng/wulate carry no usable file colouring) ──────────

from services.mengxi_nodal.dispatch_zones import consensus_zone_rows


def test_consensus_majority_and_targets():
    voters = [
        *[{"asset_code": c, "data_date": "2026-11-01", "slot": 30, "zone": z,
           "source_file": "f"}
          for c, z in (("gushanliang", "red"), ("suyou", "red"),
                       ("hangjinqi", "yellow"), ("siziwangqi", "red"))],
    ]
    rows = consensus_zone_rows(voters, {"bameng", "wulate"})
    assert len(rows) == 2
    assert all(r["zone"] == "red" for r in rows)
    assert {r["asset_code"] for r in rows} == {"bameng", "wulate"}
    assert all(r["source_file"] == "fleet_consensus" for r in rows)


def test_consensus_tie_goes_restrictive():
    voters = [
        *[{"asset_code": c, "data_date": "2026-11-01", "slot": 31, "zone": z,
           "source_file": "f"}
          for c, z in (("gushanliang", "yellow"), ("suyou", "yellow"),
                       ("hangjinqi", "red"), ("siziwangqi", "red"))],
    ]
    [row] = consensus_zone_rows(voters, {"bameng"})
    assert row["zone"] == "red"


def test_consensus_min_voters_gate():
    voters = [{"asset_code": "gushanliang", "data_date": "2026-11-01",
               "slot": 32, "zone": "red", "source_file": "f"}]
    assert consensus_zone_rows(voters, {"bameng"}) == []
