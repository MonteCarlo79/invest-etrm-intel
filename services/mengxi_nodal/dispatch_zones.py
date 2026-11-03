"""蒙西 dispatch-restriction zones — nomination-file cell fills → per-station
per-day 96-slot zone map (plan task 12).

The monthly nomination workbooks (data/raw/nomination/<station>/【*】…N月.xlsx)
carry one sheet per day ('09.01'..'09.30', with occasional single-digit
variants like '9.13') whose column A rows 2..97 are the 96 quarter-hour slots
00:00–23:45. The operator paints each slot's cell fill (solid RGB):

  red    FF0000 — NO CHARGING    (evening-peak discharge window)
  yellow FFFF00 — NO DISCHARGING (solar-midday charge window)
  green  00B050 — free           (also the default for unfilled cells)

Zone labels are stored in marketdata.bess_dispatch_zones, one row per
(asset_code, data_date, slot), slot 1-based. Slots are CST-naive wall clock —
no timezone handling anywhere in this module (matches the mnp96 filled
table's naive-CST frame).

Staleness: nomination files arrive manually (D-1 operation, often late), so
get_zones falls back to the station's LATEST available day before the
requested date and flags the result stale, rather than silently running the
PF unconstrained.
"""
from __future__ import annotations

import re
from datetime import date
from pathlib import Path

import pandas as pd
from sqlalchemy import text

_DDL = """
CREATE TABLE IF NOT EXISTS marketdata.bess_dispatch_zones (
    asset_code  TEXT      NOT NULL,
    data_date   DATE      NOT NULL,
    slot        SMALLINT  NOT NULL,
    zone        TEXT      NOT NULL,
    source_file TEXT,
    created_at  TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (asset_code, data_date, slot)
)
"""

_UPSERT = text("""
    INSERT INTO marketdata.bess_dispatch_zones
        (asset_code, data_date, slot, zone, source_file)
    VALUES (:asset_code, :data_date, :slot, :zone, :source_file)
    ON CONFLICT (asset_code, data_date, slot)
    DO UPDATE SET zone        = EXCLUDED.zone,
                  source_file = EXCLUDED.source_file
""")

# Day sheets are named '09.01' or '9.13'; anything else (template sheet
# '电力交易调度计划', cover sheets) is skipped.
_SHEET_RE = re.compile(r"^(\d{1,2})\.(\d{1,2})$")

# Fill RGB (last 6 hex chars) → zone label. Unrecognised / unfilled → green.
_RGB_ZONE = {"FF0000": "red", "FFFF00": "yellow", "00B050": "green"}

_FIRST_ROW, _LAST_ROW = 2, 97  # col A rows covering the 96 slots


def _fill_zone(cell) -> str:
    f = cell.fill
    if f is None or f.fill_type != "solid":
        return "green"
    rgb = getattr(f.start_color, "rgb", None)
    if not isinstance(rgb, str):
        return "green"
    return _RGB_ZONE.get(rgb[-6:].upper(), "green")


def _sheet_date(ws, sheet_name: str, fallback_year: int | None):
    """data_date for one day sheet: the A2 timestamp (00:00 slot) is
    authoritative — it carries the year the filename lacks. Sheet-name
    month/day must agree (loud print otherwise). Returns None when A2 is
    unparseable."""
    m = _SHEET_RE.match(sheet_name)
    name_md = (int(m.group(1)), int(m.group(2))) if m else (None, None)
    try:
        ts = pd.to_datetime(ws.cell(row=_FIRST_ROW, column=1).value)
        d = ts.date()
    except (ValueError, TypeError):
        d = None
    if d is None:
        if fallback_year is None or name_md[0] is None:
            return None
        d = date(fallback_year, name_md[0], name_md[1])
    elif name_md[0] is not None and (d.month, d.day) != name_md:
        print(f"WARNING dispatch_zones: sheet {sheet_name!r} tab date "
              f"{name_md} != A2 timestamp {d} — using A2")
    return d


def parse_nomination_zones(path: Path | str, asset_code: str) -> list[dict]:
    """Walk the day sheets of one monthly nomination workbook and return one
    row per (data_date, slot): {asset_code, data_date, slot, zone,
    source_file}. Rows with no fill default to green."""
    from openpyxl import load_workbook

    path = Path(path)
    wb = load_workbook(path, read_only=False, data_only=True)
    day_sheets = [n for n in wb.sheetnames if _SHEET_RE.match(n)]

    # Year fallback pass: any sheet whose A2 parses supplies the year for
    # sheets whose A2 does not (all sheets in one monthly file share it).
    years = []
    for name in day_sheets:
        try:
            years.append(pd.to_datetime(
                wb[name].cell(row=_FIRST_ROW, column=1).value).year)
        except (ValueError, TypeError, AttributeError):
            continue
    fallback_year = years[0] if years else None

    rows: list[dict] = []
    for name in day_sheets:
        ws = wb[name]
        d = _sheet_date(ws, name, fallback_year)
        if d is None:
            print(f"WARNING dispatch_zones: {path.name} sheet {name!r} — "
                  f"no parseable date, sheet skipped")
            continue
        for r in range(_FIRST_ROW, _LAST_ROW + 1):
            rows.append({
                "asset_code": asset_code,
                "data_date": d,
                "slot": r - 1,  # row 2 -> slot 1 (00:00–00:15)
                "zone": _fill_zone(ws.cell(row=r, column=1)),
                "source_file": path.name,
            })
    return rows


def zones_to_masks(zones: list[str]) -> tuple[set[int], set[int]]:
    """96 zone labels → (no_charge_slots, no_discharge_slots), 1-based slot
    numbers ready for compute_dispatch_from_15min_prices: red forbids
    charging, yellow forbids discharging."""
    no_charge = {i + 1 for i, z in enumerate(zones) if z == "red"}
    no_discharge = {i + 1 for i, z in enumerate(zones) if z == "yellow"}
    return no_charge, no_discharge


# Zone-severity order for consensus tie-breaks: with an even voter count a
# 2-2 tie resolves toward the more restrictive label (estimation noise errs
# toward compliance, not opportunity).
_SEVERITY = {"red": 2, "yellow": 1, "green": 0}


def consensus_zone_rows(voter_rows: list[dict], target_codes,
                        min_voters: int = 2) -> list[dict]:
    """Fleet-consensus zones for stations whose own files carry no usable
    colouring (bameng's red marks 出清偏差, wulate's files are unpainted).

    voter_rows: parsed zone rows from the COLOURED stations only. For each
    (data_date, slot) with at least min_voters labels, the majority zone
    wins (ties → most restrictive). One output row per target code with
    source_file='fleet_consensus'. The operator issues a uniform fleet
    schedule per day (user-confirmed 2026-11-02), so the vote is the
    targets' zone map.
    """
    votes: dict[tuple, list[str]] = {}
    for r in voter_rows:
        votes.setdefault((str(r["data_date"])[:10], int(r["slot"])), []).append(
            r["zone"])
    out: list[dict] = []
    for (d, slot), labels in sorted(votes.items()):
        if len(labels) < min_voters:
            continue
        counts = {z: labels.count(z) for z in set(labels)}
        best = max(counts.values())
        winners = [z for z, n in counts.items() if n == best]
        zone = max(winners, key=lambda z: _SEVERITY[z])  # tie -> restrictive
        for code in target_codes:
            out.append({"asset_code": code, "data_date": d, "slot": slot,
                        "zone": zone, "source_file": "fleet_consensus"})
    return out


def ensure_table(engine) -> None:
    with engine.begin() as conn:
        conn.execute(text(_DDL))


def load_zones(engine, path: Path | str, asset_code: str) -> int:
    """Parse one nomination workbook and upsert its rows. Returns row count."""
    rows = parse_nomination_zones(path, asset_code)
    if not rows:
        return 0
    ensure_table(engine)
    with engine.begin() as conn:
        for i in range(0, len(rows), 500):
            conn.execute(_UPSERT, rows[i : i + 500])
    return len(rows)


def _rows_to_labels(rows) -> list[str]:
    """(slot, zone) rows → 96 labels; missing slots default to green."""
    by_slot = {int(s): z for s, z in rows}
    return [by_slot.get(i, "green") for i in range(1, 97)]


def get_zones(engine, asset_code: str, day: date) -> tuple[list[str], bool]:
    """96 zone labels for one station-day, 1-based slot order.

    Staleness rule: when the requested day is absent, return the station's
    latest available day BEFORE it with stale=True. Returns ([], False) when
    the station has no zones at all — callers treat that as unconstrained.
    """
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT slot, zone FROM marketdata.bess_dispatch_zones
            WHERE asset_code = :a AND data_date = :d ORDER BY slot
        """), {"a": asset_code, "d": day}).fetchall()
        if rows:
            return _rows_to_labels(rows), False
        latest = conn.execute(text("""
            SELECT MAX(data_date) FROM marketdata.bess_dispatch_zones
            WHERE asset_code = :a AND data_date < :d
        """), {"a": asset_code, "d": day}).scalar()
        if latest is None:
            return [], False
        rows = conn.execute(text("""
            SELECT slot, zone FROM marketdata.bess_dispatch_zones
            WHERE asset_code = :a AND data_date = :d ORDER BY slot
        """), {"a": asset_code, "d": latest}).fetchall()
        return _rows_to_labels(rows), True
