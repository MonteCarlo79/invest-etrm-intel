"""Trader-nominated MW curves from the WeCom 调度计划表 workbooks — λ v2
instrument source (plan 2026-10-31 task 13 follow-up).

The day sheets carry the full dispatch chain per 15-min slot:

    时间 | SOC（%）| 交易员申报计划 (MW) | 日前出清 (MW) | 实时调度出清 (MW) | 实际执行功率 (MW)

Only column C (交易员申报计划 — the operator's D-1 nomination) is parsed
here: it is decided before RT trading, which makes it a valid instrument
for the discharge/charge → price-impact regression (market_impact v2). The
other columns stay unparsed; add them here if a future consumer needs them.

Layout/sign conventions match dispatch_zones.py: sheet per day ("09.01"),
column A timestamps, row 2 = slot 1 (00:00–00:15), MW positive = discharge.
Header-driven column detection (first header containing 申报) — no
per-station hardcoding.

Table: marketdata.bess_nomination_96, one row per (asset_code, data_date,
slot) with a non-null nomination.
"""
from __future__ import annotations

import calendar
import re
from collections import defaultdict
from datetime import date
from pathlib import Path

import pandas as pd
from sqlalchemy import text

from services.mengxi_nodal.dispatch_zones import (
    _FIRST_ROW,
    _LAST_ROW,
    _SHEET_RE,
    _sheet_date,
)

_DDL = """
CREATE TABLE IF NOT EXISTS marketdata.bess_nomination_96 (
    asset_code   TEXT        NOT NULL,
    data_date    DATE        NOT NULL,
    slot         INTEGER     NOT NULL,
    nominated_mw NUMERIC,
    source_file  TEXT,
    created_at   TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (asset_code, data_date, slot)
)
"""

_UPSERT = text("""
    INSERT INTO marketdata.bess_nomination_96
        (asset_code, data_date, slot, nominated_mw, source_file)
    VALUES (:asset_code, :data_date, :slot, :nominated_mw, :source_file)
    ON CONFLICT (asset_code, data_date, slot)
    DO UPDATE SET nominated_mw = EXCLUDED.nominated_mw,
                  source_file  = EXCLUDED.source_file
""")

_NOM_HEADER_KEY = "申报"  # 交易员申报计划 / 申报计划 / 申报功率


def _nomination_column(ws) -> int | None:
    """1-based index of the first row-1 header containing 申报."""
    for c in range(2, 10):
        v = ws.cell(row=1, column=c).value
        if v is not None and _NOM_HEADER_KEY in str(v):
            return c
    return None


def _stale_a2_detect(a2_dates: list, tab_mds: set, day_sheets: list) -> bool:
    """Shared stale-template detection: operator copied a sheet forward
    without updating the A2 timestamp (bameng's files serve ~3 distinct A2
    dates across 200+ tabs). Fire when distinct A2 dates < half the parseable
    sheets while tab names are all distinct."""
    return (len(a2_dates) >= 4
            and len(set(a2_dates)) <= len(a2_dates) // 2
            and len(tab_mds) == len(day_sheets))


def _parse_xls(path: Path, asset_code: str) -> list[dict]:
    """Legacy .xls workbook via pandas/xlrd (values only — no cell fills;
    wulate's operator switched to .xls from July 2026 and its files carry no
    zone colouring anyway). Same conventions: day sheets MM.DD, header row 1,
    col A timestamps, first 申报 column, row 2 = slot 1."""
    xl = pd.ExcelFile(path)
    day_sheets = [n for n in xl.sheet_names if _SHEET_RE.match(n)]
    frames: dict[str, pd.DataFrame] = {}
    a2_dates, years = [], []
    for name in day_sheets:
        df = pd.read_excel(xl, name, header=None, nrows=100)
        frames[name] = df
        try:
            ts = pd.to_datetime(df.iloc[1, 0])
            years.append(ts.year)
            a2_dates.append(ts.date())
        except (ValueError, TypeError, IndexError, AttributeError):
            continue
    fallback_year = years[0] if years else None
    stale_a2 = _stale_a2_detect(a2_dates,
                                {_SHEET_RE.match(n).groups() for n in day_sheets},
                                day_sheets)
    if stale_a2:
        print(f"WARNING nomination_curves: {path.name} — stale A2 timestamps "
              f"({len(set(a2_dates))} distinct across {len(a2_dates)} sheets); "
              f"dating by tab names, year {fallback_year}")

    rows: list[dict] = []
    for name, df in frames.items():
        m = _SHEET_RE.match(name)
        d = None
        if stale_a2 and fallback_year:
            d = date(fallback_year, int(m.group(1)), int(m.group(2)))
        else:
            try:
                d = pd.to_datetime(df.iloc[1, 0]).date()
            except (ValueError, TypeError, IndexError, AttributeError):
                if fallback_year:
                    d = date(fallback_year, int(m.group(1)), int(m.group(2)))
        if d is None:
            print(f"WARNING nomination_curves: {path.name} sheet {name!r} — "
                  f"no parseable date, sheet skipped")
            continue
        col = next((c for c in range(1, min(10, df.shape[1]))
                    if _NOM_HEADER_KEY in str(df.iloc[0, c])), None)
        if col is None:
            print(f"WARNING nomination_curves: {path.name} sheet {name!r} — "
                  f"no 申报 column, sheet skipped")
            continue
        for r in range(1, min(97, len(df))):
            v = df.iloc[r, col]
            if v is None or (isinstance(v, float) and pd.isna(v)):
                continue
            try:
                mw = float(v)
            except (ValueError, TypeError):
                continue
            rows.append({
                "asset_code": asset_code,
                "data_date": d,
                "slot": r,  # df row 1 = sheet row 2 = slot 1 (00:00–00:15)
                "nominated_mw": mw,
                "source_file": path.name,
            })
    return rows


def parse_nomination_curves(path: Path | str, asset_code: str) -> list[dict]:
    """Walk the day sheets of one monthly nomination workbook and return one
    row per (data_date, slot) with a numeric nomination: {asset_code,
    data_date, slot, nominated_mw, source_file}. Sheets without a 申报
    header are skipped loudly. .xlsx workbooks go through openpyxl (fills +
    values); legacy .xls through pandas/xlrd (values only).

    Date authority: dispatch_zones._sheet_date (A2 timestamp wins) — EXCEPT
    when the workbook's A2s are stale (operator copied a template forward
    without updating the timestamp cell: bameng's files serve ~3 distinct
    A2 dates across 200+ tabs). Detection: distinct A2 dates < half the
    parseable sheets while tab names are all distinct → tab name (MM.DD) is
    the date authority and A2 supplies only the year.
    """
    path = Path(path)
    if path.suffix.lower() == ".xls":
        return _parse_xls(path, asset_code)
    from openpyxl import load_workbook

    path = Path(path)
    wb = load_workbook(path, read_only=False, data_only=True)
    day_sheets = [n for n in wb.sheetnames if _SHEET_RE.match(n)]

    years = []
    a2_dates = []
    for name in day_sheets:
        try:
            ts = pd.to_datetime(wb[name].cell(row=_FIRST_ROW, column=1).value)
            years.append(ts.year)
            a2_dates.append(ts.date())
        except (ValueError, TypeError, AttributeError):
            continue
    fallback_year = years[0] if years else None
    tab_mds = {_SHEET_RE.match(n).groups() for n in day_sheets}
    stale_a2 = (len(a2_dates) >= 4
                and len(set(a2_dates)) <= len(a2_dates) // 2
                and len(tab_mds) == len(day_sheets))
    if stale_a2:
        print(f"WARNING nomination_curves: {path.name} — stale A2 timestamps "
              f"({len(set(a2_dates))} distinct across {len(a2_dates)} sheets); "
              f"dating by tab names, year {fallback_year}")

    rows: list[dict] = []
    for name in day_sheets:
        ws = wb[name]
        if stale_a2:
            m = _SHEET_RE.match(name)
            d = (date(fallback_year, int(m.group(1)), int(m.group(2)))
                 if fallback_year else None)
        else:
            d = _sheet_date(ws, name, fallback_year)
        if d is None:
            print(f"WARNING nomination_curves: {path.name} sheet {name!r} — "
                  f"no parseable date, sheet skipped")
            continue
        col = _nomination_column(ws)
        if col is None:
            print(f"WARNING nomination_curves: {path.name} sheet {name!r} — "
                  f"no 申报 column, sheet skipped")
            continue
        for r in range(_FIRST_ROW, _LAST_ROW + 1):
            v = ws.cell(row=r, column=col).value
            if v is None:
                continue
            try:
                mw = float(v)
            except (ValueError, TypeError):
                continue
            rows.append({
                "asset_code": asset_code,
                "data_date": d,
                "slot": r - 1,  # row 2 -> slot 1 (00:00–00:15)
                "nominated_mw": mw,
                "source_file": path.name,
            })
    return rows


_NAME_DDMM_RE = re.compile(r"(\d{1,2})\.(\d{1,2})")   # 调度计划表07.31 / 11.02
_NAME_MONTH_RE = re.compile(r"(\d{1,2})月")           # 调度计划表7月 / 9月(31)


def _file_name_date(source_file: str, ref: date) -> date | None:
    """The file version's own as-of date from its name: DD.MM → that date,
    X月 → that month's last day (ref's year). None when the name carries
    neither."""
    stem = Path(str(source_file)).stem
    m = _NAME_DDMM_RE.search(stem)
    if m:
        try:
            return date(ref.year, int(m.group(1)), int(m.group(2)))
        except ValueError:
            return None
    m = _NAME_MONTH_RE.search(stem)
    if m:
        mon = int(m.group(1))
        if not 1 <= mon <= 12:
            return None
        return date(ref.year, mon, calendar.monthrange(ref.year, mon)[1])
    return None


def dedupe_rows(rows: list[dict]) -> list[dict]:
    """One row per (asset_code, data_date, slot): when several file versions
    nominate the same slot, keep the nearest-covering version — the file
    whose name-date is the smallest ≥ the data date (a station's own-month
    file beats a later re-save; later re-saves of old tabs carry template
    pollution, e.g. bameng's 11.02 file showing 902 MW in July tabs). Falls
    back to the latest prior version, then to undated names. Conflicting
    values are printed loudly."""
    by: dict[tuple, list[dict]] = defaultdict(list)
    for r in rows:
        by[(r["asset_code"], str(r["data_date"])[:10], r["slot"])].append(r)

    def rank(r: dict, ref: date) -> tuple:
        nd = _file_name_date(r["source_file"], ref)
        if nd is None:
            return (2, 0)
        if nd >= ref:
            return (0, (nd - ref).days)
        return (1, (ref - nd).days)

    out: list[dict] = []
    dropped = 0
    for (ac, dd, slot), cands in sorted(by.items()):
        if len(cands) == 1:
            out.append(cands[0])
            continue
        ref = date.fromisoformat(dd)
        winner = min(cands, key=lambda r: rank(r, ref))
        vals = {float(c["nominated_mw"]) for c in cands}
        if len(vals) > 1:
            print(f"WARNING nomination_curves dedupe: {ac} {dd} slot {slot} "
                  f"conflicting {sorted(vals)} across "
                  f"{[c['source_file'] for c in cands]} -> "
                  f"{winner['source_file']}")
        out.append(winner)
        dropped += len(cands) - 1
    if dropped:
        print(f"nomination_curves dedupe: dropped {dropped} duplicate "
              f"(asset, day, slot) rows from older file versions")
    return out


def ensure_table(engine) -> None:
    with engine.begin() as conn:
        conn.execute(text(_DDL))


def load_curves(engine, path: Path | str, asset_code: str) -> int:
    """Parse one nomination workbook and upsert its rows. Returns row count."""
    rows = parse_nomination_curves(path, asset_code)
    if not rows:
        return 0
    ensure_table(engine)
    with engine.begin() as conn:
        for i in range(0, len(rows), 500):
            conn.execute(_UPSERT, rows[i : i + 500])
    return len(rows)


def get_nominations(engine, asset_code: str, start: date, end: date) -> pd.DataFrame:
    """(interval_start, nominated_mw) frame over [start, end) — interval_start
    rebuilt as naive CST (data_date + (slot-1)·15min) to join the dispatch
    and price frames. Plain date-string comparisons, PG+SQLite portable."""
    df = pd.read_sql(text("""
        SELECT data_date, slot, nominated_mw
        FROM marketdata.bess_nomination_96
        WHERE asset_code = :ac
          AND data_date >= :sd
          AND data_date <  :ed
        ORDER BY data_date, slot
    """), engine, params={"ac": asset_code,
                          "sd": start.isoformat(), "ed": end.isoformat()})
    if df.empty:
        return pd.DataFrame(columns=["interval_start", "nominated_mw"])
    df["interval_start"] = (pd.to_datetime(df["data_date"])
                            + pd.to_timedelta((df["slot"].astype(int) - 1) * 15,
                                              unit="min"))
    df["nominated_mw"] = pd.to_numeric(df["nominated_mw"], errors="coerce")
    return df[["interval_start", "nominated_mw"]]
