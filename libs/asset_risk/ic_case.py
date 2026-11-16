"""Investment-committee (投决预期) underwriting case: workbook parser, monthly value, store.

Pure domain code (no streamlit/apps imports). Prices and fees are ¥/MWh.
Two source layouts are understood:
  Format A  "<站名>-IPMT 边界" / "巴盟-IPMT": scalars + SOH table + per-year price table.
  Format B  "<站名>-测算边界汇总": labels in col B/C, units in D, values from E.
  Format C  "苏尼特-杭锦旗-乌拉特": one column per station (G/H/I), period rows in col E.

容量电价 (capacity_price_yuan_kw, annual ¥/kW): owner decision — applied from operating
year capcomp_years+1 (i.e. once the 容量补偿 subsidy ends), as
capacity_mw × 1000 × price × days / 365.
"""
from __future__ import annotations

import calendar
import math
import re
from datetime import date, datetime

import pandas as pd
from sqlalchemy import text

from libs.asset_risk.review.aliases import STATION_ALIASES

IC_COLUMNS = ["asset_code", "op_year", "soh", "cycles_per_year", "charge_price", "discharge_price",
              "dod", "rte", "capcomp_rate", "capcomp_years", "grid_fee", "capacity_mw", "energy_mwh",
              "capacity_price_yuan_kw", "ic_cod", "case_label", "source_file"]

OP_YEARS = 20

# Format A "Table I" SOH curve (also used for Format B, whose sheets only describe it in prose).
SOH_TABLE_I = [0.946, 0.923, 0.905, 0.888, 0.872, 0.857, 0.844, 0.83, 0.818, 0.806, 0.794, 0.783,
               0.772, 0.762, 0.752, 0.742, 0.732, 0.723, 0.714, 0.706]
FORMAT_B_CAPCOMP_RATE = 350.0   # ¥/MWh (0.35 元/kWh)
FORMAT_B_CAPCOMP_YEARS = 10

DDL = """CREATE TABLE IF NOT EXISTS marketdata.rm_ic_assumptions (
  asset_code TEXT NOT NULL, op_year INT NOT NULL, soh DOUBLE PRECISION, cycles_per_year DOUBLE PRECISION,
  charge_price DOUBLE PRECISION, discharge_price DOUBLE PRECISION, dod DOUBLE PRECISION, rte DOUBLE PRECISION,
  capcomp_rate DOUBLE PRECISION, capcomp_years INT, grid_fee DOUBLE PRECISION,
  capacity_mw DOUBLE PRECISION, energy_mwh DOUBLE PRECISION, capacity_price_yuan_kw DOUBLE PRECISION,
  ic_cod DATE, case_label TEXT, source_file TEXT,
  uploaded_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY (asset_code, op_year))"""


def ensure_table(engine) -> None:
    with engine.begin() as conn:
        conn.execute(text(DDL))


# ---------------------------------------------------------------- parsing helpers
def _num(v) -> float | None:
    if isinstance(v, bool) or v is None:
        return None
    if isinstance(v, (int, float)):
        return None if (isinstance(v, float) and math.isnan(v)) else float(v)
    return None


def _frac(v) -> float | None:
    """Percentage cell: 0.9 or 90 -> 0.9."""
    x = _num(v)
    if x is None:
        return None
    return x / 100.0 if x > 1.5 else x


def _as_date(v) -> date | None:
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, date):
        return v
    return None


def _station(sheet_name: str) -> tuple[str | None, str | None]:
    """(asset_code, problem). Substring alias match; ambiguity or no match -> problem."""
    hits = {code for code, names in STATION_ALIASES.items() if any(a in sheet_name for a in names)}
    if len(hits) == 1:
        return hits.pop(), None
    if not hits:
        return None, "unknown station (no alias match)"
    return None, f"ambiguous station ({sorted(hits)})"


def _find_row(ws, labels, cols=(2, 3)) -> int | None:
    for r in range(1, ws.max_row + 1):
        for c in cols:
            v = ws.cell(r, c).value
            if isinstance(v, str) and any(l in v for l in labels):
                return r
    return None


def _scalar(ws, labels, col=4):
    r = _find_row(ws, labels)
    return ws.cell(r, col).value if r else None


def _expand(rows_by_year: dict[int, dict], tail: dict | None) -> dict[int, dict]:
    """Explicit years kept; years beyond them take the tail (后15年) row, else the last explicit row."""
    if not rows_by_year:
        return {}
    last = tail or rows_by_year[max(rows_by_year)]
    return {y: rows_by_year.get(y, last) for y in range(1, OP_YEARS + 1)}


def _parse_format_a(ws, asset_code: str, file_name: str) -> tuple[list[dict], str | None]:
    cap = _scalar(ws, ["建设容量"])
    try:
        gw, gwh = [float(p) for p in str(cap).split("/")]
    except (TypeError, ValueError):
        return [], "建设容量 not parseable"
    dod, rte = _frac(_scalar(ws, ["放电深度", "DOD"])), _frac(_scalar(ws, ["充放电效率", "RTE"]))
    cc_rate, cc_years = _num(_scalar(ws, ["容量补偿"])), _num(_scalar(ws, ["补偿年限"]))
    if dod is None or rte is None or cc_rate is None or cc_years is None:
        return [], "missing DOD/RTE/容量补偿/补偿年限"

    # SOH table: header row has 年份 + SOH
    soh_hdr = next((r for r in range(1, ws.max_row + 1)
                    if ws.cell(r, 2).value == "年份" and ws.cell(r, 3).value == "SOH"), None)
    # year table: header row has 年度 (column varies: E in 武川, F in 巴盟)
    yr_hdr = yr_col = None
    for r in range(1, ws.max_row + 1):
        for c in range(1, ws.max_column + 1):
            if ws.cell(r, c).value == "年度":
                yr_hdr, yr_col = r, c
                break
        if yr_hdr:
            break
    if soh_hdr is None or yr_hdr is None:
        return [], "SOH or year table not found"
    soh = {}
    for r in range(soh_hdr + 1, ws.max_row + 1):
        y, s = _num(ws.cell(r, 2).value), _num(ws.cell(r, 3).value)
        if y is None or s is None:
            break
        soh[int(y)] = s
    heads = {str(ws.cell(yr_hdr, c).value or "").replace("\n", ""): c for c in range(yr_col, ws.max_column + 1)}
    col = {k: next((c for h, c in heads.items() if k in h), None)
           for k in ("全年充放次数", "充电价格", "放电价格")}
    if None in col.values():
        return [], "year table columns missing"
    explicit, tail = {}, None
    for r in range(yr_hdr + 1, ws.max_row + 1):
        yv = ws.cell(r, yr_col).value
        if yv is None:
            break
        rec = {k: _num(ws.cell(r, c).value) for k, c in col.items()}
        if None in rec.values():
            continue
        if isinstance(yv, str) and "后" in yv:
            tail = rec
        elif _num(yv) is not None:
            explicit[int(yv)] = rec
    yrs = _expand(explicit, tail)
    if not yrs or len(soh) < OP_YEARS:
        return [], "year table or SOH incomplete"
    cap_price = _num(_scalar(ws, ["容量电价"]))
    out = [dict(asset_code=asset_code, op_year=y, soh=soh[y], cycles_per_year=yrs[y]["全年充放次数"],
                charge_price=yrs[y]["充电价格"], discharge_price=yrs[y]["放电价格"], dod=dod, rte=rte,
                capcomp_rate=cc_rate * 1000.0, capcomp_years=int(cc_years), grid_fee=0.0,
                capacity_mw=gw * 1000.0, energy_mwh=gwh * 1000.0, capacity_price_yuan_kw=cap_price, ic_cod=None, case_label=None,
                source_file=file_name) for y in range(1, OP_YEARS + 1)]
    return out, None


def _parse_format_b(ws, asset_code: str, file_name: str) -> tuple[list[dict], str | None]:
    hdr_row = _find_row(ws, ["项目基本信息"], cols=(2,)) or 2
    vcols = []
    for c in range(5, ws.max_column + 1):
        h = ws.cell(hdr_row, c).value
        if not h or (isinstance(h, str) and "来源" in h):
            break
        vcols.append(c)
    if not vcols:
        return [], "no value columns"

    def val(labels, kind=_num):
        r = _find_row(ws, labels)
        if not r:
            return None, None
        for c in reversed(vcols):               # last non-empty value column wins
            v = kind(ws.cell(r, c).value)
            if v is not None:
                return v, c
        return None, None

    mw, mw_col = val(["额定功率"])
    mwh, _ = val(["额定容量"])
    dod, _ = val(["放电深度"], _frac)
    rte, _ = val(["充放电效率"], _frac)
    dis, _ = val(["放电电价"])
    chg, _ = val(["充电电价"])
    daily, _ = val(["日均充放电次数"])
    fee, _ = val(["电网收费"])
    cod, _ = val(["项目运营日"], _as_date)
    if None in (mw, mwh, dod, rte, dis, chg, daily, fee):
        return [], "missing required rows"
    label = str(ws.cell(hdr_row, mw_col).value).strip() if mw_col else None
    out = [dict(asset_code=asset_code, op_year=y, soh=SOH_TABLE_I[y - 1], cycles_per_year=daily * 365.0,
                charge_price=round(chg * 1000.0, 6), discharge_price=round(dis * 1000.0, 6), dod=dod, rte=rte,
                capcomp_rate=FORMAT_B_CAPCOMP_RATE, capcomp_years=FORMAT_B_CAPCOMP_YEARS,
                grid_fee=round(fee * 1000.0, 6), capacity_mw=mw * 10.0, energy_mwh=mwh * 10.0, capacity_price_yuan_kw=None, ic_cod=cod,
                case_label=label, source_file=file_name) for y in range(1, OP_YEARS + 1)]
    return out, None


C_LABEL = "经评边界值汇总"
C_STATIONS = {"苏尼特右旗": "suyou", "乌拉特中旗": "wulate"}
C_SKIP_NOTE = "杭锦旗: covered by 预审会 case — not taken from this sheet"
C_FIRST_YEAR = 2025


def _c_trend(text_, y_lo, y_hi):
    """'平均预期：2027年 0.4，逐年线性递减至 0.3' -> (0.4, 0.3), or None."""
    m = re.search(r"(\d{4})\s*年\s*(-?\d+(?:\.\d+)?)\D+?(-?\d+(?:\.\d+)?)\s*$", str(text_ or "").strip())
    if not m or int(m.group(1)) != y_lo:
        return None
    return float(m.group(2)), float(m.group(3))


def _parse_format_c(ws, file_name: str) -> tuple[list[dict], list[str]]:
    """Per-station rows op_year 1..20 (calendar year Y -> op_year Y-2024, ic_cod 2025-01-01).

    Spread convention (度电价差): discharge_price = spread (¥/MWh), charge_price = 0, so
    arbitrage = discharge energy x spread. 2025-26 = station value; 2027-34 linear from
    the text's start to end value; 2035-44 likewise. SOH: first->last value of the
    'a% - b%' text over op_year 1..10, then the same per-year step extrapolated to year 20.
    """
    notes: list[str] = []
    hdr = next((r for r in range(1, ws.max_row + 1) if ws.cell(r, 3).value == "序号"), None)
    if hdr is None:
        return [], ["header row not found"]
    cols = {str(ws.cell(hdr, c).value).strip(): c for c in range(7, ws.max_column + 1) if ws.cell(hdr, c).value}

    def first_row(label):
        return next((r for r in range(hdr + 1, ws.max_row + 1) if label in str(ws.cell(r, 4).value or "")), None)

    r_cyc, r_spr = first_row("日循环次数"), first_row("度电价差")
    r_size, r_cc = first_row("储能系统规模"), first_row("容量补偿")
    if None in (r_cyc, r_spr, r_size, r_cc):
        return [], ["missing 规模/容量补偿/日循环次数/度电价差 rows"]
    # period rows are contiguous: r, r+1, r+2
    dod = _frac(_scalar_c(ws, "放电深度", hdr))
    rte = _frac(_scalar_c(ws, "RTE", hdr))
    soh_txt = _scalar_c(ws, "SOH", hdr)
    m = re.search(r"(\d+(?:\.\d+)?)\s*%\s*[-~–]\s*(\d+(?:\.\d+)?)\s*%", str(soh_txt or ""))
    if dod is None or rte is None or not m:
        return [], ["DOD/RTE/SOH not parseable"]
    s1, s10 = float(m.group(1)) / 100, float(m.group(2)) / 100
    step = (s10 - s1) / 9.0
    soh = {y: s1 + step * (y - 1) for y in range(1, OP_YEARS + 1)}
    cc_note = str(ws.cell(r_cc, 10).value or "")
    my = re.search(r"(\d+)\s*年", cc_note)
    if not my:
        return [], ["补贴年限 not parseable"]
    cc_years = int(my.group(1))

    def spread_text(r):
        return next((ws.cell(r, c).value for c in range(7, ws.max_column + 1)
                     if isinstance(ws.cell(r, c).value, str)), None)

    t1, t2 = _c_trend(spread_text(r_spr + 1), 2027, 2034), _c_trend(spread_text(r_spr + 2), 2035, 2044)
    if t1 is None or t2 is None:
        return [], ["度电价差 trend text (2027-34 / 2035-44) not parseable"]

    out = []
    for name, code in C_STATIONS.items():
        c = cols.get(name)
        if c is None:
            notes.append(f"{name}: station column not found")
            continue
        try:
            mw, mwh = [float(p) for p in str(ws.cell(r_size, c).value).split("/")]
        except ValueError:
            notes.append(f"{name}: 储能系统规模 not parseable")
            continue
        cc, d1, d2, d3 = (_num(ws.cell(r_cc, c).value), _num(ws.cell(r_cyc, c).value),
                          _num(ws.cell(r_cyc + 1, c).value), _num(ws.cell(r_cyc + 2, c).value))
        sp0 = _num(ws.cell(r_spr, c).value)
        if None in (cc, d1, d2, d3, sp0):
            notes.append(f"{name}: missing 日循环次数/度电价差/容量补偿 values")
            continue
        for y in range(1, OP_YEARS + 1):
            cy = C_FIRST_YEAR + y - 1
            if cy <= 2026:
                daily, sp = d1, sp0
            elif cy <= 2034:
                daily, sp = d2, t1[0] + (t1[1] - t1[0]) * (cy - 2027) / (2034 - 2027)
            else:
                daily, sp = d3, t2[0] + (t2[1] - t2[0]) * (cy - 2035) / (2044 - 2035)
            out.append(dict(asset_code=code, op_year=y, soh=soh[y], cycles_per_year=daily * 365.0,
                            charge_price=0.0, discharge_price=round(sp * 1000.0, 6), dod=dod, rte=rte,
                            capcomp_rate=cc * 1000.0, capcomp_years=cc_years, grid_fee=0.0,
                            capacity_mw=mw, energy_mwh=mwh, capacity_price_yuan_kw=None,
                            ic_cod=date(C_FIRST_YEAR, 1, 1),
                            case_label=C_LABEL + " (SOH years 11-20 extrapolated)", source_file=file_name))
    if out:
        notes.append(C_SKIP_NOTE)
        notes.append("苏尼特-杭锦旗-乌拉特: SOH years 11-20 extrapolated at the year 1-10 slope")
    return out, notes


def _scalar_c(ws, label, hdr):
    r = next((r for r in range(hdr + 1, ws.max_row + 1)
              if label in str(ws.cell(r, 4).value or "") or label in str(ws.cell(r, 3).value or "")), None)
    return ws.cell(r, 5).value if r else None


def parse_ic_workbook(path_or_buffer, file_name: str) -> tuple[pd.DataFrame, list[str]]:
    """Rows per asset x op_year 1..20 plus notes (skipped sheets with reason)."""
    from openpyxl import load_workbook
    wb = load_workbook(path_or_buffer, data_only=True)
    rows, notes, seen = [], [], {}
    # Sheets whose title has stray whitespace are older duplicates (e.g. '…汇总 ' = 0429 version): last.
    for ws in sorted(wb.worksheets, key=lambda w: w.title != w.title.strip()):
        t = ws.title
        if t.strip() == "苏尼特-杭锦旗-乌拉特":
            recs, cn = _parse_format_c(ws, file_name)
            if not recs:
                notes.append(f"{t}: skipped, {'; '.join(cn)}")
            else:
                notes.extend(cn)
                rows.extend(recs)
            continue
        if "IPMT" in t:
            parser = _parse_format_a
        elif "测算边界汇总" in t:
            parser = _parse_format_b
        else:
            notes.append(f"{t}: skipped, not an IC sheet (no 'IPMT' / '测算边界汇总' in name)")
            continue
        code, problem = _station(t)
        if problem:
            notes.append(f"{t}: skipped, {problem}")
            continue
        if code in seen:
            notes.append(f"{t}: skipped, duplicate of sheet '{seen[code]}' for {code}")
            continue
        recs, problem = parser(ws, code, file_name)
        if not recs:
            notes.append(f"{t}: skipped, {problem or 'no rows'}")
            continue
        seen[code] = t
        rows.extend(recs)
    df = pd.DataFrame(rows, columns=IC_COLUMNS)
    return df, notes


# ---------------------------------------------------------------- monthly value
def _d(v) -> date | None:
    if v is None or (isinstance(v, float) and math.isnan(v)) or v is pd.NaT:
        return None
    if isinstance(v, pd.Timestamp):
        return v.date()
    if isinstance(v, datetime):
        return v.date()
    return v if isinstance(v, date) else None


def ic_month_value(ic: pd.DataFrame, asset_code: str, month: date, cod: date | None) -> dict | None:
    """IC-case P&L for one calendar month; None when no IC rows, no cod, or month precedes cod."""
    if ic is None or ic.empty:
        return None
    rows = ic[ic["asset_code"] == asset_code]
    if rows.empty:
        return None
    cod = _d(cod)
    if cod is None or cod.year < 2000:
        cod = next((c for c in (_d(v) for v in rows["ic_cod"]) if c is not None), None)
    if cod is None or cod.year < 2000:
        return None
    m0 = date(month.year, month.month, 1)
    m1 = date(month.year, month.month, calendar.monthrange(month.year, month.month)[1])
    if m1 < cod:
        return None
    start = max(m0, cod)
    days = (m1 - start).days + 1
    op_year = min(int((start - cod).days / 365.25) + 1, OP_YEARS)
    r = rows[rows["op_year"] == op_year]
    if r.empty:
        return None
    r = r.iloc[0]

    def f(k):
        v = r[k]
        return 0.0 if v is None or pd.isna(v) else float(v)

    e, dod, soh, rte = f("energy_mwh"), f("dod"), f("soh"), f("rte")
    dis_c = e * dod * soh * math.sqrt(rte)
    chg_c = e * dod * soh / math.sqrt(rte)
    cycles = f("cycles_per_year") * days / 365.0
    arb = cycles * (dis_c * f("discharge_price") - chg_c * f("charge_price"))
    cap = cycles * dis_c * f("capcomp_rate") if op_year <= f("capcomp_years") else 0.0
    fee = cycles * chg_c * f("grid_fee")
    cp = (f("capacity_price_yuan_kw") * f("capacity_mw") * 1000.0 * days / 365.0
          if op_year > f("capcomp_years") else 0.0)
    return dict(arbitrage_cny=arb, capcomp_cny=cap, capacity_price_cny=cp, grid_fee_cny=fee,
                total_cny=arb + cap + cp - fee,
                discharge_mwh=cycles * dis_c, charge_mwh=cycles * chg_c, op_year=op_year, days=days)


# ---------------------------------------------------------------- store
def save_ic(engine, df: pd.DataFrame) -> None:
    """Replace the stored case per asset_code (DELETE then INSERT, one transaction)."""
    if df is None or df.empty:
        return
    out = df[IC_COLUMNS].astype(object).where(df[IC_COLUMNS].notna(), None)
    with engine.begin() as conn:
        for code in out["asset_code"].unique():
            conn.execute(text("DELETE FROM marketdata.rm_ic_assumptions WHERE asset_code = :c"), {"c": code})
        out.to_sql("rm_ic_assumptions", conn, schema="marketdata", if_exists="append", index=False,
                   method="multi", chunksize=200)


def load_ic(engine) -> pd.DataFrame:
    with engine.connect() as conn:
        df = pd.read_sql(text("SELECT " + ", ".join(IC_COLUMNS) +
                              " FROM marketdata.rm_ic_assumptions ORDER BY asset_code, op_year"), conn)
    df["ic_cod"] = [_d(pd.to_datetime(v)) if v is not None and not pd.isna(v) else None for v in df["ic_cod"]]
    return df
