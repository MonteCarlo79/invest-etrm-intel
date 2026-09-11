# services/interconnector/ingest.py
"""Excel → staging loaders for the interconnector tab. Upload path: in-app, no S3."""
from __future__ import annotations

import calendar
import re
from datetime import date

import pandas as pd

DDL = [
    """CREATE TABLE IF NOT EXISTS staging.interconnector_trades (
        id SERIAL PRIMARY KEY,
        target_month_raw TEXT, period_type TEXT,
        month_start DATE, month_end DATE,
        recv_province TEXT, send_raw TEXT, send_anchor TEXT,
        channel_1 TEXT, channel_2 TEXT, channel_3 TEXT,
        vol_pre_mwh NUMERIC, vol_post_mwh NUMERIC,
        send_price NUMERIC, land_price NUMERIC,
        jingrong_vol_mwh NUMERIC, jingrong_price NUMERIC,
        channel_fee NUMERIC,
        source_file TEXT, uploaded_at TIMESTAMPTZ DEFAULT NOW()
    )""",
    """CREATE TABLE IF NOT EXISTS staging.interconnector_channels (
        name TEXT PRIMARY KEY, formal_name TEXT,
        send_station TEXT, send_prov TEXT, send_lon NUMERIC, send_lat NUMERIC,
        recv_station TEXT, recv_prov TEXT, recv_lon NUMERIC, recv_lat NUMERIC,
        kv TEXT, gw NUMERIC, commissioned TEXT, km NUMERIC,
        category TEXT, note TEXT, updated_at TIMESTAMPTZ DEFAULT NOW()
    )""",
    """CREATE TABLE IF NOT EXISTS staging.interconnector_mlt_snapshot (
        id SERIAL PRIMARY KEY,
        snapshot_label TEXT, sheet TEXT,
        send_region TEXT, send_prov TEXT, recv_prov TEXT,
        trade_type TEXT, channel TEXT, is_subtotal BOOLEAN DEFAULT FALSE,
        volume_100m_kwh NUMERIC, landing_price NUMERIC,
        uploaded_at TIMESTAMPTZ DEFAULT NOW(),
        UNIQUE(snapshot_label, sheet, send_prov, recv_prov, trade_type, channel)
    )""",
    """CREATE TABLE IF NOT EXISTS staging.interconnector_mlt_pct_override (
        province TEXT PRIMARY KEY, pct NUMERIC, updated_at TIMESTAMPTZ DEFAULT NOW()
    )""",
]

def ensure_tables(conn) -> None:
    with conn.cursor() as cur:
        for stmt in DDL:
            cur.execute(stmt)
    conn.commit()

def file_fingerprint(name: str, size: int) -> str:
    return f"{name}:{size}"

# ── 华东跨省数据汇总 ──────────────────────────────────────────────────────────
def normalize_month(raw: str, year: int) -> tuple[date, date]:
    s = str(raw).strip()
    if re.fullmatch(r"\d{4}", s):
        return date(year, 1, 1), date(year, 12, 31)
    m = re.fullmatch(r"(\d{1,2})-(\d{1,2})月", s)
    if m:
        m1, m2 = int(m.group(1)), int(m.group(2))
        return date(year, m1, 1), date(year, m2, calendar.monthrange(year, m2)[1])
    m = re.fullmatch(r"(\d{1,2})月", s)
    if m:
        m1 = int(m.group(1))
        return date(year, m1, 1), date(year, m1, calendar.monthrange(year, m1)[1])
    raise ValueError(f"unrecognized 标的月份: {raw!r}")

_ANCHOR_TOKENS = ["锡盟","蒙东","蒙西","黑吉辽","黑龙江","吉林","辽宁","山西","河北",
                  "甘肃","青海","宁夏","陕西","新疆","西藏","四川","云南","河南","湖北"]
def anchor_for(raw: str) -> str:
    s = str(raw).strip()
    first = re.split(r"[&/]", s)[0].strip()
    for tok in _ANCHOR_TOKENS:
        if first.startswith(tok):
            return {"黑吉辽": "黑龙江"}.get(tok, tok)
    if "锡盟" in s: return "锡盟"
    if "坤渝" in s: return "重庆"
    if "陕武" in s: return "陕西"
    if "南方" in s: return "云南"
    if "华北" in s: return "山西"
    raise ValueError(f"unmappable 对端送出省份: {raw!r}")

def _clean_ch(v) -> str | None:
    if not isinstance(v, str):
        return None
    v = v.replace("\n", "").strip()
    return v if v and v.lower() != "nan" else None

def _num(v):
    return float(v) if pd.notna(v) else None

_HUADONG_COLS = {"标的月份":"target_month_raw","标的期间类型":"period_type","华东省份-XX":"recv_province",
    "对端送出省份":"send_raw","成交电量（校核前）MWh":"vol_pre_mwh","成交电量（校核后）MWh":"vol_post_mwh",
    "上网侧均价 元/MWh":"send_price","落地侧均价 元/MWh":"land_price",
    "景融成交电量 MWh":"jingrong_vol_mwh","景融成交均价 元/MWh":"jingrong_price","通道费":"channel_fee"}

def parse_huadong(fileobj, year: int) -> list[dict]:
    df = pd.read_excel(fileobj)
    df.columns = [str(c).strip() for c in df.columns]
    out = []
    for _, r in df.iterrows():
        row = {dst: r[src] for src, dst in _HUADONG_COLS.items()}
        ptype = str(row["period_type"]).strip()
        if ptype == "年度":
            ms, me = date(year, 1, 1), date(year, 12, 31)
        else:
            ms, me = normalize_month(row["target_month_raw"], year)
        row.update(month_start=ms, month_end=me, period_type=ptype,
                   recv_province=str(row["recv_province"]).strip(),
                   send_raw=str(row["send_raw"]).strip(),
                   send_anchor=anchor_for(row["send_raw"]),
                   channel_1=_clean_ch(r.get("通道1")), channel_2=_clean_ch(r.get("通道2")),
                   channel_3=_clean_ch(r.get("通道3")))
        for k in ("vol_pre_mwh","vol_post_mwh","send_price","land_price",
                  "jingrong_vol_mwh","jingrong_price","channel_fee"):
            row[k] = _num(row[k])
        out.append(row)
    return out

_TRADES_INSERT = """INSERT INTO staging.interconnector_trades (
    target_month_raw, period_type, month_start, month_end,
    recv_province, send_raw, send_anchor, channel_1, channel_2, channel_3,
    vol_pre_mwh, vol_post_mwh, send_price, land_price,
    jingrong_vol_mwh, jingrong_price, channel_fee, source_file
) VALUES (%(target_month_raw)s, %(period_type)s, %(month_start)s, %(month_end)s,
    %(recv_province)s, %(send_raw)s, %(send_anchor)s, %(channel_1)s, %(channel_2)s, %(channel_3)s,
    %(vol_pre_mwh)s, %(vol_post_mwh)s, %(send_price)s, %(land_price)s,
    %(jingrong_vol_mwh)s, %(jingrong_price)s, %(channel_fee)s, %(source_file)s)"""

def replace_trades(conn, rows: list[dict], source_file: str) -> int:
    """Full replace: the 华东 file is a cumulative snapshot."""
    with conn.cursor() as cur:
        cur.execute("DELETE FROM staging.interconnector_trades")
        cur.executemany(_TRADES_INSERT, [dict(r, source_file=source_file) for r in rows])
    conn.commit()
    return len(rows)
