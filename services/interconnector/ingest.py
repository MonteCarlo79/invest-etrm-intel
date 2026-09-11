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
        try:
            cur.execute("BEGIN")
            cur.execute("DELETE FROM staging.interconnector_trades")
            cur.executemany(_TRADES_INSERT, [dict(r, source_file=source_file) for r in rows])
            cur.execute("COMMIT")
        except Exception:
            cur.execute("ROLLBACK")
            raise
    return len(rows)

# ── 跨区组织交易情况 snapshot ─────────────────────────────────────────────────
def _is_subtotal(trade_type, channel, send_prov, recv_prov) -> bool:
    return any("汇总" in str(x) for x in (trade_type, channel, send_prov, recv_prov))

def parse_mlt_snapshot(fileobj, sheet: str) -> list[dict]:
    df = pd.read_excel(fileobj, sheet_name=sheet)
    df.columns = [str(c).strip() for c in df.columns]
    out = []
    for _, r in df.iterrows():
        send_prov = str(r["送出省份"]).strip()
        recv_prov = str(r["受入省份"]).strip()
        if send_prov.lower() == "nan" or recv_prov.lower() == "nan":
            continue
        tt = str(r["交易类型2"]).strip()
        ch = _clean_ch(r["输电通道"])
        out.append(dict(sheet=sheet,
            send_region=str(r["送出区域"]).strip(), send_prov=send_prov, recv_prov=recv_prov,
            trade_type=tt.replace(" 汇总", ""), channel=ch,
            is_subtotal=_is_subtotal(tt, ch, send_prov, recv_prov),
            volume_100m_kwh=_num(r["落地均价（亿千瓦时）"]), landing_price=_num(r["落地均价"])))
    return out

_SNAP_INSERT = """INSERT INTO staging.interconnector_mlt_snapshot (
    snapshot_label, sheet, send_region, send_prov, recv_prov,
    trade_type, channel, is_subtotal, volume_100m_kwh, landing_price
) VALUES (%(snapshot_label)s, %(sheet)s, %(send_region)s, %(send_prov)s, %(recv_prov)s,
    %(trade_type)s, %(channel)s, %(is_subtotal)s, %(volume_100m_kwh)s, %(landing_price)s)"""

def replace_snapshot(conn, label: str, rows: list[dict]) -> int:
    with conn.cursor() as cur:
        try:
            cur.execute("BEGIN")
            cur.execute("DELETE FROM staging.interconnector_mlt_snapshot WHERE snapshot_label = %s", (label,))
            cur.executemany(_SNAP_INSERT, [dict(r, snapshot_label=label) for r in rows])
            cur.execute("COMMIT")
        except Exception:
            cur.execute("ROLLBACK")
            raise
    return len(rows)

# ── channels + MLT% overrides ─────────────────────────────────────────────────
_CHANNEL_UPSERT = """INSERT INTO staging.interconnector_channels (
    name, formal_name, send_station, send_prov, send_lon, send_lat,
    recv_station, recv_prov, recv_lon, recv_lat, kv, gw, commissioned, km, category, note
) VALUES (%(name)s, %(formal)s, %(send)s, %(send_prov)s, %(send_lon)s, %(send_lat)s,
    %(recv)s, %(recv_prov)s, %(recv_lon)s, %(recv_lat)s, %(kv)s, %(gw)s, %(commissioned)s, %(km)s,
    %(category)s, %(note)s)
ON CONFLICT (name) DO UPDATE SET
    formal_name=EXCLUDED.formal_name, send_station=EXCLUDED.send_station,
    send_prov=EXCLUDED.send_prov, send_lon=EXCLUDED.send_lon, send_lat=EXCLUDED.send_lat,
    recv_station=EXCLUDED.recv_station, recv_prov=EXCLUDED.recv_prov,
    recv_lon=EXCLUDED.recv_lon, recv_lat=EXCLUDED.recv_lat, kv=EXCLUDED.kv, gw=EXCLUDED.gw,
    commissioned=EXCLUDED.commissioned, km=EXCLUDED.km, category=EXCLUDED.category,
    note=EXCLUDED.note, updated_at=NOW()"""

def upsert_channels(conn, channels: list[dict]) -> int:
    params = [dict(c, send_lon=c["sc"][0], send_lat=c["sc"][1],
                   recv_lon=c["rc"][0], recv_lat=c["rc"][1]) for c in channels]
    with conn.cursor() as cur:
        cur.executemany(_CHANNEL_UPSERT, params)
    conn.commit()
    return len(channels)

def set_pct_override(conn, province: str, pct: float) -> None:
    with conn.cursor() as cur:
        cur.execute("""INSERT INTO staging.interconnector_mlt_pct_override (province, pct)
                       VALUES (%s, %s)
                       ON CONFLICT (province) DO UPDATE SET pct=EXCLUDED.pct, updated_at=NOW()""",
                    (province, pct))
    conn.commit()

def get_pct_overrides(conn) -> dict[str, float]:
    with conn.cursor() as cur:
        cur.execute("SELECT province, pct FROM staging.interconnector_mlt_pct_override")
        return {p: float(v) for p, v in cur.fetchall()}
