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
        CONSTRAINT ic_mlt_snapshot_uq
            UNIQUE(snapshot_label, sheet, send_region, send_prov, recv_prov, trade_type, channel)
    )""",
    """CREATE TABLE IF NOT EXISTS staging.interconnector_mlt_pct_override (
        province TEXT PRIMARY KEY, pct NUMERIC, updated_at TIMESTAMPTZ DEFAULT NOW()
    )""",
    """CREATE TABLE IF NOT EXISTS staging.interconnector_gov_agreements (
        id SERIAL PRIMARY KEY,
        send_prov TEXT, recv_prov TEXT, annual_gwh NUMERIC, period TEXT,
        channel_hint TEXT, source TEXT, note TEXT,
        updated_at TIMESTAMPTZ DEFAULT NOW(),
        UNIQUE(send_prov, recv_prov, period)
    )""",
    """CREATE TABLE IF NOT EXISTS staging.interconnector_mech_share_override (
        province TEXT PRIMARY KEY, share_pct NUMERIC, updated_at TIMESTAMPTZ DEFAULT NOW()
    )""",
]

# The original snapshot UNIQUE key omitted send_region; the same sending base
# (e.g. 锡盟二期) legitimately appears under two grid regions (华北/蒙西) with
# different volumes/prices — widening the key to include send_region. Idempotent.
_MIGRATE_SNAPSHOT_UQ = """
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'ic_mlt_snapshot_uq') THEN
        ALTER TABLE staging.interconnector_mlt_snapshot
            DROP CONSTRAINT IF EXISTS interconnector_mlt_snapshot_snapshot_label_sheet_send_prov__key;
        ALTER TABLE staging.interconnector_mlt_snapshot
            ADD CONSTRAINT ic_mlt_snapshot_uq
            UNIQUE (snapshot_label, sheet, send_region, send_prov, recv_prov, trade_type, channel);
    END IF;
END $$;
"""

def ensure_tables(conn) -> None:
    with conn.cursor() as cur:
        for stmt in DDL:
            cur.execute(stmt)
        cur.execute(_MIGRATE_SNAPSHOT_UQ)
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


# ── 政府间协议电量 (inter-government annual volume agreements) ───────────────
# Seeded from public reports 2026-09-12 — user verifies/edits in the tab.
AGREEMENT_SEED = [
    dict(send_prov="青海", recv_prov="上海", annual_gwh=4000.0, period="2026-2028",
         channel_hint="青豫/灵绍/庆东", source="人民日报 2026-06-15",
         note="青电入沪绿电长协: 40亿kWh/年×3年=120亿; 待核实"),
    dict(send_prov="山西", recv_prov="上海", annual_gwh=5000.0, period="2023-2025",
         channel_hint="雁淮", source="电力网 2023-11-29",
         note="晋电入沪3年150亿kWh(≈50亿/年); 待核实"),
    dict(send_prov="云南", recv_prov="广东", annual_gwh=153300.0, period="2026",
         channel_hint="云广/昆柳龙", source="北极星 2025-12-08",
         note="云南西电东送2026优先计划1533亿kWh(含多受端); 待核实"),
    dict(send_prov="新疆", recv_prov="重庆", annual_gwh=8000.0, period="2026",
         channel_hint="坤渝", source="重庆市政府 2025-05-21",
         note="疆电入渝2026预计>80亿kWh; 待核实"),
]

_AGR_INSERT = """INSERT INTO staging.interconnector_gov_agreements (
    send_prov, recv_prov, annual_gwh, period, channel_hint, source, note
) VALUES (%(send_prov)s, %(recv_prov)s, %(annual_gwh)s, %(period)s, %(channel_hint)s,
    %(source)s, %(note)s)
ON CONFLICT (send_prov, recv_prov, period) DO UPDATE SET
    annual_gwh=EXCLUDED.annual_gwh, channel_hint=EXCLUDED.channel_hint,
    source=EXCLUDED.source, note=EXCLUDED.note, updated_at=NOW()"""

def get_agreements(conn) -> list[dict]:
    with conn.cursor() as cur:
        cur.execute("""SELECT send_prov, recv_prov, annual_gwh, period,
                              channel_hint, source, note
                       FROM staging.interconnector_gov_agreements
                       ORDER BY annual_gwh DESC NULLS LAST""")
        cols = ["send_prov","recv_prov","annual_gwh","period","channel_hint","source","note"]
        return [dict(zip(cols, r)) for r in cur.fetchall()]

def save_agreements(conn, rows: list[dict]) -> int:
    """Full replace from the tab's data editor."""
    with conn.cursor() as cur:
        cur.execute("BEGIN")
        try:
            cur.execute("DELETE FROM staging.interconnector_gov_agreements")
            cur.executemany(_AGR_INSERT, rows)
            cur.execute("COMMIT")
        except Exception:
            cur.execute("ROLLBACK")
            raise
    return len(rows)

def seed_agreements_if_empty(conn) -> int:
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM staging.interconnector_gov_agreements")
        if cur.fetchone()[0] > 0:
            return 0
        cur.executemany(_AGR_INSERT, AGREEMENT_SEED)
    conn.commit()
    return len(AGREEMENT_SEED)


# ── 机制电量占比 (136号文 mechanism share of renewable volume) ───────────────
# Seeded from data/136号文件 extraction (knowledge/interconnectors/mechanism_136.md).
# share_pct = % of a province's renewable volume locked under the price mechanism;
# market-seeking renewable = total × (1 − share/100). User edits in the tab.
MECH_SHARE_SEED = [
    dict(province="新疆", share_pct=62.5),   # 风电项目级 62.5% (4省1017表)
    dict(province="甘肃", share_pct=8.0),    # 推导约8% (无公示)
    dict(province="河北", share_pct=80.0),   # 项目级均值 80%
    dict(province="安徽", share_pct=73.9),   # 项目级均值 73.9%
    dict(province="山东", share_pct=70.0),   # 风电 70%
    dict(province="云南", share_pct=65.0),   # 风电 ~65% (光伏 75%)
    dict(province="四川", share_pct=80.0),   # 风电 80%
    dict(province="陕西", share_pct=49.8),   # 项目级 49.8%
]

_MECH_INSERT = """INSERT INTO staging.interconnector_mech_share_override (province, share_pct)
    VALUES (%(province)s, %(share_pct)s)
    ON CONFLICT (province) DO UPDATE SET share_pct=EXCLUDED.share_pct, updated_at=NOW()"""

def get_mech_shares(conn) -> dict[str, float]:
    with conn.cursor() as cur:
        cur.execute("SELECT province, share_pct FROM staging.interconnector_mech_share_override")
        return {p: float(v) for p, v in cur.fetchall()}

def set_mech_share(conn, province: str, share_pct: float) -> None:
    with conn.cursor() as cur:
        cur.execute(_MECH_INSERT, dict(province=province, share_pct=share_pct))
    conn.commit()

def seed_mech_share_if_empty(conn) -> int:
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM staging.interconnector_mech_share_override")
        if cur.fetchone()[0] > 0:
            return 0
        cur.executemany(_MECH_INSERT, MECH_SHARE_SEED)
    conn.commit()
    return len(MECH_SHARE_SEED)
