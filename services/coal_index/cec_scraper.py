# services/coal_index/cec_scraper.py
"""CEC (China Electricity Council) coal index scraper.

Source: https://cec.org.cn/ms-mcms/mcms/zdlzs — open JSON behind
https://cec.org.cn/dmzs/index.html (webUrl resolved via the page's g_index.js).

Series stored (long format in marketdata.coal_index_daily):
  caofeidian_5500/5000/4500   — 曹妃甸指数 规格品, ¥/t, business-daily
  ceci_synth_5500/5000        — 沿海电煤指数 综合价, ¥/t, weekly
  ceci_deal_5500/5000         — 沿海电煤指数 成交价, ¥/t, weekly
  ceci_fob_5500/5000          — 沿海电煤指数 离岸价, ¥/t, weekly
  spec_7000                   — 规格品7000, ¥/t, weekly
  ceci_composite/supply/demand/inventory/price/shipping — 采购经理人指数, weekly

Entry points:
    run_daily(pg_url, feishu=None, owner_open_id="") -> dict
    latest_coal_price(eng, index="ceci_synth_5500") -> float | None
"""
from __future__ import annotations

import logging
from datetime import date

import requests

logger = logging.getLogger(__name__)

SOURCE_URL = "https://cec.org.cn/ms-mcms/mcms/zdlzs"
_REFERER = "https://cec.org.cn/dmzs/index.html"
_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"

_DDL = """
CREATE TABLE IF NOT EXISTS marketdata.coal_index_daily (
    index_name TEXT NOT NULL,
    date DATE NOT NULL,
    value_yuan_t NUMERIC NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (index_name, date)
);
"""

_UPSERT = """
INSERT INTO marketdata.coal_index_daily (index_name, date, value_yuan_t)
VALUES (%s, %s, %s)
ON CONFLICT (index_name, date) DO UPDATE SET
    value_yuan_t = EXCLUDED.value_yuan_t,
    updated_at = NOW()
"""

# series position → [(data_index, index_name)]
_SERIES_MAP: dict[int, list[tuple[int, str]]] = {
    0: [(0, "caofeidian_5500"), (1, "caofeidian_5000"), (2, "caofeidian_4500")],
    1: [(0, "ceci_synth_5500"), (1, "ceci_synth_5000"),
        (2, "ceci_deal_5500"), (3, "ceci_deal_5000"),
        (4, "ceci_fob_5500"), (5, "ceci_fob_5000")],
    2: [(0, "spec_7000")],
    3: [(0, "ceci_composite"), (1, "ceci_supply"), (2, "ceci_demand"),
        (3, "ceci_inventory"), (4, "ceci_price"), (5, "ceci_shipping")],
}

_MOVE_ALERT_PCT = 5.0  # Feishu alert when caofeidian_5500 moves >5% vs previous row


def _ensure_table(cur) -> None:
    cur.execute(_DDL)


def fetch_index(url: str = SOURCE_URL, timeout: int = 30) -> dict:
    """Fetch the raw zdlzs JSON. Raises on HTTP/shape errors."""
    resp = requests.get(url, headers={"User-Agent": _UA, "Referer": _REFERER}, timeout=timeout)
    resp.raise_for_status()
    payload = resp.json()
    if not payload.get("success"):
        raise RuntimeError(f"zdlzs returned success=false: {str(payload)[:200]}")
    return payload


def parse_rows(payload: dict) -> list[tuple[str, date, float]]:
    """Flatten the 4 series into (index_name, date, value) rows."""
    rows: list[tuple[str, date, float]] = []
    series = payload["data"]["list"]
    for pos, mapping in _SERIES_MAP.items():
        if pos >= len(series):
            continue
        for item in series[pos]["list"]:
            d = date.fromisoformat(item["nper"])
            for data_idx, name in mapping:
                try:
                    v = float(item["data"][data_idx])
                except (IndexError, TypeError, ValueError):
                    continue
                rows.append((name, d, v))
    return rows


def upsert_rows(cur, rows: list[tuple[str, date, float]]) -> int:
    _ensure_table(cur)
    for name, d, v in rows:
        cur.execute(_UPSERT, (name, d, v))
    return len(rows)


def _prev_two(cur, index_name: str) -> list[tuple[date, float]]:
    cur.execute(
        "SELECT date, value_yuan_t FROM marketdata.coal_index_daily "
        "WHERE index_name = %s ORDER BY date DESC LIMIT 2",
        (index_name,),
    )
    return [(r[0], float(r[1])) for r in cur.fetchall()]


def run_daily(pg_url: str, feishu=None, owner_open_id: str = "") -> dict:
    """Fetch + upsert; Feishu alert on failure or >5% move in caofeidian_5500."""
    import psycopg2

    try:
        payload = fetch_index()
    except Exception as exc:
        logger.error("coal_index: fetch failed: %s", exc)
        if feishu and owner_open_id:
            feishu.send_text(open_id=owner_open_id,
                             text=f"⚠️ CEC 电煤指数抓取失败：{exc}")
        return {"ok": False, "error": str(exc)}

    rows = parse_rows(payload)
    conn = psycopg2.connect(pg_url)
    try:
        with conn.cursor() as cur:
            before = _prev_two(cur, "caofeidian_5500")
            n = upsert_rows(cur, rows)
            after = _prev_two(cur, "caofeidian_5500")
        conn.commit()
    finally:
        conn.close()

    move = None
    if len(after) == 2 and after[1][1] > 0:
        move = (after[0][1] - after[1][1]) / after[1][1] * 100.0
        if abs(move) > _MOVE_ALERT_PCT and feishu and owner_open_id:
            feishu.send_text(
                open_id=owner_open_id,
                text=(f"📈 曹妃甸5500电煤 {after[0][1]:.0f} 元/吨 "
                      f"({move:+.1f}%，前次 {after[1][1]:.0f})，"
                      f"沿海综合5500 最新 {after and _safe_latest(pg_url) or '-'}"),
            )
    latest_date = max(d for _, d, _ in rows)
    logger.info("coal_index: upserted %d rows (latest data %s, caofeidian_5500 move %s)",
                n, latest_date, f"{move:+.2f}%" if move is not None else "n/a")
    return {"ok": True, "rows": n, "latest_date": str(latest_date),
            "caofeidian_5500_move_pct": move}


def _safe_latest(pg_url: str) -> str:
    import psycopg2
    try:
        conn = psycopg2.connect(pg_url)
        with conn.cursor() as cur:
            cur.execute(
                "SELECT value_yuan_t FROM marketdata.coal_index_daily "
                "WHERE index_name = 'ceci_synth_5500' ORDER BY date DESC LIMIT 1")
            r = cur.fetchone()
        conn.close()
        return f"{float(r[0]):.0f}" if r else "-"
    except Exception:
        return "-"


def latest_coal_price(eng, index: str = "ceci_synth_5500") -> float | None:
    """Latest value of an index — used by the merit-order explorer default."""
    from sqlalchemy import text as _t
    with eng.connect() as conn:
        r = conn.execute(
            _t("SELECT value_yuan_t FROM marketdata.coal_index_daily "
               "WHERE index_name = :i ORDER BY date DESC LIMIT 1"),
            {"i": index},
        ).fetchone()
    return float(r[0]) if r else None
