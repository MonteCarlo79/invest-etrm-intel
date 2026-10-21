"""DB readers for the 跨省绿电 tab (Spec §6 DB reads — read-only, no writes).

All functions are conn-injected: pass any DBAPI connection with a
context-manager cursor (psycopg2 raw connection in the app, a fake conn in
tests). SQL is kept verbatim per the Task-4 interface contract:

- load_rt_hourly: trailing-`months` RT series for one province → [(datetime, float)]
- load_trade_ttm: trailing-12-month post-check volume per channel_1, converted
  MWh → 亿kWh (1 亿kWh = 1e5 MWh) → {channel_1: 亿kWh}
- load_gov_agreements: extracted government-agreement rows (S5a secondary layer)
"""

from __future__ import annotations


def load_rt_hourly(conn, province: str, months: int = 12) -> list[tuple]:
    """Trailing-`months` hourly RT rows for a province → [(datetime, rt_price)]."""
    sql = (
        "SELECT datetime, rt_price FROM marketdata.spot_prices_hourly "
        "WHERE province = %s AND datetime >= now() - make_interval(months => %s) "
        "ORDER BY datetime"
    )
    with conn.cursor() as cur:
        cur.execute(sql, (province, months))
        return cur.fetchall()


def load_trade_ttm(conn) -> dict[str, float]:
    """Trailing-12-month volume per channel → {channel_1: 亿kWh} (MWh ÷ 1e5)."""
    sql = (
        "SELECT channel_1, SUM(vol_post_mwh) FROM staging.interconnector_trades "
        "WHERE month_start >= now() - interval '12 months' GROUP BY channel_1"
    )
    with conn.cursor() as cur:
        cur.execute(sql)
        rows = cur.fetchall()
    return {ch: float(vol or 0.0) / 1e5 for ch, vol in rows}


def load_gov_agreements(conn) -> list[tuple]:
    """Extracted agreement rows → (send_prov, recv_prov, annual_gwh, period,
    channel_hint, source), newest period first (S5a 历史协议提取 layer)."""
    sql = (
        "SELECT send_prov, recv_prov, annual_gwh, period, channel_hint, source "
        "FROM staging.interconnector_gov_agreements ORDER BY period DESC"
    )
    with conn.cursor() as cur:
        cur.execute(sql)
        return cur.fetchall()
