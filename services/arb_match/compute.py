"""Stage 2 arbitrage matching: price actual dispatch at RT nodal prices.

Settlement pricing rule (confirmed by user 2026-08-29):
  - no DA component: DA nodal prices are NOT used for settlement
  - charging energy is settled at the HOURLY AVERAGE of the 15-min nodal prices
    (basis: trading practice, not the rules-doc per-interval reading)
  - discharging energy is settled at the 15-MIN nodal price
  Verified: with correct timezone alignment this reproduces invoice effective
  prices within ±6% on all four well-covered assets (2026-06).
Price rows are timestamped at period END; dispatch interval_start at period START,
so interval t prices at t + 15min.

TIMEZONE: rm_dispatch_chain.interval_start is timestamptz (UTC instants); the price
series are Beijing-wall naive timestamps. `interval_start::timestamp` is session-TZ
dependent — under a UTC session it reads the UTC wall clock and prices every
interval 8 HOURS LATE (root cause of the 2026-08 price mismatch: charge ~+40%,
discharge ~-35%). Always convert explicitly with AT TIME ZONE 'Asia/Shanghai'.

Price sources per asset:
  'nodal'   — marketdata.md_rt_nodal_price.node_price (enos ingest, full 2026)
  'cleared' — marketdata.md_id_cleared_energy.cleared_price; verified identical
              to node_price where both exist （景蓝乌尔图 2026-05-01) and used for
              四子王旗 whose node is missing from the 2026 nodal ingest.
"""
from __future__ import annotations

import pandas as pd

# rm_assets.name → (source_kind, source_key)
PRICE_SOURCES = {
    "景蓝乌尔图": ("nodal", "内蒙.景蓝乌尔图储能电站/220kV.1M"),
    "悦杭独贵": ("nodal", "内蒙.悦杭独贵储能电站/220kV.1M"),
    "景怡查干哈达": ("nodal", "内蒙.景怡查干哈达储能电站/220kV.1M"),
    "裕昭沙子坝": ("nodal", "内蒙.裕昭沙子坝储能电站/220kV.1M"),
    "远景乌拉特": ("nodal", "内蒙.远景乌拉特储能电站/220kV.1M"),
    "四子王旗": ("cleared", "景通四益堂储能电站"),
}


def _load_dispatch(cur, asset_id: int, start: str, end: str) -> pd.DataFrame:
    cur.execute("""
        SELECT (interval_start AT TIME ZONE 'Asia/Shanghai') AS ts, actual_mw
        FROM marketdata.rm_dispatch_chain
        WHERE asset_id = %s AND actual_mw IS NOT NULL
          AND interval_start >= %s AND interval_start < %s
        ORDER BY interval_start
    """, (asset_id, start, end))
    rows = cur.fetchall()
    return pd.DataFrame(rows, columns=["ts", "actual_mw"])


def _load_prices(cur, kind: str, key: str, start: str, end: str) -> pd.Series:
    if kind == "nodal":
        cur.execute("""
            SELECT datetime, node_price FROM marketdata.md_rt_nodal_price
            WHERE node_name = %s AND datetime >= %s::timestamp
              AND datetime < %s::timestamp + interval '1 day'
            ORDER BY datetime
        """, (key, start, end))
    else:
        cur.execute("""
            SELECT datetime, cleared_price FROM marketdata.md_id_cleared_energy
            WHERE plant_name = %s AND datetime >= %s::timestamp
              AND datetime < %s::timestamp + interval '1 day'
            ORDER BY datetime
        """, (key, start, end))
    rows = cur.fetchall()
    if not rows:
        return pd.Series(dtype=float)
    s = pd.Series({pd.Timestamp(r[0]): float(r[1]) for r in rows if r[1] is not None})
    return s[~s.index.duplicated(keep="last")].sort_index()


def compute_day(dispatch: pd.DataFrame, prices: pd.Series) -> dict | None:
    """Compute one asset-day's modeled arbitrage numbers."""
    if dispatch.empty:
        return None
    p_end = dispatch["ts"] + pd.Timedelta(minutes=15)
    p15 = p_end.map(prices)  # NaN where no price row

    actual = dispatch["actual_mw"].astype(float)
    charge_mwh = (-actual.clip(upper=0)).sum() * 0.25
    discharge_mwh = actual.clip(lower=0).sum() * 0.25

    priced = p15.notna()
    # Hourly average price of the clock hour the interval starts in
    hourly = prices.groupby(prices.index.floor("h")).mean()

    def _hourly_price(ts):
        return hourly.get(ts.floor("h"), float("nan"))

    charge_cost = 0.0
    discharge_rev = 0.0
    for ts, mw, p, ok in zip(dispatch["ts"], actual, p15, priced):
        if not ok:
            continue
        if mw < 0:
            hp = _hourly_price(ts)
            if hp == hp:  # not NaN
                charge_cost += -mw * 0.25 * hp
        elif mw > 0:
            discharge_rev += mw * 0.25 * p

    return {
        "charge_mwh": round(float(charge_mwh), 4),
        "discharge_mwh": round(float(discharge_mwh), 4),
        "modeled_charge_cost_cny": round(float(-charge_cost), 2),
        "modeled_discharge_rev_cny": round(float(discharge_rev), 2),
        "intervals_actual": int(len(dispatch)),
        "intervals_priced": int(priced.sum()),
    }


def compute_range(start: str, end: str, assets: list[str] | None = None, verbose: bool = True) -> dict:
    """Compute rm_arb_match_daily for every asset with a price source, [start, end)."""
    from shared.agents.db import get_conn

    report = {}
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("SET statement_timeout = '300s'")
        for asset_name, (kind, key) in PRICE_SOURCES.items():
            if assets and asset_name not in assets:
                continue
            cur.execute("SELECT id FROM marketdata.rm_assets WHERE name = %s", (asset_name,))
            row = cur.fetchone()
            if not row:
                report[asset_name] = "asset not found"
                continue
            asset_id = row[0]

            dispatch = _load_dispatch(cur, asset_id, start, end)
            if dispatch.empty:
                report[asset_name] = "no dispatch"
                continue
            prices = _load_prices(cur, kind, key, start, end)

            written = 0
            for date, day_df in dispatch.groupby(dispatch["ts"].dt.date):
                day_prices = prices  # series already bounded to range; map() is O(1) per key
                metrics = compute_day(day_df.reset_index(drop=True), day_prices)
                if not metrics:
                    continue
                cur.execute("""
                    INSERT INTO marketdata.rm_arb_match_daily
                        (asset_id, date, charge_mwh, discharge_mwh,
                         modeled_charge_cost_cny, modeled_discharge_rev_cny,
                         intervals_actual, intervals_priced, price_source)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (asset_id, date) DO UPDATE SET
                        charge_mwh = EXCLUDED.charge_mwh,
                        discharge_mwh = EXCLUDED.discharge_mwh,
                        modeled_charge_cost_cny = EXCLUDED.modeled_charge_cost_cny,
                        modeled_discharge_rev_cny = EXCLUDED.modeled_discharge_rev_cny,
                        intervals_actual = EXCLUDED.intervals_actual,
                        intervals_priced = EXCLUDED.intervals_priced,
                        price_source = EXCLUDED.price_source,
                        computed_at = NOW()
                """, (asset_id, date, metrics["charge_mwh"], metrics["discharge_mwh"],
                      metrics["modeled_charge_cost_cny"], metrics["modeled_discharge_rev_cny"],
                      metrics["intervals_actual"], metrics["intervals_priced"],
                      f"{kind}:{key}"))
                written += 1
            conn.commit()
            report[asset_name] = written
            if verbose:
                print(f"[arb_match] {asset_name}: {written} days", flush=True)
    return report
