"""Daily nodal PF batch — computes perfect-foresight BESS revenue for every
node in a province for each date and upserts into reports.nodal_pf_node_daily.

Lives in services/ so it ships inside the mengxi-dashboard image (scripts/ is
not copied). Run as:  python -m services.bess_map.nodal_pf_daily [--date D]
[--start S --end E] [--province P] [--dry-run]

Config fixed: 100 MW / 2h / 85% RTE (matches services.mengxi_nodal.pf_results.CONFIG).
Price source: 蒙西 reads the EnOS-backed view (md_mengxi_nodal_price_96);
other provinces read the Fengxing base table filtered by market_name.
蒙西 nodes belonging to a registered BESS station are additionally constrained
by that station's nomination-file dispatch zones (task 12: red = no charge,
yellow = no discharge); all other nodes stay unconstrained. Stations with a
persisted market-impact estimate (task 13, reports.station_market_impact)
dispatch at impact-adjusted clearing prices (discharge p − λ·Q, charge
p + λ·Q); absent or non-positive λ → pure price-taker, unchanged behavior.
"""
from __future__ import annotations

import argparse
import os
import sys
import time
from datetime import date, timedelta
from pathlib import Path

_REPO = Path(__file__).resolve().parents[2]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

try:
    from dotenv import load_dotenv
    load_dotenv(_REPO / "config" / ".env", override=False)
except ImportError:
    pass

import pandas as pd
from sqlalchemy import create_engine, text

from services.bess_map.optimisation_engine import compute_dispatch_from_15min_prices

POWER_MW, DURATION_H, RTE_PCT = 100.0, 2.0, 85.0
_MIN_NODES = 100  # days with fewer stored rows are considered incomplete

_UPSERT = text("""
    INSERT INTO reports.nodal_pf_node_daily
        (data_date, province, node_name, power_mw, duration_h, rte_pct,
         revenue_cny, charge_mwh, discharge_mwh)
    VALUES (:data_date, :province, :node_name, :power_mw, :duration_h, :rte_pct,
            :revenue_cny, :charge_mwh, :discharge_mwh)
    ON CONFLICT (data_date, province, node_name, power_mw, duration_h, rte_pct)
    DO UPDATE SET revenue_cny  = EXCLUDED.revenue_cny,
                  charge_mwh   = EXCLUDED.charge_mwh,
                  discharge_mwh = EXCLUDED.discharge_mwh,
                  computed_at  = NOW()
""")


def _engine():
    url = os.environ.get("PGURL") or os.environ.get("DB_DSN")
    if not url:
        raise SystemExit("PGURL/DB_DSN not set")
    return create_engine(url, pool_pre_ping=True)


def _price_sql(province: str) -> text:
    if province == "蒙西":
        return text("""
            SELECT node_name, metric_time, time_order_96, avg_node_price
            FROM marketdata.md_mengxi_nodal_price_96
            WHERE metric_time >= :s AND metric_time < :e
            ORDER BY node_name, time_order_96
        """)
    return text("""
        SELECT node_name, metric_time, time_order_96, avg_node_price
        FROM marketdata.md_shanxi_nodal_price_96
        WHERE market_name = :prov AND metric_time >= :s AND metric_time < :e
        ORDER BY node_name, time_order_96
    """)


def _fetch_prices(engine, province: str, day: date) -> pd.DataFrame:
    # Explicit CST bounds: python dates would be coerced at midnight in the DB
    # session TZ (UTC), shifting the window 8h and dropping slots 1-32.
    # The 蒙西 view column is TIMESTAMP (naive CST walls since the v5 fill
    # 2026-10-30): naive CST literals. Other provinces read timestamptz
    # columns: the +08 suffix is required for the correct absolute comparison.
    tz_suffix = "" if province == "蒙西" else "+08"
    s = f"{day} 00:00:00{tz_suffix}"
    e = f"{day + timedelta(days=1)} 00:00:00{tz_suffix}"
    params = {"s": s, "e": e}
    if province != "蒙西":
        params["prov"] = province
    return pd.read_sql(_price_sql(province), engine, params=params)


def _stored_count(engine, province: str, day: date) -> int:
    with engine.connect() as conn:
        return conn.execute(text("""
            SELECT COUNT(*) FROM reports.nodal_pf_node_daily
            WHERE data_date = :d AND province = :p
              AND power_mw = :pw AND duration_h = :dur AND rte_pct = :rte
        """), {"d": day, "p": province, "pw": POWER_MW, "dur": DURATION_H, "rte": RTE_PCT}).scalar()


# 蒙西 discharge-volume compensation (cap-payment rule 2026-11):
# stations on the 名单 → 350 ¥/MWh; any other node → 280 ¥/MWh (the
# post-2026 unlisted default a new asset would receive). Other provinces
# keep 0.0 — their mechanisms are unchanged.
_MENGXI_DEFAULT_SUBSIDY = 280.0


def _expand_price_nodes(nodes: list[str]) -> list[str]:
    """Expand registry node shorthand: ranges 'X.1M–4M' → [X.1M..X.4M];
    bare fragments ('2M') continue the previous full prefix."""
    out, prefix = [], None
    for raw in nodes:
        raw = raw.strip()
        if "–" in raw:
            head, tail = raw.split("–")
            base, first = head.rsplit(".", 1)
            prefix = base
            a, b = int(first.rstrip("M")), int(tail.rstrip("M"))
            out.extend(f"{base}.{i}M" for i in range(a, b + 1))
        elif raw[:1].isdigit() and prefix:
            out.append(f"{prefix}.{raw}")
        else:
            out.append(raw)
            if "." in raw:
                prefix = raw.rsplit(".", 1)[0]
    return out


def _mengxi_node_subsidies() -> dict[str, float]:
    """Listed stations' price nodes (own + parents, expanded) → 350 ¥/MWh."""
    from libs.trading_book.registry import get_stations
    out: dict[str, float] = {}
    for s in get_stations(asset_type="bess"):
        if s.subsidy_cny_per_mwh and s.subsidy_cny_per_mwh > _MENGXI_DEFAULT_SUBSIDY:
            for n in _expand_price_nodes(s.price_nodes):
                out[n] = float(s.subsidy_cny_per_mwh)
    return out


def _mengxi_zone_masks(engine, day: date) -> tuple[dict, list]:
    """Per-node dispatch-restriction masks from the stations' nomination zones
    (task 12): a station's red (no-charge) / yellow (no-discharge) slots apply
    to its registry price nodes (own + expanded parents); every other node
    stays unconstrained. Stations with no zones loaded (registry_only /
    settlement_only, or no nomination file yet) contribute nothing. Nodes
    shared by two stations (谷山梁 parents: gushanliang + hangjinqi) keep the
    FIRST station's mask in registry order.

    Returns (node → (no_charge_slots, no_discharge_slots), stale asset_codes).
    Slot numbers are 1-based, as compute_dispatch_from_15min_prices expects.
    """
    from libs.trading_book.registry import get_stations
    from services.mengxi_nodal.dispatch_zones import get_zones, zones_to_masks
    masks: dict = {}
    stale: list = []
    for s in get_stations(asset_type="bess"):
        zones, is_stale = get_zones(engine, s.asset_code, day)
        if not zones:
            continue
        if is_stale:
            stale.append(s.asset_code)
        m = zones_to_masks(zones)
        for n in _expand_price_nodes(s.price_nodes):
            if n not in masks:
                masks[n] = m
    return masks, stale


def _mengxi_node_impacts(engine) -> dict[str, float]:
    """Per-node market-impact λ (CNY/MWh per MW) from the stations' latest
    persisted estimates (task 13): a station's discharge-regime λ applies to
    its registry price nodes (own + expanded parents). Nodes without an
    estimate — or with a non-positive one (the reverse-causality sign: no
    usable impact signal) — stay price-taker by absence from the map. The
    engine takes a single symmetric λ, so the discharge regime is passed as
    the dominant revenue channel. Nodes shared by two stations keep the
    FIRST station's λ in registry order (same rule as the zone masks).
    """
    from libs.trading_book.registry import get_stations
    from services.mengxi_nodal.market_impact import get_latest_lambdas
    latest = get_latest_lambdas(engine)
    out: dict[str, float] = {}
    for s in get_stations(asset_type="bess"):
        row = latest.get(s.asset_code)
        lam = row.get("lambda_discharge") if row else None
        if lam is None or lam <= 0:
            continue
        for n in _expand_price_nodes(s.price_nodes):
            if n not in out:
                out[n] = float(lam)
    return out


def run_day(engine, province: str, day: date, dry_run: bool = False) -> tuple[int, str]:
    df = _fetch_prices(engine, province, day)
    if df.empty:
        return 0, "no price data"
    nodes = df["node_name"].unique()
    if dry_run:
        return len(nodes), f"DRY-RUN {len(nodes)} nodes"

    subsidies = _mengxi_node_subsidies() if province == "蒙西" else {}
    default_sub = _MENGXI_DEFAULT_SUBSIDY if province == "蒙西" else 0.0
    zone_masks, stale_zones = (
        _mengxi_zone_masks(engine, day) if province == "蒙西" else ({}, []))
    impact_map = _mengxi_node_impacts(engine) if province == "蒙西" else {}

    day_start = pd.Timestamp(day.year, day.month, day.day)
    rows = []
    for node in nodes:
        g = df[df["node_name"] == node].sort_values("time_order_96")
        if len(g) < 96:
            continue
        series = pd.Series(
            g["avg_node_price"].astype(float).values,
            index=day_start + pd.to_timedelta((g["time_order_96"].values - 1) * 15, unit="min"),
        )
        zm = zone_masks.get(node)
        try:
            disp, profit = compute_dispatch_from_15min_prices(
                series, power_mw=POWER_MW, duration_h=DURATION_H,
                roundtrip_eff=RTE_PCT / 100.0,
                compensation_yuan_per_mwh=subsidies.get(node, default_sub),
                no_charge_slots=zm[0] if zm else None,
                no_discharge_slots=zm[1] if zm else None,
                impact_lambda=impact_map.get(node),
            )
            rows.append({
                "data_date": day, "province": province, "node_name": node,
                "power_mw": POWER_MW, "duration_h": DURATION_H, "rte_pct": RTE_PCT,
                "revenue_cny": float(profit.sum()),
                "charge_mwh": float(disp["charge_mw"].sum() * 0.25),
                "discharge_mwh": float(disp["discharge_mw"].sum() * 0.25),
            })
        except Exception:
            continue

    if not rows:
        return 0, "no results"
    with engine.begin() as conn:
        for i in range(0, len(rows), 500):
            conn.execute(_UPSERT, rows[i : i + 500])
    msg = f"{len(rows):,} rows"
    if zone_masks:
        msg += f" ({len(zone_masks)} zone-masked nodes"
        if stale_zones:
            msg += f", stale zones: {','.join(stale_zones)}"
        msg += ")"
    if impact_map:
        msg += f" ({len(impact_map)} impact-adjusted nodes)"
    return len(rows), msg


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default=None)
    ap.add_argument("--start", default=None)
    ap.add_argument("--end", default=None)
    ap.add_argument("--province", default="蒙西")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    if args.date:
        start = end = date.fromisoformat(args.date)
    else:
        end = date.fromisoformat(args.end) if args.end else date.today() - timedelta(days=1)
        start = date.fromisoformat(args.start) if args.start else end

    engine = _engine()
    total_t0 = time.time()
    d = start
    while d <= end:
        n_stored = _stored_count(engine, args.province, d)
        if n_stored >= _MIN_NODES and not args.dry_run:
            print(f"{d}: skip (already {n_stored} rows)", flush=True)
        else:
            t0 = time.time()
            n, msg = run_day(engine, args.province, d, dry_run=args.dry_run)
            print(f"{d}: {msg}  ({time.time()-t0:.0f}s)", flush=True)
        d += timedelta(days=1)
    print(f"DONE in {time.time()-total_t0:.0f}s", flush=True)


if __name__ == "__main__":
    main()
