# -*- coding: utf-8 -*-
"""Monthly settlement replication report — 零碳46 vs book 6 bills.

Usage:
    python -m services.wind_settlement.report [--months 2026-01..2026-07]
"""
from __future__ import annotations

import argparse
import os
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine

from services.wind_settlement.replicate import (
    capture_price,
    cfd_value,
    green_premium,
    green_value,
    load_bill_items,
    load_clearing,
    load_intervals,
    load_ref_price_avg,
)
from services.wind_settlement.rules import UNIFIED_FROM, cfd_zones
from services.wind_settlement.trades import monthly_position, parse_trades_file

PLANT = "悦盛昌渠风光储电站"
NODE = "内蒙.悦盛昌渠风光储电站/220kV.1M"
BOOK_ID = 6

TRADES_DIR = Path(
    "data/raw/零碳46交易数据&月度复盘/零碳46交易数据（2026年1~7月）"
)


def _load_env() -> None:
    env = Path("config/.env")
    if env.exists():
        for line in env.read_text().splitlines():
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip())


def _trades_files(month: str) -> tuple[Path | None, Path | None]:
    intra = sorted((TRADES_DIR / "3.省内成交单分月总表").glob(f"省内-全月成交单-*{month}.xlsx"))
    cross = sorted((TRADES_DIR / "2.跨省成交单分月总表").glob(f"跨省-全月成交单-*{month}.xlsx"))
    return (intra[0] if intra else None, cross[0] if cross else None)


def replicate_month(engine, month: str) -> dict:
    intra_p, cross_p = _trades_files(month)
    intra = parse_trades_file(intra_p) if intra_p else pd.DataFrame()
    cross = parse_trades_file(cross_p) if cross_p else pd.DataFrame()
    pos = monthly_position(intra, cross)

    iv = load_intervals(engine, PLANT, NODE, month)
    cl = load_clearing(engine, month)
    bill = load_bill_items(engine, BOOK_ID, month)

    # exchange clearing is authoritative when present; ID-cleared proxy is fallback
    if not cl.empty:
        gen_mwh = float(cl["metered_mwh"].sum())
        spot_value = float((cl["metered_mwh"] * cl["rt_nodal_price"]).sum())
        cfd_exchange = float(cl["energy_fee"].sum()) - spot_value
        curve_min_mwh = float(cl["curve_min"].sum())
        cap = spot_value / gen_mwh if gen_mwh else None
        contract_vol_clearing = float(cl["contract_mwh"].sum())
    else:
        gen_mwh = float(iv["gen_mwh"].sum())
        cap = capture_price(iv.dropna(subset=["rt_price"]))
        spot_value = float((iv["gen_mwh"] * iv["rt_price"]).sum())
        cfd_exchange = None
        curve_min_mwh = None
        contract_vol_clearing = None
    proxy_mwh = float(iv["gen_mwh"].sum())

    ref_west = load_ref_price_avg(engine, month, "呼包以西加权平均价格_元_mwh")
    ref_east = load_ref_price_avg(engine, month, "呼包以东加权平均价格_元_mwh")
    ref_sys = load_ref_price_avg(engine, month, "system_settlement_price")

    contract_vol = float(pos["volume_mwh"].sum()) if not pos.empty else 0.0
    cfd_west = cfd_value(pos, ref_west) if ref_west and not pos.empty else None
    cfd_sys = cfd_value(pos, ref_sys) if ref_sys and not pos.empty else None
    cfd_zone = None
    if ref_east and ref_west and ref_sys and not (intra.empty and cross.empty):
        cfd_zone = cfd_zones(intra, cross, ref_east, ref_west, ref_sys,
                             unified=(month >= UNIFIED_FROM))
    green = green_value(pos) if not pos.empty else None
    green_min = None
    if not pos.empty and not iv.empty:
        iv_e = iv.dropna(subset=["gen_mwh"])
        green_min = green_premium(pos, iv_e[["datetime", "gen_mwh"]], month)

    # bill targets
    spot_rows = bill[(bill["category"] == "discharge_energy") & (bill["notes"].str.contains("现货", na=False))]
    bill_vol = float(spot_rows["volume_mwh"].dropna().sum())
    bill_amt = float(spot_rows["amount_cny"].dropna().sum())
    green_rows = bill[bill["notes"].str.contains("绿电|填电", na=False)]
    bill_green = float(green_rows["amount_cny"].dropna().sum())
    fees = bill[~bill.index.isin(spot_rows.index.union(green_rows.index))]
    bill_fees = float(fees["amount_cny"].dropna().sum())
    bill_total = float(bill["amount_cny"].dropna().sum())

    implied_cfd = bill_amt - spot_value  # bill 现货电费 − spot value at nodal

    return {
        "month": month,
        "gen_proxy_mwh": proxy_mwh,
        "metered_mwh": gen_mwh,
        "bill_vol_mwh": bill_vol,
        "vol_ratio": gen_mwh / bill_vol if bill_vol else None,
        "data_days": int(iv["datetime"].dt.date.nunique()) if not iv.empty else 0,
        "capture_price": cap,
        "spot_value_cny": spot_value,
        "bill_spot_cny": bill_amt,
        "cfd_exchange_cny": cfd_exchange,
        "curve_min_mwh": curve_min_mwh,
        "implied_cfd_cny": implied_cfd,
        "contract_vol_mwh": contract_vol,
        "contract_vol_clearing_mwh": contract_vol_clearing,
        "ref_west": ref_west,
        "ref_east": ref_east,
        "ref_sys": ref_sys,
        "cfd_west_cny": cfd_west,
        "cfd_sys_cny": cfd_sys,
        "cfd_zone_cny": cfd_zone,
        "green_cny": green,
        "green_min_cny": green_min,
        "bill_green_cny": bill_green,
        "bill_fees_cny": bill_fees,
        "bill_total_cny": bill_total,
    }


def persist(engine, rows: list[dict]) -> None:
    """Upsert replication results into marketdata.wind_settlement_monthly."""
    from sqlalchemy import text

    ddl = Path("db/ddl/marketdata/wind_settlement_monthly.sql")
    upsert = text("""
        INSERT INTO marketdata.wind_settlement_monthly (
            asset_name, settle_month, gen_proxy_mwh, metered_mwh, bill_vol_mwh,
            capture_price, spot_value_cny, bill_spot_cny, cfd_exchange_cny,
            curve_min_mwh, implied_cfd_cny,
            contract_vol_mwh, contract_vol_clearing_mwh, ref_price_west, ref_price_east, ref_price_sys,
            cfd_west_cny, cfd_sys_cny, cfd_zone_cny, green_cny, green_min_cny, bill_green_cny,
            bill_fees_cny, bill_total_cny
        ) VALUES (
            :asset, :month, :gen_proxy_mwh, :metered_mwh, :bill_vol_mwh,
            :capture_price, :spot_value_cny, :bill_spot_cny, :cfd_exchange_cny,
            :curve_min_mwh, :implied_cfd_cny,
            :contract_vol_mwh, :contract_vol_clearing_mwh, :ref_west, :ref_east, :ref_sys,
            :cfd_west_cny, :cfd_sys_cny, :cfd_zone_cny, :green_cny, :green_min_cny, :bill_green_cny,
            :bill_fees_cny, :bill_total_cny
        )
        ON CONFLICT (asset_name, settle_month) DO UPDATE SET
            gen_proxy_mwh = EXCLUDED.gen_proxy_mwh,
            metered_mwh = EXCLUDED.metered_mwh,
            bill_vol_mwh = EXCLUDED.bill_vol_mwh,
            capture_price = EXCLUDED.capture_price,
            spot_value_cny = EXCLUDED.spot_value_cny,
            bill_spot_cny = EXCLUDED.bill_spot_cny,
            cfd_exchange_cny = EXCLUDED.cfd_exchange_cny,
            curve_min_mwh = EXCLUDED.curve_min_mwh,
            implied_cfd_cny = EXCLUDED.implied_cfd_cny,
            contract_vol_mwh = EXCLUDED.contract_vol_mwh,
            contract_vol_clearing_mwh = EXCLUDED.contract_vol_clearing_mwh,
            ref_price_west = EXCLUDED.ref_price_west,
            ref_price_east = EXCLUDED.ref_price_east,
            ref_price_sys = EXCLUDED.ref_price_sys,
            cfd_west_cny = EXCLUDED.cfd_west_cny,
            cfd_sys_cny = EXCLUDED.cfd_sys_cny,
            cfd_zone_cny = EXCLUDED.cfd_zone_cny,
            green_cny = EXCLUDED.green_cny,
            green_min_cny = EXCLUDED.green_min_cny,
            bill_green_cny = EXCLUDED.bill_green_cny,
            bill_fees_cny = EXCLUDED.bill_fees_cny,
            bill_total_cny = EXCLUDED.bill_total_cny,
            computed_at = NOW()
    """)
    with engine.begin() as conn:
        for stmt in ddl.read_text().split(";"):
            if stmt.strip():
                conn.execute(text(stmt))
        # additive column migration for existing tables
        for col in ("ref_price_east NUMERIC", "cfd_zone_cny NUMERIC", "green_min_cny NUMERIC",
                    "metered_mwh NUMERIC", "cfd_exchange_cny NUMERIC", "curve_min_mwh NUMERIC",
                    "contract_vol_clearing_mwh NUMERIC"):
            conn.execute(text(f"ALTER TABLE marketdata.wind_settlement_monthly ADD COLUMN IF NOT EXISTS {col}"))
        for r in rows:
            conn.execute(upsert, {
                "asset": "新_悦盛昌渠#1期",
                "month": f"{r['month']}-01",
                **{k: v for k, v in r.items() if k not in ("month", "vol_ratio", "data_days")},
            })


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--months", default="2026-01,2026-02,2026-03,2026-04,2026-05,2026-06,2026-07")
    ap.add_argument("--persist", action="store_true", help="upsert results into wind_settlement_monthly")
    args = ap.parse_args()

    _load_env()
    engine = create_engine(os.environ["PGURL"], connect_args={"connect_timeout": 10})

    rows = [replicate_month(engine, m) for m in args.months.split(",")]
    df = pd.DataFrame(rows).set_index("month")
    pd.set_option("display.width", 250)
    pd.set_option("display.max_columns", 50)
    pd.set_option("display.float_format", lambda v: f"{v:,.1f}")
    print(df.T.to_string())

    if args.persist:
        persist(engine, rows)
        print(f"\npersisted {len(rows)} months to marketdata.wind_settlement_monthly")


if __name__ == "__main__":
    main()
