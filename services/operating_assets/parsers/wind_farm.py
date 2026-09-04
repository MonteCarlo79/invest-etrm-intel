"""Parser for 零碳46风电经营统计_YYYYMMDD.xlsx (wind farm operations file).

Ingests 3 sheets:
- 风场功率 → rm_dispatch_plan (15-min, forecast_mw + actual_mw)
- 结算明细 → rm_position_volumes (15-min aggregated to hourly)
- 经营统计 → rm_pnl_snapshots (monthly KPIs)
"""
from __future__ import annotations

import pandas as pd
import numpy as np
from typing import Any
from shared.agents.db import get_conn


def parse_settlement_detail_row(row: pd.Series) -> dict[str, Any]:
    """Parse a single 结算明细 row to canonical position volume dict.

    Prices in source are CNY/kWh; converted to CNY/MWh (* 1000).
    """
    return {
        "date": str(row.get("日期", "")),
        "time": str(row.get("时间", "")),
        "settled_mwh": float(row.get("省调电量", 0) or 0),
        "rt_price_cny_mwh": float(row.get("省级实时价格", 0) or 0) * 1000,
        "market_price_cny_mwh": float(row.get("省级实时节点价", 0) or 0) * 1000,
        "da_price_cny_mwh": float(row.get("省级日前价格", 0) or 0) * 1000,
        "da_volume_mwh": float(row.get("省级日前电量", 0) or 0),
        "intramonth_match_price_cny_mwh": float(row.get("省级月内撮合价格", 0) or 0) * 1000,
        "intramonth_match_volume_mwh": float(row.get("省级月内撮合电量", 0) or 0),
        "annual_price_cny_mwh": float(row.get("市场合约价格", 0) or 0) * 1000,
        "pnl_cny": float(row.get("收益", 0) or 0),
        "deviation_grid_flow_mwh": float(row.get("弃风量", 0) or 0),
    }


def aggregate_15min_to_hourly(rows: list[dict[str, Any]]) -> dict[str, Any]:
    """Aggregate 4 x 15-min rows into one hourly row.

    Volumes are summed. Prices are volume-weighted averages.
    """
    da_vol = sum(r.get("da_volume", 0) or 0 for r in rows)
    rt_vol = sum(r.get("rt_volume", 0) or 0 for r in rows)

    da_price_wt = sum((r.get("da_volume", 0) or 0) * (r.get("da_price", 0) or 0) for r in rows)
    rt_price_wt = sum((r.get("rt_volume", 0) or 0) * (r.get("rt_price", 0) or 0) for r in rows)

    return {
        "da_volume_mwh": da_vol,
        "rt_volume_mwh": rt_vol,
        "da_price_cny_mwh": da_price_wt / da_vol if da_vol > 0 else 0.0,
        "rt_price_cny_mwh": rt_price_wt / rt_vol if rt_vol > 0 else 0.0,
    }


def parse_wind_farm(file_path: str, asset_name: str, batch_id: str) -> dict:
    """Parse full wind farm operations Excel and write to DB."""
    xl = pd.ExcelFile(file_path)
    rows_written = 0
    errors = []

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM marketdata.rm_assets WHERE name = %s", (asset_name,))
            row = cur.fetchone()
            if not row:
                return {"asset_name": asset_name, "asset_type": "wind", "parser": "wind_farm",
                        "rows_written": 0, "errors": [f"Asset not found: {asset_name}"]}
            asset_id = row[0]

            cur.execute("SELECT id FROM marketdata.rm_books WHERE asset_id = %s", (asset_id,))
            book_row = cur.fetchone()
            book_id = book_row[0] if book_row else None

            # Sheet 1: 风场功率 → rm_dispatch_plan
            if "风场功率" in xl.sheet_names:
                try:
                    df = xl.parse("风场功率")
                    for _, r in df.iterrows():
                        date_val = r.get("日期")
                        time_val = r.get("时间")
                        if pd.isna(date_val) or pd.isna(time_val):
                            continue
                        interval_start = pd.Timestamp(
                            f"{pd.to_datetime(date_val).date()} {time_val}",
                            tz="Asia/Shanghai"
                        )
                        forecast = r.get("D+1日前预测功率(MW)", r.get("D+1预测功率(MW)"))
                        actual = r.get("实际出力(MW)", r.get("实际功率(MW)"))
                        cur.execute("""
                            INSERT INTO marketdata.rm_dispatch_plan
                                (asset_id, interval_start, forecast_mw, actual_mw, upload_batch_id)
                            VALUES (%s,%s,%s,%s,%s)
                            ON CONFLICT (asset_id, interval_start) DO UPDATE SET
                                forecast_mw = EXCLUDED.forecast_mw,
                                actual_mw = EXCLUDED.actual_mw,
                                upload_batch_id = EXCLUDED.upload_batch_id
                        """, (asset_id, interval_start, forecast, actual, batch_id))
                        rows_written += 1
                except Exception as e:
                    errors.append(f"风场功率: {str(e)}")

            # Sheet 2: 结算明细 → rm_position_volumes
            if "结算明细" in xl.sheet_names and book_id:
                try:
                    df = xl.parse("结算明细")
                    for _, r in df.iterrows():
                        parsed = parse_settlement_detail_row(r)
                        delivery_date = pd.to_datetime(parsed["date"]).date()
                        time_parts = str(parsed["time"]).split(":")
                        hour = int(time_parts[0]) if time_parts else 0

                        cur.execute("""
                            INSERT INTO marketdata.rm_position_volumes
                                (book_id, delivery_date, hour, da_price_cny_mwh, rt_price_cny_mwh,
                                 da_volume_mwh, intramonth_match_price_cny_mwh,
                                 intramonth_match_volume_mwh, annual_price_cny_mwh,
                                 market_price_cny_mwh, settled_mwh,
                                 deviation_grid_flow_mwh, pnl_cny, upload_batch_id)
                            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                            ON CONFLICT (book_id, delivery_date, hour) DO UPDATE SET
                                da_price_cny_mwh = COALESCE(EXCLUDED.da_price_cny_mwh, marketdata.rm_position_volumes.da_price_cny_mwh),
                                rt_price_cny_mwh = COALESCE(EXCLUDED.rt_price_cny_mwh, marketdata.rm_position_volumes.rt_price_cny_mwh),
                                da_volume_mwh = COALESCE(EXCLUDED.da_volume_mwh, marketdata.rm_position_volumes.da_volume_mwh),
                                settled_mwh = COALESCE(EXCLUDED.settled_mwh, marketdata.rm_position_volumes.settled_mwh),
                                deviation_grid_flow_mwh = COALESCE(EXCLUDED.deviation_grid_flow_mwh, marketdata.rm_position_volumes.deviation_grid_flow_mwh),
                                pnl_cny = COALESCE(EXCLUDED.pnl_cny, marketdata.rm_position_volumes.pnl_cny),
                                upload_batch_id = EXCLUDED.upload_batch_id
                        """, (
                            book_id, delivery_date, hour,
                            parsed["da_price_cny_mwh"], parsed["rt_price_cny_mwh"],
                            parsed["da_volume_mwh"], parsed["intramonth_match_price_cny_mwh"],
                            parsed["intramonth_match_volume_mwh"], parsed["annual_price_cny_mwh"],
                            parsed["market_price_cny_mwh"], parsed["settled_mwh"],
                            parsed["deviation_grid_flow_mwh"], parsed["pnl_cny"], batch_id,
                        ))
                        rows_written += 1
                except Exception as e:
                    errors.append(f"结算明细: {str(e)}")

            # Sheet 3: 经营统计 → rm_pnl_snapshots
            if "经营统计" in xl.sheet_names and book_id:
                try:
                    df = xl.parse("经营统计")
                    for _, r in df.iterrows():
                        snapshot_date = pd.to_datetime(r.get("月份", r.get("日期"))).date()
                        cur.execute("""
                            INSERT INTO marketdata.rm_pnl_snapshots
                                (book_id, snapshot_date, realized_cny,
                                 curtailment_mwh, curtailment_rate_pct,
                                 curtailment_opportunity_cost_cny, equivalent_hours)
                            VALUES (%s,%s,%s,%s,%s,%s,%s)
                            ON CONFLICT (book_id, snapshot_date) DO UPDATE SET
                                realized_cny = EXCLUDED.realized_cny,
                                curtailment_mwh = EXCLUDED.curtailment_mwh,
                                curtailment_rate_pct = EXCLUDED.curtailment_rate_pct,
                                curtailment_opportunity_cost_cny = EXCLUDED.curtailment_opportunity_cost_cny,
                                equivalent_hours = EXCLUDED.equivalent_hours
                        """, (
                            book_id, snapshot_date,
                            r.get("收益", r.get("realized_cny")),
                            r.get("弃风量"),
                            r.get("弃风率"),
                            r.get("弃风损失", r.get("curtailment_opportunity_cost_cny")),
                            r.get("等效满负荷小时数", r.get("equivalent_hours")),
                        ))
                        rows_written += 1
                except Exception as e:
                    errors.append(f"经营统计: {str(e)}")

        conn.commit()

    return {"asset_name": asset_name, "asset_type": "wind", "parser": "wind_farm",
            "rows_written": rows_written, "errors": errors}
