"""Parser for BESS 运营统计 Excel (daily operations summary).

Source: 【日期】内蒙储能电站运营统计.xlsx — one sheet per station.
Target: rm_dispatch_daily (one row per asset per day).
"""
from __future__ import annotations

import pandas as pd
from shared.agents.db import get_conn


def parse_bess_daily(xl: pd.ExcelFile, asset_name: str, batch_id: str) -> dict:
    """Parse BESS daily operations sheets and write to rm_dispatch_daily."""
    rows_written = 0
    errors = []

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id FROM marketdata.rm_assets WHERE name = %s", (asset_name,)
            )
            row = cur.fetchone()
            if not row:
                return {"rows_written": 0, "errors": [f"Asset not found: {asset_name}"]}
            asset_id = row[0]

            for sheet_name in xl.sheet_names:
                if asset_name not in sheet_name and "运营" not in sheet_name:
                    continue
                try:
                    df = xl.parse(sheet_name)
                    for _, r in df.iterrows():
                        dispatch_date = pd.to_datetime(r.get("日期", r.get("Date"))).date()
                        cur.execute("""
                            INSERT INTO marketdata.rm_dispatch_daily
                                (asset_id, dispatch_date, charge_mwh, discharge_mwh,
                                 auxiliary_consumption_mwh, cycle_count_day, conversion_ratio,
                                 discharge_revenue_cny, charge_cost_cny, net_margin_cny,
                                 upload_batch_id)
                            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                            ON CONFLICT (asset_id, dispatch_date) DO UPDATE SET
                                charge_mwh = EXCLUDED.charge_mwh,
                                discharge_mwh = EXCLUDED.discharge_mwh,
                                auxiliary_consumption_mwh = EXCLUDED.auxiliary_consumption_mwh,
                                cycle_count_day = EXCLUDED.cycle_count_day,
                                conversion_ratio = EXCLUDED.conversion_ratio,
                                discharge_revenue_cny = EXCLUDED.discharge_revenue_cny,
                                charge_cost_cny = EXCLUDED.charge_cost_cny,
                                net_margin_cny = EXCLUDED.net_margin_cny,
                                upload_batch_id = EXCLUDED.upload_batch_id
                        """, (
                            asset_id, dispatch_date,
                            r.get("日充电量", r.get("charge_mwh")),
                            r.get("日放电量", r.get("discharge_mwh")),
                            r.get("综合站用电", r.get("auxiliary_consumption_mwh")),
                            r.get("日充放次数", r.get("cycle_count_day")),
                            r.get("日充放转化率", r.get("conversion_ratio")),
                            r.get("放电收入", r.get("discharge_revenue_cny")),
                            r.get("充电费用", r.get("charge_cost_cny")),
                            r.get("站点毛利", r.get("net_margin_cny")),
                            batch_id,
                        ))
                        rows_written += 1
                except Exception as e:
                    errors.append(f"Sheet '{sheet_name}': {str(e)}")

        conn.commit()

    return {"rows_written": rows_written, "errors": errors}
