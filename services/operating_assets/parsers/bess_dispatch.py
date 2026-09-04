"""Parser for BESS 调度计划表 Excel (15-min dispatch plan).

Source: 电力交易调度计划表 — one sheet per day.
Target: rm_dispatch_plan (one row per asset per 15-min interval).
"""
from __future__ import annotations

import pandas as pd
from shared.agents.db import get_conn


def parse_bess_dispatch(xl: pd.ExcelFile, asset_name: str, batch_id: str) -> dict:
    """Parse BESS 15-min dispatch plan sheets."""
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
                try:
                    df = xl.parse(sheet_name)
                    date_str = None
                    for col in df.columns:
                        if "日期" in str(col) or "date" in str(col).lower():
                            date_str = str(df[col].iloc[0])
                            break

                    if date_str is None:
                        try:
                            date_str = pd.to_datetime(sheet_name).strftime("%Y-%m-%d")
                        except Exception:
                            continue

                    base_date = pd.to_datetime(date_str).date()

                    time_col = next((c for c in df.columns if "时间" in str(c) or "time" in str(c).lower()), None)
                    if time_col is None:
                        continue

                    for _, r in df.iterrows():
                        time_val = r[time_col]
                        if pd.isna(time_val):
                            continue

                        interval_start = pd.Timestamp(f"{base_date} {time_val}", tz="Asia/Shanghai")

                        soc = r.get("SOC(%)", r.get("SOC", None))
                        nominated = r.get("操作员申报计划(MW)", r.get("nominated_mw", None))
                        forecast = r.get("当前预测(MW)", r.get("forecast_mw", None))
                        dispatched = r.get("实时调度出力(MW)", r.get("dispatched_mw", None))
                        actual = r.get("实际执行功率(MW)", r.get("actual_mw", None))

                        cur.execute("""
                            INSERT INTO marketdata.rm_dispatch_plan
                                (asset_id, interval_start, soc_pct, nominated_mw,
                                 forecast_mw, dispatched_mw, actual_mw, upload_batch_id)
                            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                            ON CONFLICT (asset_id, interval_start) DO UPDATE SET
                                soc_pct = EXCLUDED.soc_pct,
                                nominated_mw = EXCLUDED.nominated_mw,
                                forecast_mw = EXCLUDED.forecast_mw,
                                dispatched_mw = EXCLUDED.dispatched_mw,
                                actual_mw = EXCLUDED.actual_mw,
                                upload_batch_id = EXCLUDED.upload_batch_id
                        """, (
                            asset_id, interval_start, soc, nominated,
                            forecast, dispatched, actual, batch_id,
                        ))
                        rows_written += 1
                except Exception as e:
                    errors.append(f"Sheet '{sheet_name}': {str(e)}")

        conn.commit()

    return {"rows_written": rows_written, "errors": errors}
