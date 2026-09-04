"""
Manual upsert for province_cap_comp rows verified from infographic
"各省独立储能容量补偿汇总（截至2026年6月）" — 2026-07-09

Operations:
  INSERT (if not exists): 辽宁, 吉林, 新疆, 广东
  UPDATE (fix values):    浙江 170→180, 甘肃 peak_duration→6h, 陕西 peak_duration→6h

Usage:
    py scripts/upsert_capcomp_manual_2026.py [--dry-run]
"""
from __future__ import annotations
import argparse, os, sys
sys.path.insert(0, str(__file__.replace("\\", "/").rsplit("/scripts/", 1)[0]))

import psycopg2

PG_URL = os.environ.get("PGURL", "postgresql://postgres:root@127.0.0.1:5433/marketdata")

# Rows to INSERT (skipped if province+effective_date already exists with confirmed/draft status)
_INSERT_ROWS = [
    {
        "province": "辽宁",
        "effective_date": "2027-01-01",   # 拟2027年1月1日起执行
        "cap_comp_yuan_kw": 370.0,
        "peak_duration_hours": None,
        "status": "draft",                # 征求意见稿
        "source": "manual_image:各省独立储能容量补偿汇总2026年6月",
    },
    {
        "province": "吉林",
        "effective_date": "2026-01-01",
        "cap_comp_yuan_kw": 330.0,
        "peak_duration_hours": 8.0,
        "status": "confirmed",            # 正式落地
        "source": "manual_image:各省独立储能容量补偿汇总2026年6月",
    },
    {
        "province": "新疆",
        "effective_date": "2026-01-01",
        "cap_comp_yuan_kw": 165.0,
        "peak_duration_hours": 6.0,
        "status": "confirmed",            # 正式落地
        "source": "manual_image:各省独立储能容量补偿汇总2026年6月",
    },
    {
        "province": "广东",
        "effective_date": "2026-01-01",
        "cap_comp_yuan_kw": 200.0,
        "peak_duration_hours": None,
        "status": "confirmed",            # 正式落地
        "source": "manual_image:各省独立储能容量补偿汇总2026年6月",
    },
]

# Field updates for existing rows (matched by province + effective_date)
_UPDATE_ROWS = [
    {
        "province": "吉林",
        "effective_date": "2026-01-01",
        "set": {"cap_comp_yuan_kw": 330.0, "peak_duration_hours": 8.0},
        "reason": "Infographic shows 330/8h; DB had 380 (wrong)",
    },
    {
        "province": "浙江",
        "effective_date": "2026-01-01",
        "set": {"cap_comp_yuan_kw": 180.0},
        "reason": "Infographic clearly shows 180, table had 170",
    },
    {
        "province": "甘肃",
        "effective_date": "2026-01-01",
        "set": {"peak_duration_hours": 6.0},
        "reason": "Infographic shows 6h peak duration",
    },
    {
        "province": "陕西",
        "effective_date": "2026-01-01",
        "set": {"peak_duration_hours": 6.0},
        "reason": "Infographic shows 6h peak duration (暂定)",
    },
]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    conn = psycopg2.connect(PG_URL)
    try:
        with conn.cursor() as cur:
            # ── INSERTs ──
            print("=== INSERT missing provinces ===")
            for row in _INSERT_ROWS:
                cur.execute(
                    """
                    SELECT id, cap_comp_yuan_kw, status
                    FROM marketdata.province_cap_comp
                    WHERE province = %s AND effective_date = %s
                    """,
                    (row["province"], row["effective_date"]),
                )
                existing = cur.fetchone()
                if existing:
                    print(f"  SKIP  {row['province']} {row['effective_date']} — already exists "
                          f"(id={existing[0]}, val={existing[1]}, status={existing[2]})")
                    continue

                print(f"  INSERT {row['province']} {row['effective_date']} "
                      f"cap={row['cap_comp_yuan_kw']} peak={row['peak_duration_hours']} "
                      f"status={row['status']}")
                if not args.dry_run:
                    cur.execute(
                        """
                        INSERT INTO marketdata.province_cap_comp
                            (province, effective_date, cap_comp_yuan_kw, peak_duration_hours,
                             status, source, ingested_at)
                        VALUES (%s, %s, %s, %s, %s, %s, NOW())
                        """,
                        (
                            row["province"],
                            row["effective_date"],
                            row["cap_comp_yuan_kw"],
                            row["peak_duration_hours"],
                            row["status"],
                            row["source"],
                        ),
                    )

            # ── UPDATEs ──
            print("\n=== UPDATE existing rows ===")
            for upd in _UPDATE_ROWS:
                cur.execute(
                    """
                    SELECT id, cap_comp_yuan_kw, peak_duration_hours, status
                    FROM marketdata.province_cap_comp
                    WHERE province = %s AND effective_date = %s
                    """,
                    (upd["province"], upd["effective_date"]),
                )
                existing = cur.fetchone()
                if not existing:
                    print(f"  SKIP  {upd['province']} — no existing row found")
                    continue

                row_id, old_val, old_peak, old_status = existing
                set_clause = ", ".join(f"{k} = %s" for k in upd["set"])
                set_vals = list(upd["set"].values())
                print(f"  UPDATE {upd['province']} id={row_id}: {upd['set']}  [{upd['reason']}]")

                if not args.dry_run:
                    cur.execute(
                        f"UPDATE marketdata.province_cap_comp SET {set_clause}, ingested_at = NOW() "
                        f"WHERE id = %s",
                        (*set_vals, row_id),
                    )

        if not args.dry_run:
            conn.commit()
            print("\nDone — changes committed.")
        else:
            print("\nDry run — no changes made.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
