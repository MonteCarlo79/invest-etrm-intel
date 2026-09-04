"""
Fix cap comp conflicts and 辽宁 visibility — 2026-07-09

Issues:
  1. 辽宁 effective_date=2027-01-01 → filtered out by year selector (2026). Fix to 2026-01-01.
  2. 浙江 conflict: both rows now 180.0 — keep infographic row, supersede KB/Claude row.
  3. 甘肃 conflict: 380 row is wrong (Haiku mistranscription). Supersede 380, keep 330+6h.

Usage:
    py scripts/fix_capcomp_conflicts_2026.py [--dry-run]
"""
from __future__ import annotations
import argparse, os, sys
sys.path.insert(0, str(__file__.replace("\\", "/").rsplit("/scripts/", 1)[0]))

import psycopg2

PG_URL = os.environ.get("PGURL", "postgresql://postgres:root@127.0.0.1:5433/marketdata")
T = "marketdata.province_cap_comp"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    conn = psycopg2.connect(PG_URL)
    try:
        with conn.cursor() as cur:

            # ── 1. 辽宁: move from 2027 → 2026 so it shows up in year selector ──
            cur.execute(
                f"SELECT id, effective_date, cap_comp_yuan_kw, status FROM {T} WHERE province = '辽宁'"
            )
            rows = cur.fetchall()
            print("辽宁 rows:", rows)
            for row_id, eff, val, status in rows:
                if hasattr(eff, 'year') and eff.year == 2027:
                    print(f"  FIX 辽宁 id={row_id}: effective_date 2027-01-01 → 2026-01-01")
                    if not args.dry_run:
                        cur.execute(
                            f"UPDATE {T} SET effective_date = '2026-01-01', ingested_at = NOW() WHERE id = %s",
                            (row_id,),
                        )

            # ── 2. 浙江: supersede KB/Claude row, confirm infographic row ──
            cur.execute(
                f"SELECT id, cap_comp_yuan_kw, status, source FROM {T} WHERE province = '浙江' ORDER BY ingested_at"
            )
            rows = cur.fetchall()
            print("\n浙江 rows:", rows)
            # Keep the one sourced from infographic (not KB/Claude search)
            keep_id = None
            drop_ids = []
            for row_id, val, status, source in rows:
                if source and "KB/Claude search" in source:
                    drop_ids.append(row_id)
                else:
                    keep_id = row_id
            if keep_id and drop_ids:
                print(f"  KEEP 浙江 id={keep_id} → confirmed")
                print(f"  SUPERSEDE 浙江 ids={drop_ids}")
                if not args.dry_run:
                    cur.execute(f"UPDATE {T} SET status = 'confirmed', ingested_at = NOW() WHERE id = %s", (keep_id,))
                    for d in drop_ids:
                        cur.execute(f"UPDATE {T} SET status = 'superseded', ingested_at = NOW() WHERE id = %s", (d,))
            else:
                print("  浙江: unexpected state, manual review needed")
                for r in rows:
                    print("   ", r)

            # ── 3. 甘肃: supersede 380 row, confirm 330+6h row ──
            cur.execute(
                f"SELECT id, cap_comp_yuan_kw, peak_duration_hours, status, source FROM {T} WHERE province = '甘肃' ORDER BY ingested_at"
            )
            rows = cur.fetchall()
            print("\n甘肃 rows:", rows)
            keep_id = None
            drop_ids = []
            for row_id, val, peak, status, source in rows:
                if val == 330.0:
                    keep_id = row_id
                else:
                    drop_ids.append((row_id, val))
            if keep_id and drop_ids:
                print(f"  KEEP 甘肃 id={keep_id} (330, 6h) → confirmed")
                print(f"  SUPERSEDE 甘肃 ids={[d[0] for d in drop_ids]} (wrong values: {[d[1] for d in drop_ids]})")
                if not args.dry_run:
                    cur.execute(f"UPDATE {T} SET status = 'confirmed', ingested_at = NOW() WHERE id = %s", (keep_id,))
                    for d_id, _ in drop_ids:
                        cur.execute(f"UPDATE {T} SET status = 'superseded', ingested_at = NOW() WHERE id = %s", (d_id,))
            else:
                print("  甘肃: unexpected state, manual review needed")
                for r in rows:
                    print("   ", r)

        if not args.dry_run:
            conn.commit()
            print("\nDone — changes committed.")
        else:
            print("\nDry run — no changes made.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
