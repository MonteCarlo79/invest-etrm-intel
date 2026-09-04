"""
Backfill source column for province_cap_comp and province_fr_market rows
that have the generic 'KB/Claude search (YYYY-MM-DD scan)' tag.

For each affected province, re-runs the KB search and updates the source
column in-place (no new rows, no conflict detection).

Usage:
    py scripts/backfill_capcomp_sources.py [--dry-run]
"""
from __future__ import annotations
import argparse, os, sys
sys.path.insert(0, str(__file__.replace("\\", "/").rsplit("/scripts/", 1)[0]))

import psycopg2
from services.hermes.capcomp_screener import _search_kb, _CAP_COMP_KEYWORDS, _FR_KEYWORDS

PG_URL = os.environ.get("PGURL", "postgresql://postgres:root@127.0.0.1:5433/marketdata")


def _get_source_hint(province: str, keywords: list, pg_url: str) -> str:
    kb_rows = _search_kb(province, keywords, pg_url)
    sources = {fn for _, fn in kb_rows if fn}
    if sources:
        return "; ".join(sorted(sources)[:3])
    return "claude_training_knowledge"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    conn = psycopg2.connect(PG_URL)
    try:
        with conn.cursor() as cur:
            # Cap comp
            cur.execute("""
                SELECT DISTINCT province FROM marketdata.province_cap_comp
                WHERE source LIKE 'KB/Claude search%'
                ORDER BY province
            """)
            cap_provinces = [r[0] for r in cur.fetchall()]

            # FR market
            cur.execute("""
                SELECT DISTINCT province FROM marketdata.province_fr_market
                WHERE source LIKE 'KB/Claude search%'
                ORDER BY province
            """)
            fr_provinces = [r[0] for r in cur.fetchall()]

        print(f"Cap comp provinces to update: {len(cap_provinces)}")
        print(f"FR market provinces to update: {len(fr_provinces)}")

        with conn.cursor() as cur:
            for prov in cap_provinces:
                new_src = _get_source_hint(prov, _CAP_COMP_KEYWORDS, PG_URL)
                print(f"  cap_comp {prov:20s} → {new_src[:80]}")
                if not args.dry_run:
                    cur.execute("""
                        UPDATE marketdata.province_cap_comp
                        SET source = %s
                        WHERE province = %s AND source LIKE 'KB/Claude search%%'
                    """, (new_src, prov))

            for prov in fr_provinces:
                new_src = _get_source_hint(prov, _FR_KEYWORDS, PG_URL)
                print(f"  fr_market {prov:20s} → {new_src[:80]}")
                if not args.dry_run:
                    cur.execute("""
                        UPDATE marketdata.province_fr_market
                        SET source = %s
                        WHERE province = %s AND source LIKE 'KB/Claude search%%'
                    """, (new_src, prov))

        if not args.dry_run:
            conn.commit()
            print("\nDone — sources updated.")
        else:
            print("\nDry run — no changes made.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
