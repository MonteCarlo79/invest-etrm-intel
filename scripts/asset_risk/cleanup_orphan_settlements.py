"""One-off cleanup: orphan rm_settlements headers left by the overwrite bug.

An orphan = header with ZERO items AND a newer header (higher id) with the same
(book_id, settlement_month, file_name) that HAS items. Zero-item headers with no
such twin are listed only, never deleted.

Reads PGURL. Default is --dry-run (touches nothing); --apply backs up to
marketdata.rm_settlements_orphan_bak_20261115 and deletes, in one transaction.
"""
import argparse
import os
import sys

from sqlalchemy import create_engine, text

BACKUP = "marketdata.rm_settlements_orphan_bak_20261115"

_ZERO_ITEMS = ("NOT EXISTS (SELECT 1 FROM marketdata.rm_settlement_items i "
               "WHERE i.settlement_id = s.id)")
_TWIN = ("EXISTS (SELECT 1 FROM marketdata.rm_settlements t "
         "WHERE t.book_id = s.book_id AND t.settlement_month = s.settlement_month "
         "AND t.file_name = s.file_name AND t.id > s.id "
         "AND EXISTS (SELECT 1 FROM marketdata.rm_settlement_items ti WHERE ti.settlement_id = t.id))")

SELECT_ORPHANS = (f"SELECT s.id, s.book_id, s.settlement_month, s.file_name, s.upload_date "
                  f"FROM marketdata.rm_settlements s WHERE {_ZERO_ITEMS} AND {_TWIN} ORDER BY s.id")
SELECT_LONE = (f"SELECT s.id, s.book_id, s.settlement_month, s.file_name, s.upload_date "
               f"FROM marketdata.rm_settlements s WHERE {_ZERO_ITEMS} AND NOT {_TWIN} ORDER BY s.id")


def select_orphans(conn):
    return [tuple(r) for r in conn.execute(text(SELECT_ORPHANS)).fetchall()]


def select_lone_zero_item(conn):
    return [tuple(r) for r in conn.execute(text(SELECT_LONE)).fetchall()]


def _print_rows(label, rows):
    print(f"{label}: {len(rows)}")
    for r in rows:
        print(f"  id={r[0]} book_id={r[1]} month={r[2]} file_name={r[3]} upload_date={r[4]}")


def apply_cleanup(conn, orphan_ids):
    """Back up and delete orphan headers; raise (-> rollback) on count mismatch."""
    if not orphan_ids:
        return 0, 0
    conn.execute(text(f"CREATE TABLE IF NOT EXISTS {BACKUP} AS "
                      "SELECT * FROM marketdata.rm_settlements WHERE false"))
    params = {f"p{i}": v for i, v in enumerate(orphan_ids)}
    ph = ", ".join(f":p{i}" for i in range(len(orphan_ids)))
    backed = conn.execute(text(
        f"INSERT INTO {BACKUP} SELECT * FROM marketdata.rm_settlements WHERE id IN ({ph})"), params).rowcount
    deleted = conn.execute(text(
        f"DELETE FROM marketdata.rm_settlements WHERE id IN ({ph})"), params).rowcount
    print(f"backed up: {backed}, deleted: {deleted}")
    if backed != deleted or deleted != len(orphan_ids):
        raise RuntimeError(f"count mismatch (selected={len(orphan_ids)} backed={backed} "
                           f"deleted={deleted}); rolling back")
    return backed, deleted


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__)
    g = ap.add_mutually_exclusive_group()
    g.add_argument("--dry-run", action="store_true", help="print plan only (default)")
    g.add_argument("--apply", action="store_true", help="perform backup + delete")
    args = ap.parse_args(argv)
    url = os.environ.get("PGURL")
    if not url:
        sys.exit("PGURL not set")
    eng = create_engine(url)
    with eng.begin() as conn:
        orphans = select_orphans(conn)
        _print_rows("orphan headers selected", orphans)
        _print_rows("zero-item headers WITHOUT itemised twin (listed only, not deleted)",
                    select_lone_zero_item(conn))
        if not args.apply:
            print("dry-run: nothing changed (use --apply)")
            return 0
        apply_cleanup(conn, [r[0] for r in orphans])
    print("applied")
    return 0


if __name__ == "__main__":
    sys.exit(main())
