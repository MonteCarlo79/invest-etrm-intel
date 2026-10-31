"""Settlement bridge — bill daily breakdown → per-book daily rows (plan task 4).

Bills are authoritative for the settled layer (spec decision 3). The ingest-time
parsers (services/settlement_ingest/scanner.py, apps/asset_risk/tab_settlement.py,
services/retail_risk/loader.py) already ran: parsed lines live in
marketdata.rm_settlement_items, and rm_settlements.raw_data carries only
file_hash/total_kind metadata — there is nothing to reparse at runtime, so this
module reads items only (a settlement with no items contributes nothing).

Row derivation for one book + month (rm_settlements.settlement_month is a DATE,
first of month):

- items with delivery_date (bill daily breakdown — Excel trade-capture rows)
  aggregate per (day, category) and insert as accrual_grade='bill',
  source=<category>;
- items without delivery_date (monthly lumps — PDF bill lines) aggregate per
  category and spread pro-rata over the month via _spread_lump, inserting as
  accrual_grade='accrual', source=<category>_accrual. The suffix keeps a spread
  lump from ever colliding with a bill row of the same category on the same day
  under the (asset_code, data_date, source) PK.

Amounts pass through with the stored sign convention (charge lines negative —
stored = −printed, uniform since the 2026-09 charge remediation). All settlement
statuses count ('processed' and 'flagged' alike — flagged rows still carry real
money; the flag is a recon hint, not an exclusion).

asset_code → book resolution goes through the rm_assets short name
(_RM_ASSET_NAME) — the same names services/settlement_ingest/folder_mapper.py
maps the stations' invoice folders to (book_type='asset').
"""
from __future__ import annotations

from datetime import date, timedelta

import pandas as pd
from sqlalchemy import text

from libs.trading_book.settlement_bridge import month_bounds

_SCHEMA_DDL = "CREATE SCHEMA IF NOT EXISTS reports"

_DDL = """
CREATE TABLE IF NOT EXISTS reports.book_settled_daily (
    asset_code    TEXT    NOT NULL,
    data_date     DATE    NOT NULL,
    amount_cny    NUMERIC NOT NULL,
    source        TEXT    NOT NULL,
    accrual_grade TEXT    NOT NULL,
    PRIMARY KEY (asset_code, data_date, source)
)
"""

# Registry asset_code -> marketdata.rm_assets.name (short name; matches the
# folder_mapper fallback values, e.g. "B-11 内蒙四子王旗" -> "四子王旗" — the
# rm short name is NOT always a substring of the registry plant_name).
_RM_ASSET_NAME: dict[str, str] = {
    "suyou": "景蓝乌尔图",
    "hangjinqi": "悦杭独贵",
    "siziwangqi": "四子王旗",
    "gushanliang": "裕昭沙子坝",
    "bameng": "景怡查干哈达",
    "wulate": "远景乌拉特",
}

_BOOK_SQL = """
    SELECT b.id
    FROM marketdata.rm_books b
    JOIN marketdata.rm_assets a ON a.id = b.asset_id
    WHERE a.name = :name AND b.book_type = 'asset'
    LIMIT 1
"""

_ITEMS_SQL = """
    SELECT i.category, i.delivery_date, i.amount_cny
    FROM marketdata.rm_settlement_items i
    JOIN marketdata.rm_settlements s ON s.id = i.settlement_id
    WHERE s.book_id = :bid AND s.settlement_month = :ms
"""

_UPSERT_SQL = """
    INSERT INTO reports.book_settled_daily
        (asset_code, data_date, amount_cny, source, accrual_grade)
    VALUES (:ac, :d, :amt, :src, :grade)
    ON CONFLICT (asset_code, data_date, source)
    DO UPDATE SET amount_cny = EXCLUDED.amount_cny,
                  accrual_grade = EXCLUDED.accrual_grade
"""


def _spread_lump(amount: float, month_start: date,
                 month_end: date) -> list[tuple[date, float, str]]:
    """Pro-rata daily rows for a monthly lump, tagged 'accrual'.

    Even split over [month_start, month_end] inclusive; the last day absorbs
    the residual so the rows sum to `amount` exactly.
    """
    n = (month_end - month_start).days + 1
    if n <= 0:
        return []
    per = amount / n
    rows = [(month_start + timedelta(days=i), per, "accrual")
            for i in range(n - 1)]
    rows.append((month_end, amount - per * (n - 1), "accrual"))
    return rows


def refresh_settled_daily(engine, asset_code: str, month: str) -> int:
    """Refresh one book's daily settled rows for month 'YYYY-MM'; returns rows
    written (upserted on PK, so re-runs are idempotent).

    Rows from a previous run whose (day, source) is not regenerated this run
    are left in place — supersede/delete of the underlying settlement is an
    rm-side concern. Unknown asset_code -> KeyError; known code with no rm
    book -> 0, nothing written.
    """
    name = _RM_ASSET_NAME.get(asset_code)
    if name is None:
        raise KeyError(f"unknown asset_code: {asset_code!r}")
    month_start, month_end = month_bounds(month)
    with engine.begin() as conn:
        conn.execute(text(_SCHEMA_DDL))
        conn.execute(text(_DDL))
        book_id = conn.execute(text(_BOOK_SQL), {"name": name}).scalar()
        if book_id is None:
            return 0
        df = pd.read_sql(text(_ITEMS_SQL), conn,
                         params={"bid": book_id, "ms": month_start})

    rows: list[dict] = []
    if not df.empty:
        df["amount_cny"] = df["amount_cny"].astype(float)
        billed = df[df["delivery_date"].notna()]
        for (day, cat), g in billed.groupby(["delivery_date", "category"]):
            rows.append({"ac": asset_code, "d": day,
                         "amt": float(g["amount_cny"].sum()),
                         "src": cat, "grade": "bill"})
        lumps = df[df["delivery_date"].isna()]
        for cat, g in lumps.groupby("category"):
            for day, amt, grade in _spread_lump(
                    float(g["amount_cny"].sum()), month_start, month_end):
                rows.append({"ac": asset_code, "d": day, "amt": amt,
                             "src": f"{cat}_accrual", "grade": grade})

    with engine.begin() as conn:
        for r in rows:
            conn.execute(text(_UPSERT_SQL), r)
    return len(rows)
