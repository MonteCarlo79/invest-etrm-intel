"""
DB Coverage Audit — read-only.
Audits all tables in schemas 'public' and 'marketdata'.
Outputs:
  coverage_summary.md
  table_coverage.csv
  table_coverage_detailed.md

Usage:
  PGURL=postgresql://... python audit_table_coverage.py
  or source config/.env first.
"""
from __future__ import annotations

import csv
import os
import sys
import textwrap
from datetime import date, timedelta
from pathlib import Path
from typing import Optional

import psycopg2
import psycopg2.extras

# ---------------------------------------------------------------------------
# Connection
# ---------------------------------------------------------------------------

_DB_URL: str = ""

def _build_url() -> str:
    global _DB_URL
    if _DB_URL:
        return _DB_URL
    url = os.environ.get("PGURL") or os.environ.get("DB_DSN")
    if not url:
        host = os.environ.get("DB_HOST", "localhost")
        port = os.environ.get("DB_PORT", "5432")
        dbname = os.environ.get("DB_NAME", "marketdata")
        user = os.environ.get("DB_USER", "postgres")
        password = os.environ.get("DB_PASSWORD", "")
        url = f"postgresql://{user}:{password}@{host}:{port}/{dbname}"
    _DB_URL = url
    return url


def get_conn():
    url = _build_url()
    conn = psycopg2.connect(
        url,
        keepalives=1,
        keepalives_idle=30,
        keepalives_interval=10,
        keepalives_count=5,
        connect_timeout=15,
        options="-c statement_timeout=120000",   # 2-min per-statement safety cap
    )
    conn.set_session(readonly=True, autocommit=True)
    return conn


# ---------------------------------------------------------------------------
# Step 1: discover tables + candidate temporal columns
# ---------------------------------------------------------------------------

TEMPORAL_PRIORITY = [
    # business-semantic first
    "data_date", "trading_date", "delivery_date", "settlement_date",
    "biz_date", "trade_date", "report_date", "value_date",
    "date", "ds", "dt",
    # generic time
    "time", "timestamp", "datetime",
    # audit fallbacks
    "created_at", "updated_at", "inserted_at", "modified_at",
]

TEMPORAL_DTYPES = {"date", "timestamp without time zone",
                   "timestamp with time zone", "timestamptz", "time without time zone"}


STAGING_PREFIXES = ("_stg_", "_tmp_", "_temp_")


def discover_tables(conn) -> list[dict]:
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
        SELECT table_schema, table_name
        FROM information_schema.tables
        WHERE table_schema IN ('public', 'marketdata')
          AND table_type = 'BASE TABLE'
        ORDER BY table_schema, table_name
    """)
    rows = [dict(r) for r in cur.fetchall()]
    # Separate staging/temp tables — audit them but flag clearly
    for r in rows:
        r["is_staging"] = any(r["table_name"].startswith(p) for p in STAGING_PREFIXES)
    return rows


def discover_columns(conn) -> dict[tuple, list[dict]]:
    """Returns {(schema, table): [col_info, ...]}"""
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
        SELECT table_schema, table_name, column_name, data_type, ordinal_position
        FROM information_schema.columns
        WHERE table_schema IN ('public', 'marketdata')
        ORDER BY table_schema, table_name, ordinal_position
    """)
    result: dict[tuple, list] = {}
    for r in cur.fetchall():
        key = (r["table_schema"], r["table_name"])
        result.setdefault(key, []).append(dict(r))
    return result


def pick_temporal_column(cols: list[dict]) -> Optional[dict]:
    """Pick best temporal column using priority list, then dtype fallback."""
    col_map = {c["column_name"].lower(): c for c in cols}

    # Priority 1: exact name match in priority list
    for name in TEMPORAL_PRIORITY:
        if name in col_map and col_map[name]["data_type"] in TEMPORAL_DTYPES:
            return col_map[name]

    # Priority 2: any column with a temporal dtype + name containing date/time
    for c in cols:
        cname = c["column_name"].lower()
        if c["data_type"] in TEMPORAL_DTYPES and any(
            kw in cname for kw in ("date", "time", "ts", "dt", "day")
        ):
            return c

    # Priority 3: any column with temporal dtype
    for c in cols:
        if c["data_type"] in TEMPORAL_DTYPES:
            return c

    return None


# ---------------------------------------------------------------------------
# Step 2: per-table coverage stats
# ---------------------------------------------------------------------------

TODAY = date.today()


def classify_freshness(max_val, row_count: int) -> str:
    if row_count == 0:
        return "empty"
    if max_val is None:
        return "error"
    if isinstance(max_val, str):
        try:
            max_val = date.fromisoformat(str(max_val)[:10])
        except Exception:
            return "error"
    if hasattr(max_val, "date"):
        max_val = max_val.date()
    stale_days = (TODAY - max_val).days
    if stale_days <= 1:
        return "fresh"
    elif stale_days <= 7:
        return "slightly_stale"
    else:
        return "stale"


def audit_table(schema: str, table: str, tcol: Optional[dict]) -> dict:
    """Each call opens its own fresh connection to tolerate long audit runs."""
    conn = get_conn()
    fq = f'"{schema}"."{table}"'
    result = dict(
        schema_name=schema,
        table_name=table,
        selected_temporal_column=None,
        selected_temporal_type=None,
        row_count=None,
        min_value=None,
        max_value=None,
        distinct_date_count=None,
        missing_dates_count=None,
        first_missing_date=None,
        last_missing_date=None,
        latest_7d_rows=None,
        latest_30d_rows=None,
        stale_days=None,
        status="error",
        notes="",
    )

    cur = conn.cursor()

    # Row count (fast)
    try:
        cur.execute(f"SELECT COUNT(*) FROM {fq}")
        row_count = cur.fetchone()[0]
        result["row_count"] = row_count
    except Exception as e:
        result["status"] = "error"
        result["notes"] = f"COUNT error: {e}"
        return result

    if row_count == 0:
        result["status"] = "empty"
        return result

    if tcol is None:
        result["status"] = "no_temporal_column"
        return result

    col = f'"{tcol["column_name"]}"'
    dtype = tcol["data_type"]
    result["selected_temporal_column"] = tcol["column_name"]
    result["selected_temporal_type"] = dtype

    is_date_type = dtype == "date"
    date_expr = col if is_date_type else f"{col}::date"

    try:
        cur.execute(f"""
            SELECT
                MIN({date_expr}),
                MAX({date_expr}),
                COUNT(DISTINCT {date_expr})
            FROM {fq}
            WHERE {col} IS NOT NULL
        """)
        row = cur.fetchone()
        min_d, max_d, distinct_d = row
        result["min_value"] = str(min_d) if min_d else None
        result["max_value"] = str(max_d) if max_d else None
        result["distinct_date_count"] = distinct_d

        if max_d:
            stale_days = (TODAY - (max_d if isinstance(max_d, date) else max_d.date())).days
            result["stale_days"] = stale_days
            result["status"] = classify_freshness(max_d, row_count)
        else:
            result["status"] = "error"
            result["notes"] = "all NULL temporal values"
            return result

    except Exception as e:
        result["status"] = "error"
        result["notes"] = f"min/max query error: {e}"
        return result

    # Latest 7d / 30d row counts
    try:
        cur.execute(f"""
            SELECT
                COUNT(*) FILTER (WHERE {date_expr} >= CURRENT_DATE - 6) AS r7d,
                COUNT(*) FILTER (WHERE {date_expr} >= CURRENT_DATE - 29) AS r30d
            FROM {fq}
            WHERE {col} IS NOT NULL
        """)
        r7d, r30d = cur.fetchone()
        result["latest_7d_rows"] = r7d
        result["latest_30d_rows"] = r30d
    except Exception as e:
        result["notes"] += f" | 7d/30d error: {e}"

    # Missing dates (only if span <= 5 years to avoid expensive generation)
    if min_d and max_d:
        try:
            min_date = min_d if isinstance(min_d, date) else min_d.date()
            max_date = max_d if isinstance(max_d, date) else max_d.date()
            span_days = (max_date - min_date).days

            if span_days <= 1825:  # ~5 years
                cur.execute(f"""
                    WITH date_series AS (
                        SELECT generate_series(
                            {date_expr}::date,
                            MAX({date_expr}::date) OVER (),
                            '1 day'::interval
                        )::date AS d
                        FROM {fq}
                        WHERE {col} IS NOT NULL
                        LIMIT 1
                    ),
                    -- simpler approach:
                    expected AS (
                        SELECT gs::date AS d
                        FROM generate_series(
                            '{min_date}'::date,
                            '{max_date}'::date,
                            '1 day'::interval
                        ) gs
                    ),
                    actual AS (
                        SELECT DISTINCT {date_expr} AS d
                        FROM {fq}
                        WHERE {col} IS NOT NULL
                    ),
                    missing AS (
                        SELECT e.d FROM expected e
                        LEFT JOIN actual a ON a.d = e.d
                        WHERE a.d IS NULL
                    )
                    SELECT COUNT(*), MIN(d), MAX(d) FROM missing
                """)
                mc, mmin, mmax = cur.fetchone()
                result["missing_dates_count"] = mc
                result["first_missing_date"] = str(mmin) if mmin else None
                result["last_missing_date"] = str(mmax) if mmax else None
            else:
                result["notes"] += f" | span {span_days}d > 5yr, missing-date check skipped"
                result["missing_dates_count"] = -1  # sentinel: not computed
        except Exception as e:
            result["notes"] += f" | missing-date error: {e}"

    try:
        conn.close()
    except Exception:
        pass
    return result


# ---------------------------------------------------------------------------
# Step 3: orchestrate
# ---------------------------------------------------------------------------

def run_audit() -> list[dict]:
    # Use a single shared connection for discovery queries
    conn = get_conn()
    print("Discovering tables...")
    tables = discover_tables(conn)
    print(f"  Found {len(tables)} tables")

    print("Discovering columns...")
    col_map = discover_columns(conn)
    conn.close()

    results = []
    for i, t in enumerate(tables, 1):
        schema, tname = t["table_schema"], t["table_name"]
        is_staging = t.get("is_staging", False)
        cols = col_map.get((schema, tname), [])
        tcol = pick_temporal_column(cols)
        label = f"[STAGING] {schema}.{tname}" if is_staging else f"{schema}.{tname}"
        print(f"  [{i:3d}/{len(tables)}] {label} → temporal: {tcol['column_name'] if tcol else 'none'}")
        row = audit_table(schema, tname, tcol)
        row["is_staging"] = is_staging
        results.append(row)

    return results


# ---------------------------------------------------------------------------
# Step 4: render outputs
# ---------------------------------------------------------------------------

OUT_DIR = Path(__file__).parent


def write_csv(results: list[dict]):
    path = OUT_DIR / "table_coverage.csv"
    if not results:
        return
    fields = list(results[0].keys())
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(results)
    print(f"Wrote {path}")


def write_detailed_md(results: list[dict]):
    path = OUT_DIR / "table_coverage_detailed.md"
    lines = ["# Table Coverage — Detailed\n", f"**Generated:** {TODAY}  \n",
             f"**Tables audited:** {len(results)}\n\n---\n"]

    for r in sorted(results, key=lambda x: (x["schema_name"], x["table_name"])):
        lines.append(f"## `{r['schema_name']}.{r['table_name']}`\n")
        lines.append(f"| Field | Value |\n|---|---|\n")
        lines.append(f"| status | **{r['status']}** |\n")
        lines.append(f"| row_count | {r['row_count']} |\n")
        lines.append(f"| temporal_column | {r['selected_temporal_column']} |\n")
        lines.append(f"| temporal_type | {r['selected_temporal_type']} |\n")
        lines.append(f"| min_value | {r['min_value']} |\n")
        lines.append(f"| max_value | {r['max_value']} |\n")
        lines.append(f"| stale_days | {r['stale_days']} |\n")
        lines.append(f"| distinct_date_count | {r['distinct_date_count']} |\n")
        missing = r['missing_dates_count']
        if missing == -1:
            lines.append(f"| missing_dates_count | not computed (span > 5yr) |\n")
        else:
            lines.append(f"| missing_dates_count | {missing} |\n")
        lines.append(f"| first_missing_date | {r['first_missing_date']} |\n")
        lines.append(f"| last_missing_date | {r['last_missing_date']} |\n")
        lines.append(f"| latest_7d_rows | {r['latest_7d_rows']} |\n")
        lines.append(f"| latest_30d_rows | {r['latest_30d_rows']} |\n")
        if r["notes"]:
            lines.append(f"| notes | {r['notes']} |\n")
        lines.append("\n")

    with open(path, "w", encoding="utf-8") as f:
        f.writelines(lines)
    print(f"Wrote {path}")


def write_summary_md(results: list[dict]):
    path = OUT_DIR / "coverage_summary.md"

    total = len(results)
    by_status: dict[str, list] = {}
    for r in results:
        by_status.setdefault(r["status"], []).append(r)

    public_rows = [r for r in results if r["schema_name"] == "public"]
    mkt_rows    = [r for r in results if r["schema_name"] == "marketdata"]

    stale_sorted = sorted(
        [r for r in results if r["stale_days"] is not None],
        key=lambda x: x["stale_days"], reverse=True
    )
    fresh_large = sorted(
        [r for r in results if r["status"] in ("fresh", "slightly_stale") and (r["row_count"] or 0) > 0],
        key=lambda x: x["row_count"], reverse=True
    )[:20]

    with_missing = [r for r in results
                    if r["missing_dates_count"] and r["missing_dates_count"] > 0]
    no_temporal  = [r for r in results if r["status"] == "no_temporal_column"]
    empty_tables = [r for r in results if r["status"] == "empty"]
    error_tables = [r for r in results if r["status"] == "error"]

    def tbl_row(r):
        return (f"| `{r['schema_name']}.{r['table_name']}` "
                f"| {r['row_count']} "
                f"| {r['selected_temporal_column']} "
                f"| {r['min_value']} "
                f"| {r['max_value']} "
                f"| {r['stale_days']} "
                f"| **{r['status']}** |")

    HDR = "| table | rows | temporal_col | min | max | stale_days | status |\n|---|---|---|---|---|---|---|\n"

    lines = []
    lines.append(f"# DB Coverage Audit — Summary\n\n")
    lines.append(f"**Generated:** {TODAY}  \n")
    lines.append(f"**Database:** marketdata  \n")
    lines.append(f"**Schemas audited:** public, marketdata  \n\n---\n\n")

    # A. Totals
    lines.append("## A. Totals\n\n")
    lines.append(f"- **Total tables audited:** {total}\n")
    for s in ["fresh", "slightly_stale", "stale", "empty", "no_temporal_column", "error"]:
        lines.append(f"- **{s}:** {len(by_status.get(s, []))}\n")
    lines.append("\n")

    # B. Status counts
    lines.append("## B. Status Counts\n\n")
    lines.append("| status | count |\n|---|---|\n")
    for s, rows in sorted(by_status.items(), key=lambda x: -len(x[1])):
        lines.append(f"| {s} | {len(rows)} |\n")
    lines.append("\n")

    # C. Top 20 stalest
    lines.append("## C. Top 20 Stalest Tables\n\n")
    lines.append(HDR)
    for r in stale_sorted[:20]:
        lines.append(tbl_row(r) + "\n")
    lines.append("\n")

    # D. Top 20 freshest large
    lines.append("## D. Top 20 Freshest Large Tables\n\n")
    lines.append(HDR)
    for r in fresh_large:
        lines.append(tbl_row(r) + "\n")
    lines.append("\n")

    # E. Tables with missing dates
    lines.append("## E. Tables with Missing Dates\n\n")
    if with_missing:
        lines.append("| table | rows | temporal_col | missing_count | first_missing | last_missing | status |\n|---|---|---|---|---|---|---|\n")
        for r in sorted(with_missing, key=lambda x: -(x["missing_dates_count"] or 0)):
            lines.append(f"| `{r['schema_name']}.{r['table_name']}` "
                         f"| {r['row_count']} "
                         f"| {r['selected_temporal_column']} "
                         f"| {r['missing_dates_count']} "
                         f"| {r['first_missing_date']} "
                         f"| {r['last_missing_date']} "
                         f"| {r['status']} |\n")
    else:
        lines.append("_No tables with missing calendar dates detected._\n")
    lines.append("\n")

    # F. No temporal column
    lines.append("## F. Tables with No Temporal Column\n\n")
    if no_temporal:
        lines.append("| table | rows |\n|---|---|\n")
        for r in no_temporal:
            lines.append(f"| `{r['schema_name']}.{r['table_name']}` | {r['row_count']} |\n")
    else:
        lines.append("_All tables have at least one temporal column._\n")
    lines.append("\n")

    # G. Schema-level summary
    def schema_summary(rows, label):
        total_t = len(rows)
        total_r = sum(r["row_count"] or 0 for r in rows)
        buckets = {}
        for r in rows:
            buckets[r["status"]] = buckets.get(r["status"], 0) + 1
        return f"**{label}:** {total_t} tables, {total_r:,} total rows — " + \
               ", ".join(f"{k}: {v}" for k, v in sorted(buckets.items()))

    lines.append("## G. Schema-Level Summary\n\n")
    lines.append(schema_summary(public_rows, "public") + "\n\n")
    lines.append(schema_summary(mkt_rows, "marketdata") + "\n\n")

    lines.append("### Row Count by Schema\n\n")
    lines.append("| schema | tables | total_rows | fresh | slightly_stale | stale | empty | no_temporal | error |\n")
    lines.append("|---|---|---|---|---|---|---|---|---|\n")
    for label, rows in [("public", public_rows), ("marketdata", mkt_rows)]:
        b = {}
        for r in rows:
            b[r["status"]] = b.get(r["status"], 0) + 1
        lines.append(f"| {label} | {len(rows)} | {sum(r['row_count'] or 0 for r in rows):,} "
                     f"| {b.get('fresh',0)} | {b.get('slightly_stale',0)} | {b.get('stale',0)} "
                     f"| {b.get('empty',0)} | {b.get('no_temporal_column',0)} | {b.get('error',0)} |\n")
    lines.append("\n")

    # H. Suspected ingestion candidates
    lines.append("## H. Suspected Ingestion Candidates\n\n")

    stale_gt7 = [r for r in results if r["stale_days"] is not None and r["stale_days"] > 7]
    lines.append(f"### Stale > 7 days ({len(stale_gt7)} tables)\n\n")
    if stale_gt7:
        lines.append(HDR)
        for r in sorted(stale_gt7, key=lambda x: -x["stale_days"]):
            lines.append(tbl_row(r) + "\n")
    lines.append("\n")

    recent_missing = [r for r in with_missing
                      if r["last_missing_date"] and r["last_missing_date"] >= str(TODAY - timedelta(days=30))]
    lines.append(f"### Tables with missing dates in last 30 days ({len(recent_missing)} tables)\n\n")
    if recent_missing:
        lines.append("| table | missing_count | last_missing | status |\n|---|---|---|---|\n")
        for r in recent_missing:
            lines.append(f"| `{r['schema_name']}.{r['table_name']}` "
                         f"| {r['missing_dates_count']} "
                         f"| {r['last_missing_date']} "
                         f"| {r['status']} |\n")
    else:
        lines.append("_None._\n")
    lines.append("\n")

    prod_sounding_empty = [r for r in empty_tables
                           if not any(kw in r["table_name"] for kw in ("tmp", "temp", "test", "bak", "backup"))]
    lines.append(f"### Empty tables with production-sounding names ({len(prod_sounding_empty)} tables)\n\n")
    if prod_sounding_empty:
        lines.append("| table |\n|---|\n")
        for r in prod_sounding_empty:
            lines.append(f"| `{r['schema_name']}.{r['table_name']}` |\n")
    else:
        lines.append("_None._\n")
    lines.append("\n")

    # Error tables
    if error_tables:
        lines.append("## I. Error Tables\n\n")
        lines.append("| table | notes |\n|---|---|\n")
        for r in error_tables:
            lines.append(f"| `{r['schema_name']}.{r['table_name']}` | {r['notes']} |\n")
        lines.append("\n")

    # Methodology
    lines.append("## J. Methodology & Limitations\n\n")
    lines.append(textwrap.dedent("""
    - **Temporal column selection:** priority list (business-semantic names first, then generic
      date/time names, then any column with a temporal dtype, then created_at/updated_at as fallback).
    - **Freshness buckets:** fresh = max_date within 1 day of today; slightly_stale = 2–7 days;
      stale = > 7 days. Today is {today}.
    - **Missing dates:** computed via generate_series between MIN and MAX date for tables with span
      ≤ 5 years. Tables with longer spans are flagged but not fully enumerated.
    - **Row counts:** via COUNT(*) — exact but may be slow on very large tables.
    - **Performance:** aggregate queries only; no full-scan SELECT * was used.
    - **Read-only:** no data was modified. Session set to `readonly=True`.
    - **Schemas:** only BASE TABLEs in schemas `public` and `marketdata`. Views, foreign tables,
      partitioned parent tables excluded.
    """.format(today=TODAY)).strip() + "\n")

    with open(path, "w", encoding="utf-8") as f:
        f.writelines(lines)
    print(f"Wrote {path}")


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------

def main():
    # Verify connectivity once before the main loop
    try:
        conn = get_conn()
        conn.cursor().execute("SELECT 1")
        conn.close()
        print("Connected to DB.")
    except Exception as e:
        print(f"ERROR: Cannot connect to database: {e}", file=sys.stderr)
        sys.exit(1)

    results = run_audit()

    write_csv(results)
    write_detailed_md(results)
    write_summary_md(results)

    # Terminal summary
    print("\n" + "="*60)
    print(f"AUDIT COMPLETE — {TODAY}")
    print("="*60)
    total = len(results)
    by_status: dict[str, list] = {}
    for r in results:
        by_status.setdefault(r["status"], []).append(r)

    print(f"Total tables: {total}")
    for s in ["fresh", "slightly_stale", "stale", "empty", "no_temporal_column", "error"]:
        print(f"  {s}: {len(by_status.get(s, []))}")

    missing_tables = [r for r in results if r["missing_dates_count"] and r["missing_dates_count"] > 0]
    print(f"\nTables with missing dates: {len(missing_tables)}")

    print("\nTop 10 tables most in need of attention:")
    attention = sorted(
        [r for r in results if r["stale_days"] is not None],
        key=lambda x: x["stale_days"], reverse=True
    )[:10]
    for r in attention:
        print(f"  {r['schema_name']}.{r['table_name']}: stale_days={r['stale_days']}, "
              f"status={r['status']}, max={r['max_value']}")

    if by_status.get("error"):
        print("\nTables that could not be analyzed:")
        for r in by_status["error"]:
            print(f"  {r['schema_name']}.{r['table_name']}: {r['notes']}")

    if by_status.get("no_temporal_column"):
        print("\nTables with no temporal column:")
        for r in by_status["no_temporal_column"]:
            print(f"  {r['schema_name']}.{r['table_name']} ({r['row_count']} rows)")


if __name__ == "__main__":
    main()
