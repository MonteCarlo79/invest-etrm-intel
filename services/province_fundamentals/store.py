"""Read/write layer for marketdata.province_fundamentals_monthly.

Write rules (spec 2026-11-02, docs/superpowers/specs/2026-11-02-market-fundamentals-tab-design.md):
- Natural key: (province, month, period, section, metric). DB uniqueness adds
  COALESCE(source, ''), so a restatement from a NEW source lands as a separate row.
- Per-row pre-check of the current confirmed value for the natural key:
    no confirmed value                  -> write with default_status
    equal to confirmed value            -> skip (idempotent backfill re-runs)
    |new-old|/|old| <= 0.5% (or new None) -> overwrite (upsert, default_status)
    |new-old|/|old| >  0.5%             -> do NOT overwrite; write with status='conflict'
      AND source tagged '#conflict', so the conflict row is always a NEW row (its
      unique key differs from the confirmed row's raw source). Without the tag, a
      same-source restatement would flip the confirmed row in place — destroying the
      old value and leaving the key with zero confirmed rows (不覆盖 rule broken).
- Rows are deduped last-wins on the full unique key (province, month, period, section,
  metric, source) before batching: two identical keys in one INSERT ... ON CONFLICT
  batch make PostgreSQL reject the whole statement ("ON CONFLICT DO UPDATE command
  cannot affect row a second time") and roll back everything.
- Bulk writes via psycopg2.extras.execute_values (never row-by-row INSERT over WAN).

Read rule: confirmed-only, latest-created row per (month, period, metric) via
DISTINCT ON. period is in the distinct key deliberately: the January month row and
the year row share month=YYYY-01-01 and must not collapse into each other.
"""
from __future__ import annotations

from typing import Optional

import psycopg2
import psycopg2.extras

from services.province_fundamentals.models import FundRow

_TABLE = "marketdata.province_fundamentals_monthly"
_CONFLICT_TOL = 0.005  # 0.5%

_INSERT_SQL = f"""
INSERT INTO {_TABLE}
    (province, month, period, section, metric, value, unit, source, status)
VALUES %s
ON CONFLICT (province, month, period, section, metric, COALESCE(source, ''))
DO UPDATE SET value = EXCLUDED.value, unit = EXCLUDED.unit, status = EXCLUDED.status
"""

_CURRENT_CONFIRMED_SQL = f"""
SELECT value FROM {_TABLE}
WHERE province = %s AND month = %s AND period = %s AND section = %s AND metric = %s
  AND status = 'confirmed'
ORDER BY created_at DESC
LIMIT 1
"""


def _decide(old, new) -> str:
    """'skip' | 'overwrite' | 'conflict' — old is the current confirmed value (or None)."""
    if old is None:
        return "overwrite"                       # no confirmed value yet -> fresh write
    if new is None:
        return "overwrite"                       # deviation undefined -> let writer through
    old_f, new_f = float(old), float(new)
    if old_f == new_f:
        return "skip"
    if old_f == 0.0:
        return "conflict"                        # relative deviation undefined -> conservative
    return ("overwrite" if abs(new_f - old_f) / abs(old_f) <= _CONFLICT_TOL
            else "conflict")


def write_rows(rows: list[FundRow], *, pg_url: str,
               default_status: str = "confirmed") -> dict:
    """Bulk-write rows with the 0.5% restatement guard.

    Returns {"written": n, "conflicts": n, "skipped": n}.
    """
    stats = {"written": 0, "conflicts": 0, "skipped": 0}
    if not rows:
        return stats
    # Last-wins dedupe on the full DB unique key — duplicate keys in one batch make
    # PG reject ON CONFLICT ("cannot affect row a second time") and roll back all.
    deduped: dict[tuple, FundRow] = {}
    for r in rows:
        deduped[(r.province, r.month, r.period, r.section, r.metric, r.source)] = r
    batch: list[tuple] = []
    with psycopg2.connect(pg_url) as conn:
        with conn.cursor() as cur:
            for r in deduped.values():
                cur.execute(_CURRENT_CONFIRMED_SQL,
                            (r.province, r.month, r.period, r.section, r.metric))
                got = cur.fetchone()
                verdict = _decide(got[0] if got else None, r.value)
                if verdict == "skip":
                    stats["skipped"] += 1
                    continue
                if verdict == "overwrite":
                    stats["written"] += 1
                    status, source = default_status, r.source
                else:
                    stats["conflicts"] += 1
                    # '#conflict' tag keeps the row's unique key distinct from the
                    # confirmed row even when the source filename is identical.
                    status, source = "conflict", f"{r.source}#conflict"
                batch.append((r.province, r.month, r.period, r.section, r.metric,
                              r.value, r.unit, source, status))
            if batch:
                psycopg2.extras.execute_values(cur, _INSERT_SQL, batch, page_size=1000)
        conn.commit()
    return stats


def read_series(province: str, section: str, metrics: Optional[list[str]] = None,
                *, pg_url: str, months: int = 24) -> list[dict]:
    """Confirmed-only series for one province+section over the last `months` months.

    Latest-created confirmed row per (month, period, metric). Returns dicts with
    province, month, period, section, metric, value (float|None), unit, source,
    created_at.
    """
    sql = f"""
SELECT DISTINCT ON (month, period, metric)
    province, month, period, section, metric, value, unit, source, created_at
FROM {_TABLE}
WHERE province = %s AND section = %s AND status = 'confirmed'
  AND month >= (date_trunc('month', CURRENT_DATE) - INTERVAL '1 month' * %s)::date
"""
    params: list = [province, section, months]
    if metrics:
        sql += "  AND metric = ANY(%s)\n"
        params.append(list(metrics))
    sql += "ORDER BY month, period, metric, created_at DESC"
    with psycopg2.connect(pg_url) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
    return [
        {"province": p, "month": m, "period": per, "section": sec, "metric": met,
         "value": float(v) if v is not None else None,
         "unit": u, "source": src, "created_at": ts}
        for p, m, per, sec, met, v, u, src, ts in rows
    ]
