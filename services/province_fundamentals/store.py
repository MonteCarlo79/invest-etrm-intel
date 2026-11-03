"""Read/write layer for marketdata.province_fundamentals_monthly.

Write rules (spec 2026-11-02, docs/superpowers/specs/2026-11-02-market-fundamentals-tab-design.md):
- Natural key: (province, month, period, section, metric). DB uniqueness adds
  COALESCE(source, ''), so a restatement from a NEW source lands as a separate row.
- Per-row pre-check of the current confirmed value for the natural key:
    no confirmed value                  -> write with default_status
    equal to confirmed value            -> skip (idempotent backfill re-runs)
    new None over a confirmed number    -> skip (blanking restatement must not
      erase the confirmed value; None over None still writes so reported-but-empty
      cells can land)
    |new-old|/|old| <= 0.5%             -> overwrite (upsert, default_status)
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
        # None over a confirmed number is a blanking restatement (later cumulative
        # workbook empties a prior month's cell) — never let it erase the number:
        # a different-source row would shadow it via DISTINCT-ON, a same-source
        # row would null it via ON CONFLICT. None->None stays writable so the
        # record-empty-cell path keeps working.
        return "overwrite" if old is None else "skip"
    old_f, new_f = float(old), float(new)
    if old_f == new_f:
        return "skip"
    if old_f == 0.0:
        return "conflict"                        # relative deviation undefined -> conservative
    return ("overwrite" if abs(new_f - old_f) / abs(old_f) <= _CONFLICT_TOL
            else "conflict")


# ── Cross-source component-sum sanity check (2026-11-03, option A) ────────────
# 交易中心月报 vintage 错乱时, 分项行会离谱偏离总计行 (安徽 2026-01~03:
# 分项加总 ≈ 总计的 10%). 规则: 同 (province, month, section) 内,
# 火电/水电/核电/风电/光伏 加总 < 总计 × 0.6 且 ≥3 个分项非空时,
# 分项行判为存疑 (写 conflict, 不进入 confirmed 序列), 总计行保留.
# 单位必须与总计一致才比较 (单位异常则跳过校验, 不误伤).

_FUEL_COMPONENTS = ("thermal", "hydro", "nuclear", "wind", "solar")
_SUM_RATIO_MIN = 0.6


def _crosscheck_components(rows: list[FundRow]):
    """(clean, suspect, warnings) — 见上注释."""
    by_key: dict = {}
    for r in rows:
        by_key.setdefault((r.province, r.month, r.section), []).append(r)
    clean, suspect, warns = [], [], []
    for (prov, month, section), grp in by_key.items():
        total_row = next(
            (r for r in grp if r.metric.endswith("total") or r.metric.endswith("total_full")),
            None)
        comps = [r for r in grp
                 if any(r.metric.endswith(f"_{f}") or r.metric.endswith(f"_{f}_full")
                        for f in _FUEL_COMPONENTS)]
        present = [r for r in comps if r.value is not None]
        if total_row is None or total_row.value in (None, 0) or len(present) < 3:
            clean.extend(grp)
            continue
        if any(r.unit != total_row.unit for r in present):
            clean.extend(grp)  # 单位不一致, 跳过校验
            continue
        ratio = sum(r.value for r in present) / total_row.value
        if ratio < _SUM_RATIO_MIN:
            suspect.extend(present)
            clean.extend([r for r in grp if r not in present])
            warns.append(
                f"{prov} {month} {section}: 分项加总 "
                f"{sum(r.value for r in present):.1f} << 总计 {total_row.value:.1f} "
                f"({ratio:.0%}) — {len(present)} 个分项行标为存疑(源数据 vintage 错乱)")
        else:
            clean.extend(grp)
    return clean, suspect, warns


def write_rows(rows: list[FundRow], *, pg_url: str,
               default_status: str = "confirmed") -> dict:
    """Bulk-write rows with the 0.5% restatement guard + component-sum crosscheck.

    Suspect component rows (see _crosscheck_components) are written as conflict
    regardless of default_status. Returns
    {"written": n, "conflicts": n, "skipped": n, "suspect": n, "warnings": [...]}.
    """
    stats = {"written": 0, "conflicts": 0, "skipped": 0,
             "suspect": 0, "warnings": []}
    if not rows:
        return stats
    rows, suspect_rows, stats["warnings"] = _crosscheck_components(rows)
    stats["suspect"] = len(suspect_rows)
    # Last-wins dedupe on the full DB unique key — duplicate keys in one batch make
    # PG reject ON CONFLICT ("cannot affect row a second time") and roll back all.
    deduped: dict[tuple, FundRow] = {}
    for r in list(rows) + list(suspect_rows):
        deduped[(r.province, r.month, r.period, r.section, r.metric, r.source,
                 r in suspect_rows)] = r
    batch: list[tuple] = []
    with psycopg2.connect(pg_url) as conn:
        with conn.cursor() as cur:
            for key, r in deduped.items():
                is_suspect = key[6]
                if is_suspect:
                    stats["conflicts"] += 1
                    batch.append((r.province, r.month, r.period, r.section, r.metric,
                                  r.value, r.unit, f"{r.source}#suspect", "conflict"))
                    continue
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
