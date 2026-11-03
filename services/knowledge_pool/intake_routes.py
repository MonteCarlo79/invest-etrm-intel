"""Intake route writers — one connection per writer so route failures stay isolated."""
from __future__ import annotations

from datetime import date
from typing import Optional

import psycopg2

from services.knowledge_pool.knowledge_docs import get_conn

_PIPELINE_DDL = """
CREATE TABLE IF NOT EXISTS marketdata.province_storage_pipeline (
    id            SERIAL PRIMARY KEY,
    province      TEXT   NOT NULL,
    as_of_date    DATE   NOT NULL,
    metric        TEXT   NOT NULL,
    value         NUMERIC,
    unit          TEXT,
    source        TEXT,
    source_doc_id INT REFERENCES staging.spot_knowledge_docs(id),
    status        TEXT   NOT NULL DEFAULT 'confirmed',
    notes         TEXT,
    ingested_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_psp_nat
    ON marketdata.province_storage_pipeline (province, as_of_date, metric, COALESCE(source, ''));
CREATE INDEX IF NOT EXISTS idx_psp_prov_date
    ON marketdata.province_storage_pipeline (province, as_of_date DESC);
"""


def ensure_pipeline_table() -> None:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(_PIPELINE_DDL)
        conn.commit()


def write_pipeline_rows(rows: list[dict], *, province: Optional[str], source_doc_id: int) -> int:
    ensure_pipeline_table()
    n = 0
    with get_conn() as conn:
        with conn.cursor() as cur:
            for r in rows:
                cur.execute(
                    """
                    INSERT INTO marketdata.province_storage_pipeline
                        (province, as_of_date, metric, value, unit, source, source_doc_id, status, notes)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, 'confirmed', %s)
                    ON CONFLICT (province, as_of_date, metric, COALESCE(source, ''))
                    DO UPDATE SET value = EXCLUDED.value, unit = EXCLUDED.unit,
                                  source_doc_id = EXCLUDED.source_doc_id, ingested_at = NOW()
                    """,
                    (r.get("province") or province, r["as_of_date"], r["metric"],
                     r.get("value"), r.get("unit"), r.get("source"), source_doc_id,
                     r.get("notes")),
                )
                n += 1
        conn.commit()
    return n


def upsert_memory_note(*, app: str, subject: str, content: str, source: str = "intake") -> str:
    """Update-in-place on existing active (app, 'province_note', subject); else insert."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id FROM marketdata.agent_memory
                WHERE active AND app = %s AND category = 'province_note' AND subject = %s
                ORDER BY id DESC LIMIT 1
                """,
                (app, subject),
            )
            row = cur.fetchone()
            if row:
                cur.execute(
                    "UPDATE marketdata.agent_memory SET content = %s, source = %s WHERE id = %s",
                    (content, source, row[0]),
                )
                action = "updated"
            else:
                cur.execute(
                    """
                    INSERT INTO marketdata.agent_memory (app, category, subject, content, source)
                    VALUES (%s, 'province_note', %s, %s, %s)
                    """,
                    (app, subject, content, source),
                )
                action = "inserted"
        conn.commit()
    return action


def _write_rate_draft(*, table: str, key_cols: dict, conflict_target: str,
                      update_cols: list[str], payload: dict) -> str:
    """Shared skip-guard: never touch confirmed/superseded rows; upsert drafts only.

    key_cols: natural-key lookup for the status check (keys used verbatim, so
              expressions like "COALESCE(source, '')" are valid).
    conflict_target: exact ON CONFLICT target matching the table's unique index.
    update_cols: payload columns refreshed on conflict (natural keys excluded).
    """
    where = " AND ".join(f"{c} = %s" for c in key_cols)
    with get_conn() as conn:
        with conn.cursor() as cur:
            # EXISTS over the natural key is deterministic even when a confirmed
            # row and a draft twin (different source_file) coexist — a plain
            # "SELECT status ... LIMIT 1" has no ORDER BY and could return the
            # draft, letting the upsert overwrite the confirmed amount.
            cur.execute(f"SELECT EXISTS(SELECT 1 FROM {table} WHERE {where} "
                        f"AND status IN ('confirmed','superseded'))",
                        tuple(key_cols.values()))
            if cur.fetchone()[0]:
                return "skipped"
            cols = ", ".join(payload)
            ph = ", ".join(["%s"] * len(payload))
            updates = ", ".join(f"{c} = EXCLUDED.{c}" for c in update_cols)
            cur.execute(
                f"INSERT INTO {table} ({cols}) VALUES ({ph}) "
                f"ON CONFLICT {conflict_target} DO UPDATE SET {updates}",
                tuple(payload.values()),
            )
        conn.commit()
    return "written"


def write_capcomp_draft(*, province, effective_date, cap_comp_yuan_kw,
                        peak_duration_hours, source) -> str:
    return _write_rate_draft(
        table="marketdata.province_cap_comp",
        key_cols={"province": province, "effective_date": effective_date,
                  "COALESCE(source, '')": source},
        conflict_target="(province, effective_date, COALESCE(source, ''))",
        update_cols=["cap_comp_yuan_kw", "peak_duration_hours", "status"],
        payload={"province": province, "effective_date": effective_date,
                 "cap_comp_yuan_kw": cap_comp_yuan_kw,
                 "peak_duration_hours": peak_duration_hours,
                 "source": source, "status": "draft"},
    )


def write_fr_market_draft(*, province, effective_date, fr_price_yuan_kw_h,
                          fr_pool_billion_yuan, source) -> str:
    return _write_rate_draft(
        table="marketdata.province_fr_market",
        key_cols={"province": province, "effective_date": effective_date,
                  "COALESCE(source, '')": source},
        conflict_target="(province, effective_date, COALESCE(source, ''))",
        update_cols=["fr_price_yuan_kw_h", "fr_pool_billion_yuan", "status"],
        payload={"province": province, "effective_date": effective_date,
                 "fr_price_yuan_kw_h": fr_price_yuan_kw_h,
                 "fr_pool_billion_yuan": fr_pool_billion_yuan,
                 "source": source, "status": "draft"},
    )


def write_ancillary_draft(*, province, month, metric, amount_yuan, source_file) -> str:
    return _write_rate_draft(
        table="marketdata.province_ancillary_revenue",
        key_cols={"province": province, "month": month, "metric": metric},
        conflict_target="(province, month, metric, COALESCE(source_file, ''))",
        update_cols=["amount_yuan", "status"],
        payload={"province": province, "month": month, "metric": metric,
                 "amount_yuan": amount_yuan, "source_file": source_file,
                 "status": "draft"},
    )


# ── fundamentals_stat (province_fundamentals_monthly, spec 2026-11-02 写入路径 2) ──

_FUND_CONFIRMED_SQL = """
SELECT EXISTS(SELECT 1 FROM marketdata.province_fundamentals_monthly
              WHERE province = %s AND month = %s AND period = %s
                AND section = %s AND metric = %s
                AND status IN ('confirmed','superseded'))
"""


def _pg_url() -> str:
    from services.knowledge_pool.db import _get_url
    return _get_url()[0]


def _fund_natkey(fr) -> tuple:
    return (fr.province, fr.month, fr.period, fr.section, fr.metric)


def _fund_confirmed_keys(rows, *, pg_url: str) -> set[tuple]:
    """Natural keys already holding a confirmed/superseded row.

    Source-agnostic on purpose (same semantics as _write_rate_draft's EXISTS
    guard): a draft must never flip a confirmed row, even a same-source one —
    store.write_rows' within-0.5% "overwrite" verdict would otherwise upsert a
    same-source confirmed row down to status='draft'.
    """
    blocked: set[tuple] = set()
    if not rows:
        return blocked
    with psycopg2.connect(pg_url) as conn:
        with conn.cursor() as cur:
            for fr in rows:
                cur.execute(_FUND_CONFIRMED_SQL, _fund_natkey(fr))
                if cur.fetchone()[0]:
                    blocked.add(_fund_natkey(fr))
    return blocked


def _as_month(v) -> date:
    s = str(v)[:10]
    if len(s) == 7:                      # bare "YYYY-MM" → month start
        s += "-01"
    return date.fromisoformat(s)


def write_fundamentals_draft(*, province: str, rows: list[dict], source: str) -> str:
    """Land fundamentals_stat rows as status='draft'; never touch confirmed rows.

    rows: dicts shaped like the route's structured payload —
    {month (YYYY-MM-01 or YYYY-MM), period (month|year, default month), section,
    metric, value (None ok), unit, source? , province?}. Per-row province/source
    override the call-level fallbacks (registry-parsed national workbooks carry
    their own province; vision rows fall back to the route/doc province and
    "intake:<title>" source). Rows missing month/section/metric/unit are counted
    as malformed, not written. Returns "written N, skipped M[, malformed K]".
    """
    from services.province_fundamentals import store as _store      # lazy: route isolation
    from services.province_fundamentals.models import FundRow

    prepared, malformed = [], 0
    for r in rows:
        try:
            fr = FundRow(province=r.get("province") or province,
                         month=_as_month(r["month"]),
                         period=r.get("period") or "month",
                         section=r["section"], metric=r["metric"],
                         value=r.get("value"), unit=r.get("unit") or "",
                         source=r.get("source") or source)
            if not fr.unit or not fr.province:
                raise ValueError("unit/province required")
        except (KeyError, ValueError, TypeError):
            malformed += 1
            continue
        prepared.append(fr)
    if not prepared:
        return f"written 0, skipped 0" + (f", malformed {malformed}" if malformed else "")

    url = _pg_url()
    blocked = _fund_confirmed_keys(prepared, pg_url=url)
    survivors = [fr for fr in prepared if _fund_natkey(fr) not in blocked]
    stats = (_store.write_rows(survivors, pg_url=url, default_status="draft")
             if survivors else {"written": 0, "conflicts": 0, "skipped": 0})
    skipped = len(prepared) - len(survivors) + stats["skipped"]
    out = f"written {stats['written']}, skipped {skipped}"
    if malformed:
        out += f", malformed {malformed}"
    if stats["conflicts"]:
        out += f", conflicts {stats['conflicts']}"
    return out


def read_storage_pipeline(conn, provinces: Optional[list[str]] = None,
                          metric: Optional[str] = None) -> dict:
    """Latest confirmed value per (province, metric) + history (≤200 rows)."""
    sql = ("SELECT province, as_of_date, metric, value, unit, source, status, notes "
           "FROM marketdata.province_storage_pipeline WHERE status = 'confirmed'")
    params: list = []
    if provinces:
        sql += " AND province = ANY(%s)"; params.append(provinces)
    if metric:
        sql += " AND metric = %s"; params.append(metric)
    sql += " ORDER BY province, metric, as_of_date DESC"
    with conn.cursor() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()
    cols = ["province", "as_of_date", "metric", "value", "unit", "source", "status", "notes"]
    recs = [dict(zip(cols, (str(v) if c == "as_of_date" else (float(v) if c == "value" and v is not None else v)
                            for c, v in zip(cols, r)))) for r in rows]
    seen, latest = set(), []
    for r in recs:
        k = (r["province"], r["metric"])
        if k not in seen:
            seen.add(k); latest.append(r)
    return {"count": len(recs), "latest": latest, "history": recs[:200]}
