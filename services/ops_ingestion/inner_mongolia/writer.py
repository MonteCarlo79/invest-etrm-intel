"""
services/ops_ingestion/inner_mongolia/writer.py

Idempotent, supersession-aware writer for Inner Mongolia BESS ops dispatch data.

Responsibilities
----------------
1. compute_file_hash(path)      → SHA-256 hex of raw file bytes
2. ensure_tables(engine)        → CREATE TABLE IF NOT EXISTS (from DDL constants)
3. ingest_file(...)             → full pipeline: hash check → parse → match → verify → write

Two distinct reprocess cases
-----------------------------
Case 1 — Exact same file bytes (same file_hash):
  Without --force : skip (status='skipped_duplicate' or 'skipped_superseded').
  With    --force : reuse the existing registry row in-place (Option A).
                   Reset parse_status to 'pending', reprocess sheets/facts via ON CONFLICT
                   DO UPDATE, then mark 'success'.  No new registry row is created, so
                   UNIQUE(file_hash) is never violated.

Case 2 — Corrected replacement file (different file_hash, same report_date):
  Always creates a new registry row (ingest_version = MAX+1, is_current=TRUE).
  Previous is_current row is marked is_current=FALSE (superseded).
  Fact rows are updated to point at the new source_file_id via ON CONFLICT DO UPDATE.

All steps are wrapped in a single transaction. On failure, everything rolls back.
"""
from __future__ import annotations

import datetime
import hashlib
import json
import logging
import time
from dataclasses import dataclass, field
from typing import List, Optional

from sqlalchemy import text

from .date_parser import parse_date
from .matcher import load_asset_map, match_sheet, MatchResult
from .parser import parse_workbook, SheetParseResult
from .price_verifier import (
    verify_prices,
    verify_prices_no_db,
    PriceVerificationResult,
)

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Retry configuration
# ---------------------------------------------------------------------------

_MAX_RETRIES = 3          # total attempts per file (first try + 2 retries)
_RETRY_DELAY_BASE = 1.0   # seconds; doubles each retry: 1 s, 2 s

# Substrings that identify transient DB / SSL / network errors (lower-cased).
_TRANSIENT_ERROR_STRINGS: tuple = (
    'ssl syscall error',
    'connection abort',
    'connection reset',
    'connection refused',
    'could not connect',
    'server closed the connection',
    'connection timed out',
    'broken pipe',
    'no connection to the server',
    'connection is closed',
    'lost connection',
)


# ---------------------------------------------------------------------------
# Result type
# ---------------------------------------------------------------------------

@dataclass
class IngestResult:
    path: str
    report_date: Optional[str]       # ISO date
    file_hash: str
    status: str                       # 'skipped_duplicate' | 'skipped_superseded' | 'dry_run'
                                      # | 'success' | 'failed'
                                      # | 'unsupported_format' | 'no_dispatch_section' | 'partial_bundle'
    file_id: Optional[int] = None     # new registry id
    ingest_version: Optional[int] = None
    sheets_matched: int = 0
    rows_written: int = 0
    notes: str = ""
    sheet_results: List[dict] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def compute_file_hash(path: str) -> str:
    """SHA-256 hex digest of the raw file bytes."""
    h = hashlib.sha256()
    with open(path, 'rb') as fh:
        for chunk in iter(lambda: fh.read(65536), b''):
            h.update(chunk)
    return h.hexdigest()


def ingest_file(
    path: str,
    engine,
    force: bool = False,
    dry_run: bool = False,
    year_hint: Optional[int] = None,
    verify_prices_flag: bool = False,
) -> IngestResult:
    """
    Full ingestion pipeline for one Excel file.

    Parameters
    ----------
    path : str
        Path to the .xlsx file.
    engine : sqlalchemy.Engine
    force : bool
        If True, re-process even if file_hash is already in the registry.
    dry_run : bool
        Parse + match + (optionally) verify without writing to DB.
    year_hint : int | None
        Passed to date_parser.parse_date as year override.
    verify_prices_flag : bool
        If True, run price verification against md_id_cleared_energy.

    Returns
    -------
    IngestResult
    """
    import os
    basename = os.path.basename(path)
    file_hash = compute_file_hash(path)

    # ------------------------------------------------------------------
    # 1. Hash dedup check (skip DB check in dry_run)
    #
    # Two cases:
    #   a) Same hash, no --force  → skip
    #   b) Same hash,    --force  → force_reprocess_id set; reuse existing row (Option A)
    # ------------------------------------------------------------------
    force_reprocess_id: Optional[int] = None    # set when reusing an existing registry row
    force_reprocess_version: Optional[int] = None

    if not dry_run:
        existing = _lookup_by_hash(engine, file_hash)
        if existing is not None:
            if not force:
                if existing['is_current']:
                    log.info("Exact duplicate (hash match), skipping: %s", basename)
                    return IngestResult(
                        path=path, report_date=existing['report_date'],
                        file_hash=file_hash, status='skipped_duplicate',
                        file_id=existing['id'], notes="Exact duplicate; use --force to reprocess",
                    )
                else:
                    log.info("File was already superseded (hash match), skipping: %s", basename)
                    return IngestResult(
                        path=path, report_date=existing['report_date'],
                        file_hash=file_hash, status='skipped_superseded',
                        file_id=existing['id'],
                        notes="File was already superseded; this is a historical record",
                    )
            else:
                # --force + same hash → reuse existing registry row in-place
                force_reprocess_id = existing['id']
                force_reprocess_version = existing['ingest_version']
                if existing['is_current']:
                    log.info(
                        "Force-reprocessing existing registry row (same hash, id=%d): %s",
                        force_reprocess_id, basename,
                    )
                else:
                    log.warning(
                        "Force-reprocessing a superseded registry row (id=%d, hash=%s). "
                        "This will overwrite fact rows that may already point to a newer file.",
                        force_reprocess_id, file_hash[:12],
                    )

    # ------------------------------------------------------------------
    # 2. Parse date
    # ------------------------------------------------------------------
    report_date = parse_date(path, year_hint=year_hint)
    report_date_str = report_date.isoformat()

    # ------------------------------------------------------------------
    # 3. Load asset map + parse workbook
    # ------------------------------------------------------------------
    asset_map = load_asset_map(engine=(None if dry_run else engine))
    sheet_results: List[SheetParseResult] = parse_workbook(path, report_date)

    # ------------------------------------------------------------------
    # 4. Match sheets + optionally verify prices
    # ------------------------------------------------------------------
    matched: List[dict] = []   # {sheet_result, match_result, verify_result}
    for sr in sheet_results:
        mr = match_sheet(sr.sheet_name, asset_map)
        if verify_prices_flag and mr.asset_code and not dry_run:
            vr = verify_prices(
                excel_rows=[_row_to_dict(r) for r in sr.rows],
                dispatch_unit_name=mr.dispatch_unit_name or "",
                data_date=report_date_str,
                engine=engine,
            )
        else:
            vr = verify_prices_no_db([_row_to_dict(r) for r in sr.rows])
        matched.append({'sheet': sr, 'match': mr, 'verify': vr})

    sheets_matched = sum(1 for m in matched if m['match'].asset_code is not None)
    total_rows = sum(len(m['sheet'].rows) for m in matched if m['match'].asset_code)

    sheet_summary = [
        {
            'sheet_name': m['sheet'].sheet_name,
            'asset_code': m['match'].asset_code,
            'match_method': m['match'].match_method,
            'n_rows': m['sheet'].n_rows,
            'price_verification_level': m['verify'].price_verification_level,
            'price_verification_notes': m['verify'].price_verification_notes,
        }
        for m in matched
    ]

    # ------------------------------------------------------------------
    # 4b. Classify zero-row results before any DB work
    # ------------------------------------------------------------------
    zero_row_status: Optional[str] = None
    zero_row_notes: Optional[str] = None
    if total_rows == 0 and not dry_run:
        zero_row_status, zero_row_notes = _classify_zero_rows(sheet_results, matched)
        log.warning(
            "Zero-row result for %s (date=%s): status=%s — %s",
            basename, report_date_str, zero_row_status, zero_row_notes,
        )

    # ------------------------------------------------------------------
    # 5. Dry run — print summary and return
    # ------------------------------------------------------------------
    if dry_run:
        dry_note = zero_row_notes or "Dry run — no DB writes"
        dry_status = zero_row_status or 'dry_run'
        log.info(
            "[DRY RUN] %s | date=%s | sheets_matched=%d/%d | rows=%d | status=%s",
            basename, report_date_str, sheets_matched, len(matched), total_rows, dry_status,
        )
        for s in sheet_summary:
            log.info("  Sheet %-30s → asset=%-15s method=%-10s rows=%d verify=%s",
                     s['sheet_name'], s['asset_code'] or 'UNMATCHED',
                     s['match_method'], s['n_rows'], s['price_verification_level'])
        return IngestResult(
            path=path, report_date=report_date_str, file_hash=file_hash,
            status='dry_run', sheets_matched=sheets_matched, rows_written=total_rows,
            notes=dry_note, sheet_results=sheet_summary,
        )

    # ------------------------------------------------------------------
    # 6. Write to DB — with per-file retry for transient network/SSL errors.
    #
    # Rollback is automatic: engine.begin() rolls back on any exception.
    # For force-reprocess (existing row), file_id is stable across retries.
    # For a new file, file_id is assigned inside the transaction; a rolled-back
    # transaction leaves the DB clean — the next retry re-inserts a fresh row.
    # ------------------------------------------------------------------
    file_id: Optional[int] = force_reprocess_id         # None for new files
    ingest_version: Optional[int] = force_reprocess_version
    final_status: str = zero_row_status or 'success'

    for attempt in range(1, _MAX_RETRIES + 1):
        rows_written = 0
        try:
            with engine.begin() as conn:
                if force_reprocess_id is not None:
                    # ---- Case 1b: same hash + --force ----
                    file_id = force_reprocess_id
                    ingest_version = force_reprocess_version
                    _reset_registry_pending(conn, file_id, path, sheet_count=len(sheet_results))
                    # _supersede_previous intentionally skipped: supersession chain
                    # is unchanged when the file itself hasn't changed.
                else:
                    # ---- Case 2: new file (different hash or first ingest) ----
                    ingest_version = _next_ingest_version(conn, report_date_str)
                    file_id = _insert_registry(
                        conn, path, file_hash, report_date_str, ingest_version,
                        sheet_count=len(sheet_results),
                    )
                    _supersede_previous(conn, report_date_str, file_id)

                # Write sheet map (always — records match/verify results even for zero-row)
                for m in matched:
                    _upsert_sheet_map(conn, file_id, m['match'], m['verify'])

                # Write fact rows only when data exists
                if total_rows > 0:
                    for m in matched:
                        if m['match'].asset_code is None:
                            continue   # skip unmatched sheets
                        rows_written += _upsert_dispatch_rows(conn, file_id, m['sheet'], m['match'])

                _update_registry_status(conn, file_id, final_status, rows_written,
                                        notes=zero_row_notes)

            # ---- success ----
            action = "Force-reprocessed" if force_reprocess_id is not None else "Ingested"
            log.info(
                "%s %s | date=%s | v%d | sheets=%d/%d | rows=%d | status=%s",
                action, basename, report_date_str, ingest_version,
                sheets_matched, len(matched), rows_written, final_status,
            )
            return IngestResult(
                path=path, report_date=report_date_str, file_hash=file_hash,
                status=final_status, file_id=file_id, ingest_version=ingest_version,
                sheets_matched=sheets_matched, rows_written=rows_written,
                sheet_results=sheet_summary,
            )

        except Exception as exc:
            if _is_transient_db_error(exc) and attempt < _MAX_RETRIES:
                wait = _RETRY_DELAY_BASE * (2 ** (attempt - 1))
                log.warning(
                    "Transient DB error on attempt %d/%d for %s — rolled back; "
                    "retrying in %.0fs: %s",
                    attempt, _MAX_RETRIES, basename, wait, exc,
                )
                time.sleep(wait)
                continue

            # Non-transient error, or max retries exhausted
            retry_suffix = f" (failed after {attempt} attempt(s))" if attempt > 1 else ""
            log.exception("Failed to ingest %s%s: %s", basename, retry_suffix, exc)

            # Best-effort: persist failed status to registry (may itself fail if
            # the connection is completely gone — caught and ignored silently).
            if file_id is not None:
                try:
                    with engine.begin() as conn:
                        _update_registry_status(conn, file_id, 'failed', 0,
                                                notes=str(exc)[:500])
                except Exception:
                    pass
            return IngestResult(
                path=path, report_date=report_date_str, file_hash=file_hash,
                status='failed', notes=str(exc), sheet_results=sheet_summary,
            )


def ensure_tables(engine) -> None:
    """
    Run CREATE TABLE IF NOT EXISTS for all ops ingestion tables.
    Safe to call on every startup — idempotent.
    """
    ddl_path = _find_ddl_path()
    if ddl_path is None:
        log.warning(
            "ops_bess_dispatch.sql not found — tables must be created manually. "
            "Expected location: db/ddl/marketdata/ops_bess_dispatch.sql"
        )
        return
    with open(ddl_path, 'r', encoding='utf-8') as fh:
        ddl_sql = fh.read()
    with engine.begin() as conn:
        conn.execute(text(ddl_sql))
    log.info("Tables ensured from %s", ddl_path)


# ---------------------------------------------------------------------------
# Transient-error detection
# ---------------------------------------------------------------------------

def _is_transient_db_error(exc: Exception) -> bool:
    """
    Return True if *exc* is a transient DB / SSL / network error worth retrying.

    Matches SQLAlchemy OperationalError (which wraps psycopg2 errors) and raw
    psycopg2.OperationalError, keyed on well-known substrings in the message.
    """
    from sqlalchemy.exc import OperationalError as SAOperationalError

    is_op = isinstance(exc, SAOperationalError)
    if not is_op:
        try:
            import psycopg2
            is_op = isinstance(exc, psycopg2.OperationalError)
        except ImportError:
            pass
    if not is_op:
        return False

    msg = str(exc).lower()
    return any(s in msg for s in _TRANSIENT_ERROR_STRINGS)


# ---------------------------------------------------------------------------
# Zero-row classification
# ---------------------------------------------------------------------------

def _classify_zero_rows(
    sheet_results: List,
    matched: List[dict],
) -> tuple:
    """
    Classify why an ingest produced zero fact rows.

    Returns (parse_status, notes_str) where parse_status is one of:
      'unsupported_format'  — parse_workbook returned no sheets at all
      'no_dispatch_section' — sheets parsed but none matched a known asset
      'partial_bundle'      — asset sheets matched but contained no data rows
    """
    if not sheet_results:
        return (
            'unsupported_format',
            'No parseable dispatch sheets found in workbook',
        )

    sheets_matched = sum(1 for m in matched if m['match'].asset_code is not None)
    if sheets_matched == 0:
        names = [m['sheet'].sheet_name for m in matched]
        return (
            'no_dispatch_section',
            f'Sheets found but none matched a known asset: {names}',
        )

    # Sheets matched but every row list was empty
    return (
        'partial_bundle',
        f'Matched {sheets_matched} asset sheet(s) but all had 0 data rows',
    )


# ---------------------------------------------------------------------------
# Internal DB helpers
# ---------------------------------------------------------------------------

def _lookup_by_hash(engine, file_hash: str) -> Optional[dict]:
    sql = text("""
        SELECT id, report_date::text AS report_date, is_current, ingest_version
        FROM marketdata.ops_dispatch_file_registry
        WHERE file_hash = :hash
        LIMIT 1
    """)
    with engine.connect() as conn:
        row = conn.execute(sql, {"hash": file_hash}).fetchone()
    if row is None:
        return None
    return {
        "id": row.id,
        "report_date": row.report_date,
        "is_current": row.is_current,
        "ingest_version": row.ingest_version,
    }


def _reset_registry_pending(conn, file_id: int, path: str, sheet_count: int) -> None:
    """
    Reset an existing registry row back to 'pending' before force-reprocessing.

    Updates parse_status, ingested_at, source_file_path, source_file_name, and
    sheet_count to reflect this reprocess run.  ingest_version, is_current,
    supersedes_file_id, and file_hash are intentionally left unchanged.
    """
    import os
    sql = text("""
        UPDATE marketdata.ops_dispatch_file_registry
        SET parse_status     = 'pending',
            ingested_at      = now(),
            source_file_path = :fpath,
            source_file_name = :fname,
            sheet_count      = :sc,
            notes            = 'force-reprocessed'
        WHERE id = :fid
    """)
    conn.execute(sql, {
        "fid": file_id,
        "fpath": path,
        "fname": os.path.basename(path),
        "sc": sheet_count,
    })


def _next_ingest_version(conn, report_date_str: str) -> int:
    sql = text("""
        SELECT COALESCE(MAX(ingest_version), 0) + 1 AS next_version
        FROM marketdata.ops_dispatch_file_registry
        WHERE report_date = :rd
    """)
    row = conn.execute(sql, {"rd": report_date_str}).fetchone()
    return row.next_version


def _insert_registry(conn, path: str, file_hash: str, report_date_str: str,
                     ingest_version: int, sheet_count: int) -> int:
    import os
    sql = text("""
        INSERT INTO marketdata.ops_dispatch_file_registry
          (source_file_name, source_file_path, file_hash, report_date,
           ingest_version, sheet_count, parse_status, is_current)
        VALUES
          (:name, :fpath, :hash, :rd, :ver, :sc, 'pending', TRUE)
        RETURNING id
    """)
    row = conn.execute(sql, {
        "name": os.path.basename(path),
        "fpath": path,
        "hash": file_hash,
        "rd": report_date_str,
        "ver": ingest_version,
        "sc": sheet_count,
    }).fetchone()
    return row.id


def _supersede_previous(conn, report_date_str: str, new_file_id: int) -> None:
    """Mark all other is_current=TRUE rows for the same report_date as superseded."""
    sql = text("""
        UPDATE marketdata.ops_dispatch_file_registry
        SET is_current = FALSE,
            supersedes_file_id = CASE WHEN supersedes_file_id IS NULL THEN id ELSE supersedes_file_id END
        WHERE report_date = :rd
          AND id != :new_id
          AND is_current = TRUE
    """)
    conn.execute(sql, {"rd": report_date_str, "new_id": new_file_id})


def _upsert_sheet_map(conn, file_id: int, mr: MatchResult, vr: PriceVerificationResult) -> None:
    sql = text("""
        INSERT INTO marketdata.ops_dispatch_asset_sheet_map
          (source_file_id, sheet_name, asset_nickname_cn, asset_bracket_name_cn,
           matched_asset_code, matched_dispatch_unit, matched_plant_name, match_method,
           price_match_n, price_match_mae, price_match_r,
           price_verification_level, price_verification_notes)
        VALUES
          (:fid, :sn, :nick, :bracket, :ac, :unit, :plant, :method,
           :n, :mae, :r, :level, :notes)
        ON CONFLICT (source_file_id, sheet_name) DO UPDATE SET
          matched_asset_code       = EXCLUDED.matched_asset_code,
          matched_dispatch_unit    = EXCLUDED.matched_dispatch_unit,
          match_method             = EXCLUDED.match_method,
          price_match_n            = EXCLUDED.price_match_n,
          price_match_mae          = EXCLUDED.price_match_mae,
          price_match_r            = EXCLUDED.price_match_r,
          price_verification_level = EXCLUDED.price_verification_level,
          price_verification_notes = EXCLUDED.price_verification_notes
    """)
    conn.execute(sql, {
        "fid": file_id,
        "sn": mr.sheet_name,
        "nick": mr.nickname_cn,
        "bracket": mr.bracket_cn,
        "ac": mr.asset_code,
        "unit": mr.dispatch_unit_name,
        "plant": mr.plant_name,
        "method": mr.match_method,
        "n": vr.price_match_n,
        "mae": vr.price_match_mae,
        "r": vr.price_match_r,
        "level": vr.price_verification_level,
        "notes": vr.price_verification_notes,
    })


def _upsert_dispatch_rows(conn, file_id: int, sr: SheetParseResult, mr: MatchResult) -> int:
    """Upsert all parsed rows for one matched sheet. Returns number of rows processed."""
    sql = text("""
        INSERT INTO marketdata.ops_bess_dispatch_15min
          (asset_code, interval_start, interval_end, data_date,
           source_file_id, sheet_name, dispatch_unit_name,
           nominated_dispatch_mw, actual_dispatch_mw, nodal_price_excel,
           raw_nominated, raw_actual, raw_nodal_price, raw_payload)
        VALUES
          (:asset_code, :interval_start, :interval_end, :data_date,
           :source_file_id, :sheet_name, :dispatch_unit_name,
           :nominated_mw, :actual_mw, :nodal_price,
           :raw_nom, :raw_act, :raw_price, :raw_payload)
        ON CONFLICT (asset_code, interval_start) DO UPDATE SET
          source_file_id        = EXCLUDED.source_file_id,
          sheet_name            = EXCLUDED.sheet_name,
          dispatch_unit_name    = EXCLUDED.dispatch_unit_name,
          nominated_dispatch_mw = EXCLUDED.nominated_dispatch_mw,
          actual_dispatch_mw    = EXCLUDED.actual_dispatch_mw,
          nodal_price_excel     = EXCLUDED.nodal_price_excel,
          raw_nominated         = EXCLUDED.raw_nominated,
          raw_actual            = EXCLUDED.raw_actual,
          raw_nodal_price       = EXCLUDED.raw_nodal_price,
          raw_payload           = EXCLUDED.raw_payload
          -- created_at NOT updated: preserves original first-ingest timestamp
    """)
    count = 0
    for row in sr.rows:
        conn.execute(sql, {
            "asset_code": mr.asset_code,
            "interval_start": row.interval_start,
            "interval_end": row.interval_end,
            "data_date": row.data_date,
            "source_file_id": file_id,
            "sheet_name": row.sheet_name,
            "dispatch_unit_name": mr.dispatch_unit_name,
            "nominated_mw": row.nominated_dispatch_mw,
            "actual_mw": row.actual_dispatch_mw,
            "nodal_price": row.nodal_price_excel,
            "raw_nom": row.raw_nominated,
            "raw_act": row.raw_actual,
            "raw_price": row.raw_nodal_price,
            "raw_payload": json.dumps(row.raw_payload, ensure_ascii=False),
        })
        count += 1
    return count


def _update_registry_status(
    conn,
    file_id: int,
    status: str,
    row_count: int,
    notes: Optional[str] = None,
) -> None:
    """
    Update parse_status and row_count on the registry row.

    If *notes* is given it is written to the notes column; if None, the
    existing notes value is preserved (useful for force-reprocess where
    _reset_registry_pending already wrote 'force-reprocessed').
    """
    sql = text("""
        UPDATE marketdata.ops_dispatch_file_registry
        SET parse_status = :status,
            row_count    = :rc,
            notes        = CASE WHEN :notes IS NOT NULL THEN :notes ELSE notes END
        WHERE id = :fid
    """)
    conn.execute(sql, {"status": status, "rc": row_count, "notes": notes, "fid": file_id})


def _row_to_dict(row) -> dict:
    """Convert a ParsedRow to a plain dict for price verification."""
    return {
        "interval_start": row.interval_start,
        "nodal_price_excel": row.nodal_price_excel,
    }


def _find_ddl_path() -> Optional[str]:
    """Find ops_bess_dispatch.sql relative to common repo root patterns."""
    import os
    candidates = [
        # From this file: services/ops_ingestion/inner_mongolia/writer.py
        # → ../../.. → project root → db/ddl/marketdata/
        os.path.join(os.path.dirname(__file__), '..', '..', '..', 'db', 'ddl', 'marketdata', 'ops_bess_dispatch.sql'),
    ]
    for p in candidates:
        normalised = os.path.normpath(p)
        if os.path.isfile(normalised):
            return normalised
    return None
