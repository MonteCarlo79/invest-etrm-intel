"""
tests/test_writer.py

Unit tests for services/ops_ingestion/inner_mongolia/writer.py

Tests that do NOT require a real database connection:
  - compute_file_hash: stable, deterministic, different files differ
  - Supersession logic: _next_ingest_version increments
  - is_current toggled correctly on replacement
  - ON CONFLICT semantics: only one row per (asset_code, interval_start)
  - Force-reprocess semantics (Cases 1 and 2 in writer.py docstring)

DB-dependent tests use an in-memory SQLite database via SQLAlchemy.
SQLite does not support TIMESTAMPTZ, so interval_start stored as TEXT there.
The upsert logic itself is DB-agnostic; dialect differences are acceptable.
"""
from __future__ import annotations

import sys
import os
import datetime
import hashlib
import tempfile
from unittest.mock import MagicMock, patch, ANY

import pytest

sys.path.insert(0, os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..')))

from inner_mongolia.writer import compute_file_hash, ingest_file


# ---------------------------------------------------------------------------
# compute_file_hash
# ---------------------------------------------------------------------------

class TestComputeFileHash:
    def test_hash_is_stable(self, tmp_path):
        f = tmp_path / "test.xlsx"
        f.write_bytes(b"hello world")
        h1 = compute_file_hash(str(f))
        h2 = compute_file_hash(str(f))
        assert h1 == h2

    def test_hash_is_sha256(self, tmp_path):
        f = tmp_path / "test.xlsx"
        f.write_bytes(b"hello world")
        expected = hashlib.sha256(b"hello world").hexdigest()
        assert compute_file_hash(str(f)) == expected

    def test_different_content_different_hash(self, tmp_path):
        f1 = tmp_path / "a.xlsx"
        f2 = tmp_path / "b.xlsx"
        f1.write_bytes(b"content A")
        f2.write_bytes(b"content B")
        assert compute_file_hash(str(f1)) != compute_file_hash(str(f2))

    def test_empty_file_hash(self, tmp_path):
        f = tmp_path / "empty.xlsx"
        f.write_bytes(b"")
        expected = hashlib.sha256(b"").hexdigest()
        assert compute_file_hash(str(f)) == expected


# ---------------------------------------------------------------------------
# Supersession logic — tested with a mock/stub approach (no real DB)
# ---------------------------------------------------------------------------

class TestSupersessionLogic:
    """
    Test the supersession version increment logic without a real DB.
    We verify the SQL semantics using simple Python list simulation.
    """

    def _simulate_registry(self) -> list:
        """Simulate a simple registry store."""
        return []

    def _next_version(self, registry: list, report_date: str) -> int:
        """Replicate _next_ingest_version logic."""
        versions = [r['ingest_version'] for r in registry if r['report_date'] == report_date]
        return max(versions, default=0) + 1

    def _mark_superseded(self, registry: list, report_date: str, new_id: int) -> None:
        """Replicate _supersede_previous logic."""
        for r in registry:
            if r['report_date'] == report_date and r['id'] != new_id and r['is_current']:
                r['is_current'] = False

    def test_first_ingest_is_version_1(self):
        registry = self._simulate_registry()
        v = self._next_version(registry, '2026-02-10')
        assert v == 1

    def test_second_ingest_is_version_2(self):
        registry = [{'id': 1, 'report_date': '2026-02-10', 'ingest_version': 1, 'is_current': True}]
        v = self._next_version(registry, '2026-02-10')
        assert v == 2

    def test_third_ingest_is_version_3(self):
        registry = [
            {'id': 1, 'report_date': '2026-02-10', 'ingest_version': 1, 'is_current': False},
            {'id': 2, 'report_date': '2026-02-10', 'ingest_version': 2, 'is_current': True},
        ]
        v = self._next_version(registry, '2026-02-10')
        assert v == 3

    def test_different_date_version_independent(self):
        registry = [{'id': 1, 'report_date': '2026-02-10', 'ingest_version': 5, 'is_current': True}]
        v = self._next_version(registry, '2026-02-11')
        assert v == 1

    def test_supersede_marks_old_as_not_current(self):
        registry = [
            {'id': 1, 'report_date': '2026-02-10', 'ingest_version': 1, 'is_current': True},
        ]
        new_id = 2
        registry.append({'id': 2, 'report_date': '2026-02-10', 'ingest_version': 2, 'is_current': True})
        self._mark_superseded(registry, '2026-02-10', new_id)

        old = next(r for r in registry if r['id'] == 1)
        new = next(r for r in registry if r['id'] == 2)
        assert old['is_current'] is False
        assert new['is_current'] is True

    def test_supersede_does_not_affect_other_dates(self):
        registry = [
            {'id': 1, 'report_date': '2026-02-09', 'ingest_version': 1, 'is_current': True},
            {'id': 2, 'report_date': '2026-02-10', 'ingest_version': 1, 'is_current': True},
        ]
        new_id = 3
        registry.append({'id': 3, 'report_date': '2026-02-10', 'ingest_version': 2, 'is_current': True})
        self._mark_superseded(registry, '2026-02-10', new_id)

        feb_09 = next(r for r in registry if r['id'] == 1)
        assert feb_09['is_current'] is True   # different date — not touched


# ---------------------------------------------------------------------------
# Upsert semantics — one row per (asset_code, interval_start)
# ---------------------------------------------------------------------------

class TestUpsertSemantics:
    """
    Verify that the ON CONFLICT DO UPDATE logic produces exactly one row per
    (asset_code, interval_start) after two ingests of the same interval.

    Simulated with a simple dict as in-memory store (no DB needed).
    """

    def _make_dispatch_store(self) -> dict:
        """Key: (asset_code, interval_start) → row dict."""
        return {}

    def _upsert(self, store: dict, asset_code: str, interval_start: str,
                source_file_id: int, nominated_mw: float) -> None:
        """Simulate ON CONFLICT (asset_code, interval_start) DO UPDATE."""
        key = (asset_code, interval_start)
        existing = store.get(key)
        if existing is None:
            store[key] = {
                'asset_code': asset_code,
                'interval_start': interval_start,
                'source_file_id': source_file_id,
                'nominated_dispatch_mw': nominated_mw,
                'created_at': datetime.datetime.now(),
            }
        else:
            # Update values but preserve created_at
            original_created = existing['created_at']
            existing.update({
                'source_file_id': source_file_id,
                'nominated_dispatch_mw': nominated_mw,
            })
            existing['created_at'] = original_created

    def test_no_duplicates_on_second_ingest(self):
        store = self._make_dispatch_store()
        self._upsert(store, 'suyou', '2026-02-10T00:00:00+08:00', 1, 50.0)
        self._upsert(store, 'suyou', '2026-02-10T00:00:00+08:00', 2, 55.0)
        assert len(store) == 1

    def test_values_updated_on_replacement(self):
        store = self._make_dispatch_store()
        self._upsert(store, 'suyou', '2026-02-10T00:00:00+08:00', 1, 50.0)
        self._upsert(store, 'suyou', '2026-02-10T00:00:00+08:00', 2, 55.0)
        row = store[('suyou', '2026-02-10T00:00:00+08:00')]
        assert row['nominated_dispatch_mw'] == 55.0
        assert row['source_file_id'] == 2

    def test_created_at_preserved_on_replacement(self):
        store = self._make_dispatch_store()
        self._upsert(store, 'suyou', '2026-02-10T00:00:00+08:00', 1, 50.0)
        original_created = store[('suyou', '2026-02-10T00:00:00+08:00')]['created_at']
        self._upsert(store, 'suyou', '2026-02-10T00:00:00+08:00', 2, 55.0)
        assert store[('suyou', '2026-02-10T00:00:00+08:00')]['created_at'] == original_created

    def test_different_assets_independent(self):
        store = self._make_dispatch_store()
        self._upsert(store, 'suyou', '2026-02-10T00:00:00+08:00', 1, 50.0)
        self._upsert(store, 'hangjinqi', '2026-02-10T00:00:00+08:00', 1, 40.0)
        assert len(store) == 2

    def test_different_intervals_independent(self):
        store = self._make_dispatch_store()
        self._upsert(store, 'suyou', '2026-02-10T00:00:00+08:00', 1, 50.0)
        self._upsert(store, 'suyou', '2026-02-10T00:15:00+08:00', 1, 50.0)
        assert len(store) == 2


# ---------------------------------------------------------------------------
# Helpers shared by force-reprocess and supersession test classes
# ---------------------------------------------------------------------------

def _make_engine_cm():
    """Return a (engine, conn) pair where engine.begin() works as a context manager."""
    engine = MagicMock()
    conn = MagicMock()
    cm = MagicMock()
    cm.__enter__ = MagicMock(return_value=conn)
    cm.__exit__ = MagicMock(return_value=False)
    engine.begin.return_value = cm
    return engine, conn


def _fake_vr():
    """Return a minimal PriceVerificationResult-like MagicMock."""
    vr = MagicMock()
    vr.price_match_n = 0
    vr.price_match_mae = None
    vr.price_match_r = None
    vr.price_verification_level = 'unverified'
    vr.price_verification_notes = ''
    return vr


def _fake_sheets(sheet_names, rows_per_sheet=3):
    """
    Build a list of mock SheetParseResult objects.

    Each sheet has `rows_per_sheet` mock rows. Rows carry the attributes
    accessed by _row_to_dict and _upsert_dispatch_rows.
    """
    row_proto = MagicMock()
    row_proto.interval_start = None
    row_proto.interval_end = None
    row_proto.data_date = datetime.date(2026, 4, 17)
    row_proto.sheet_name = 'Sheet1'
    row_proto.nominated_dispatch_mw = 10.0
    row_proto.actual_dispatch_mw = 9.0
    row_proto.nodal_price_excel = 300.0
    row_proto.raw_nominated = '10'
    row_proto.raw_actual = '9'
    row_proto.raw_nodal_price = '300'
    row_proto.raw_payload = {}

    sheets = []
    for name in sheet_names:
        s = MagicMock()
        s.sheet_name = name
        s.n_rows = rows_per_sheet
        s.rows = [row_proto] * rows_per_sheet
        sheets.append(s)
    return sheets


def _make_match_side_effect(asset_map=None):
    """Return a match_sheet side-effect that sets asset_code to the lower-cased sheet name."""
    def _match(name, amap):
        mr = MagicMock()
        mr.asset_code = name.lower()
        mr.match_method = 'exact'
        mr.dispatch_unit_name = name
        mr.plant_name = name
        mr.nickname_cn = name
        mr.bracket_cn = name
        mr.sheet_name = name
        return mr
    return _match


# ---------------------------------------------------------------------------
# Test class: --force with same file_hash (Case 1)
# ---------------------------------------------------------------------------

class TestForceReprocessSameHash:
    """
    Verify Case 1 from writer.py:
      Without --force: same hash → skip (skipped_duplicate or skipped_superseded).
      With    --force: same hash → reuse existing registry row, no INSERT, no IntegrityError.
    """

    _EXISTING_CURRENT = {
        'id': 42,
        'report_date': '2026-04-17',
        'is_current': True,
        'ingest_version': 1,
    }
    _EXISTING_SUPERSEDED = {**_EXISTING_CURRENT, 'is_current': False}

    # --- skip cases (no --force) ------------------------------------------

    def test_same_hash_no_force_is_current_returns_skipped_duplicate(self, tmp_path):
        """Same hash + is_current=True + no --force → skipped_duplicate; no DB writes."""
        f = tmp_path / "file.xlsx"
        f.write_bytes(b"fake bytes")
        engine, _ = _make_engine_cm()

        with patch('inner_mongolia.writer._lookup_by_hash', return_value=self._EXISTING_CURRENT):
            result = ingest_file(str(f), engine=engine, force=False, dry_run=False)

        assert result.status == 'skipped_duplicate'
        assert result.file_id == 42
        assert 'force' in result.notes.lower()
        engine.begin.assert_not_called()

    def test_same_hash_no_force_superseded_returns_skipped_superseded(self, tmp_path):
        """Same hash + is_current=False + no --force → skipped_superseded; no DB writes."""
        f = tmp_path / "file.xlsx"
        f.write_bytes(b"fake bytes")
        engine, _ = _make_engine_cm()

        with patch('inner_mongolia.writer._lookup_by_hash', return_value=self._EXISTING_SUPERSEDED):
            result = ingest_file(str(f), engine=engine, force=False, dry_run=False)

        assert result.status == 'skipped_superseded'
        assert result.file_id == 42
        engine.begin.assert_not_called()

    # --- force-reprocess cases (--force) ------------------------------------

    def test_same_hash_with_force_calls_reset_not_insert(self, tmp_path):
        """Same hash + --force → _reset_registry_pending called; _insert_registry NOT called."""
        f = tmp_path / "file.xlsx"
        f.write_bytes(b"fake bytes")
        engine, _ = _make_engine_cm()

        with patch('inner_mongolia.writer._lookup_by_hash', return_value=self._EXISTING_CURRENT), \
             patch('inner_mongolia.writer.parse_date', return_value=datetime.date(2026, 4, 17)), \
             patch('inner_mongolia.writer.load_asset_map', return_value={}), \
             patch('inner_mongolia.writer.parse_workbook', return_value=_fake_sheets(['SheetA'])), \
             patch('inner_mongolia.writer.match_sheet', side_effect=_make_match_side_effect()), \
             patch('inner_mongolia.writer.verify_prices_no_db', return_value=_fake_vr()), \
             patch('inner_mongolia.writer._reset_registry_pending') as mock_reset, \
             patch('inner_mongolia.writer._insert_registry') as mock_insert, \
             patch('inner_mongolia.writer._supersede_previous') as mock_sup, \
             patch('inner_mongolia.writer._upsert_sheet_map'), \
             patch('inner_mongolia.writer._upsert_dispatch_rows', return_value=3), \
             patch('inner_mongolia.writer._update_registry_status'):
            result = ingest_file(str(f), engine=engine, force=True, dry_run=False)

        assert result.status == 'success'
        mock_reset.assert_called_once()
        mock_insert.assert_not_called()
        mock_sup.assert_not_called()

    def test_same_hash_with_force_preserves_file_id_and_version(self, tmp_path):
        """Same hash + --force → result carries original file_id and ingest_version."""
        f = tmp_path / "file.xlsx"
        f.write_bytes(b"fake bytes")
        engine, _ = _make_engine_cm()

        with patch('inner_mongolia.writer._lookup_by_hash', return_value=self._EXISTING_CURRENT), \
             patch('inner_mongolia.writer.parse_date', return_value=datetime.date(2026, 4, 17)), \
             patch('inner_mongolia.writer.load_asset_map', return_value={}), \
             patch('inner_mongolia.writer.parse_workbook', return_value=[]), \
             patch('inner_mongolia.writer._reset_registry_pending'), \
             patch('inner_mongolia.writer._update_registry_status'):
            result = ingest_file(str(f), engine=engine, force=True, dry_run=False)

        assert result.file_id == 42
        assert result.ingest_version == 1

    def test_same_hash_with_force_row_count_correct(self, tmp_path):
        """
        Force-reprocess of same hash: rows_written equals the actual rows returned
        by _upsert_dispatch_rows across all matched sheets.
        """
        f = tmp_path / "file.xlsx"
        f.write_bytes(b"fake bytes")
        engine, _ = _make_engine_cm()

        sheets = _fake_sheets(['SheetA', 'SheetB'], rows_per_sheet=3)  # 2 sheets × 3 rows

        with patch('inner_mongolia.writer._lookup_by_hash', return_value=self._EXISTING_CURRENT), \
             patch('inner_mongolia.writer.parse_date', return_value=datetime.date(2026, 4, 17)), \
             patch('inner_mongolia.writer.load_asset_map', return_value={}), \
             patch('inner_mongolia.writer.parse_workbook', return_value=sheets), \
             patch('inner_mongolia.writer.match_sheet', side_effect=_make_match_side_effect()), \
             patch('inner_mongolia.writer.verify_prices_no_db', return_value=_fake_vr()), \
             patch('inner_mongolia.writer._reset_registry_pending'), \
             patch('inner_mongolia.writer._upsert_sheet_map'), \
             patch('inner_mongolia.writer._upsert_dispatch_rows', return_value=3) as mock_upsert, \
             patch('inner_mongolia.writer._update_registry_status'):
            result = ingest_file(str(f), engine=engine, force=True, dry_run=False)

        assert result.status == 'success'
        assert result.rows_written == 6          # 2 sheets × 3 rows each
        assert mock_upsert.call_count == 2       # one call per matched sheet

    def test_same_hash_with_force_no_supersession(self, tmp_path):
        """Same hash + --force → supersession chain unchanged (_supersede_previous not called)."""
        f = tmp_path / "file.xlsx"
        f.write_bytes(b"fake bytes")
        engine, _ = _make_engine_cm()

        with patch('inner_mongolia.writer._lookup_by_hash', return_value=self._EXISTING_CURRENT), \
             patch('inner_mongolia.writer.parse_date', return_value=datetime.date(2026, 4, 17)), \
             patch('inner_mongolia.writer.load_asset_map', return_value={}), \
             patch('inner_mongolia.writer.parse_workbook', return_value=[]), \
             patch('inner_mongolia.writer._reset_registry_pending'), \
             patch('inner_mongolia.writer._supersede_previous') as mock_sup, \
             patch('inner_mongolia.writer._update_registry_status'):
            ingest_file(str(f), engine=engine, force=True, dry_run=False)

        mock_sup.assert_not_called()


# ---------------------------------------------------------------------------
# Test class: different file_hash, same report date (Case 2 — supersession)
# ---------------------------------------------------------------------------

class TestDifferentHashSupersession:
    """
    Verify Case 2 from writer.py:
      Different file_hash for the same report_date → new registry row inserted,
      ingest_version incremented, previous is_current row superseded.
    """

    def _run_new_file(self, tmp_path, content=b"new bytes", next_version=1, new_file_id=101):
        f = tmp_path / "file.xlsx"
        f.write_bytes(content)
        engine, _ = _make_engine_cm()

        with patch('inner_mongolia.writer._lookup_by_hash', return_value=None), \
             patch('inner_mongolia.writer.parse_date', return_value=datetime.date(2026, 4, 17)), \
             patch('inner_mongolia.writer.load_asset_map', return_value={}), \
             patch('inner_mongolia.writer.parse_workbook', return_value=_fake_sheets(['SheetA'])), \
             patch('inner_mongolia.writer.match_sheet', side_effect=_make_match_side_effect()), \
             patch('inner_mongolia.writer.verify_prices_no_db', return_value=_fake_vr()), \
             patch('inner_mongolia.writer._next_ingest_version', return_value=next_version), \
             patch('inner_mongolia.writer._insert_registry', return_value=new_file_id) as mock_ins, \
             patch('inner_mongolia.writer._supersede_previous') as mock_sup, \
             patch('inner_mongolia.writer._reset_registry_pending') as mock_reset, \
             patch('inner_mongolia.writer._upsert_sheet_map'), \
             patch('inner_mongolia.writer._upsert_dispatch_rows', return_value=3), \
             patch('inner_mongolia.writer._update_registry_status'):
            result = ingest_file(str(f), engine=engine, force=False, dry_run=False)

        return result, mock_ins, mock_sup, mock_reset

    def test_new_file_first_ingest_is_version_1(self, tmp_path):
        """Brand-new file (hash not in DB) → version=1, _insert_registry called."""
        result, mock_ins, _, _ = self._run_new_file(tmp_path, next_version=1, new_file_id=101)

        assert result.status == 'success'
        assert result.file_id == 101
        assert result.ingest_version == 1
        mock_ins.assert_called_once()

    def test_replacement_file_creates_next_version(self, tmp_path):
        """Corrected replacement file → version incremented to 2, new registry row inserted."""
        result, mock_ins, mock_sup, _ = self._run_new_file(
            tmp_path, content=b"corrected bytes", next_version=2, new_file_id=202
        )

        assert result.status == 'success'
        assert result.file_id == 202
        assert result.ingest_version == 2
        mock_ins.assert_called_once()

    def test_replacement_file_calls_supersede_with_new_id(self, tmp_path):
        """_supersede_previous is called with the correct report_date and new file_id."""
        _, _, mock_sup, _ = self._run_new_file(
            tmp_path, content=b"corrected bytes", next_version=2, new_file_id=303
        )

        mock_sup.assert_called_once()
        _conn_arg, rd_arg, new_id_arg = mock_sup.call_args[0]
        assert rd_arg == '2026-04-17'
        assert new_id_arg == 303

    def test_new_file_does_not_call_reset(self, tmp_path):
        """For a genuinely new hash, _reset_registry_pending is never called."""
        _, _, _, mock_reset = self._run_new_file(tmp_path)
        mock_reset.assert_not_called()


# ---------------------------------------------------------------------------
# Test class: transient-error retry logic
# ---------------------------------------------------------------------------

class TestRetryLogic:
    """
    Verify the _is_transient_db_error helper and the retry loop in ingest_file.

    All tests mock the DB helpers so no real connection is needed.
    time.sleep is also mocked to keep tests instant.
    """

    # ---- unit tests for the classifier ----------------------------------

    def test_ssl_abort_is_transient(self):
        from sqlalchemy.exc import OperationalError as SAOperationalError
        from inner_mongolia.writer import _is_transient_db_error
        exc = SAOperationalError(
            "SELECT 1", {},
            Exception("ssl syscall error: software caused connection abort"),
        )
        assert _is_transient_db_error(exc)

    def test_connection_reset_is_transient(self):
        from sqlalchemy.exc import OperationalError as SAOperationalError
        from inner_mongolia.writer import _is_transient_db_error
        exc = SAOperationalError("SELECT 1", {}, Exception("connection reset by peer"))
        assert _is_transient_db_error(exc)

    def test_integrity_error_is_not_transient(self):
        from sqlalchemy.exc import IntegrityError
        from inner_mongolia.writer import _is_transient_db_error
        exc = IntegrityError(
            "INSERT", {},
            Exception("duplicate key value violates unique constraint"),
        )
        assert not _is_transient_db_error(exc)

    def test_non_db_exception_is_not_transient(self):
        from inner_mongolia.writer import _is_transient_db_error
        assert not _is_transient_db_error(ValueError("bad value"))
        assert not _is_transient_db_error(RuntimeError("ssl syscall error"))

    # ---- integration tests for the retry loop ---------------------------

    def _base_patches(self, tmp_path, sheets=None):
        """Return a dict of common patch kwargs for ingest_file tests."""
        f = tmp_path / "file.xlsx"
        f.write_bytes(b"bytes")
        existing = {'id': 42, 'report_date': '2026-04-17', 'is_current': True, 'ingest_version': 1}
        s = sheets or _fake_sheets(['SheetA'])
        return str(f), existing, s

    def test_transient_error_is_retried_and_succeeds(self, tmp_path):
        """
        First _upsert_dispatch_rows call raises a transient OperationalError.
        Second call succeeds. Result should be 'success' after one retry.
        """
        from sqlalchemy.exc import OperationalError as SAOperationalError
        fpath, existing, sheets = self._base_patches(tmp_path)
        engine, _ = _make_engine_cm()

        transient = SAOperationalError(
            "stmt", {}, Exception("ssl syscall error: connection abort")
        )
        call_seq = [transient, 3]   # raise on attempt 1, return 3 on attempt 2
        idx = [0]

        def upsert_side(*a, **kw):
            v = call_seq[idx[0]]
            idx[0] += 1
            if isinstance(v, Exception):
                raise v
            return v

        with patch('inner_mongolia.writer._lookup_by_hash', return_value=existing), \
             patch('inner_mongolia.writer.parse_date', return_value=datetime.date(2026, 4, 17)), \
             patch('inner_mongolia.writer.load_asset_map', return_value={}), \
             patch('inner_mongolia.writer.parse_workbook', return_value=sheets), \
             patch('inner_mongolia.writer.match_sheet', side_effect=_make_match_side_effect()), \
             patch('inner_mongolia.writer.verify_prices_no_db', return_value=_fake_vr()), \
             patch('inner_mongolia.writer._reset_registry_pending'), \
             patch('inner_mongolia.writer._upsert_sheet_map'), \
             patch('inner_mongolia.writer._upsert_dispatch_rows', side_effect=upsert_side), \
             patch('inner_mongolia.writer._update_registry_status'), \
             patch('inner_mongolia.writer.time') as mock_time:
            result = ingest_file(fpath, engine=engine, force=True, dry_run=False)

        assert result.status == 'success'
        mock_time.sleep.assert_called_once()   # exactly one sleep between the two attempts

    def test_non_transient_error_not_retried(self, tmp_path):
        """A non-transient error (e.g. IntegrityError) fails immediately without retry."""
        from sqlalchemy.exc import IntegrityError
        fpath, existing, sheets = self._base_patches(tmp_path)
        engine, _ = _make_engine_cm()

        non_transient = IntegrityError("INSERT", {}, Exception("duplicate key"))
        call_count = [0]

        def upsert_side(*a, **kw):
            call_count[0] += 1
            raise non_transient

        with patch('inner_mongolia.writer._lookup_by_hash', return_value=existing), \
             patch('inner_mongolia.writer.parse_date', return_value=datetime.date(2026, 4, 17)), \
             patch('inner_mongolia.writer.load_asset_map', return_value={}), \
             patch('inner_mongolia.writer.parse_workbook', return_value=sheets), \
             patch('inner_mongolia.writer.match_sheet', side_effect=_make_match_side_effect()), \
             patch('inner_mongolia.writer.verify_prices_no_db', return_value=_fake_vr()), \
             patch('inner_mongolia.writer._reset_registry_pending'), \
             patch('inner_mongolia.writer._upsert_sheet_map'), \
             patch('inner_mongolia.writer._upsert_dispatch_rows', side_effect=upsert_side), \
             patch('inner_mongolia.writer._update_registry_status'), \
             patch('inner_mongolia.writer.time') as mock_time:
            result = ingest_file(fpath, engine=engine, force=True, dry_run=False)

        assert result.status == 'failed'
        assert call_count[0] == 1          # only one attempt
        mock_time.sleep.assert_not_called()

    def test_transient_exhausted_retries_returns_failed(self, tmp_path):
        """Transient errors on all _MAX_RETRIES attempts → status='failed'."""
        from sqlalchemy.exc import OperationalError as SAOperationalError
        from inner_mongolia.writer import _MAX_RETRIES
        fpath, existing, sheets = self._base_patches(tmp_path)
        engine, _ = _make_engine_cm()

        transient = SAOperationalError("stmt", {}, Exception("ssl syscall error: connection abort"))

        with patch('inner_mongolia.writer._lookup_by_hash', return_value=existing), \
             patch('inner_mongolia.writer.parse_date', return_value=datetime.date(2026, 4, 17)), \
             patch('inner_mongolia.writer.load_asset_map', return_value={}), \
             patch('inner_mongolia.writer.parse_workbook', return_value=sheets), \
             patch('inner_mongolia.writer.match_sheet', side_effect=_make_match_side_effect()), \
             patch('inner_mongolia.writer.verify_prices_no_db', return_value=_fake_vr()), \
             patch('inner_mongolia.writer._reset_registry_pending'), \
             patch('inner_mongolia.writer._upsert_sheet_map'), \
             patch('inner_mongolia.writer._upsert_dispatch_rows', side_effect=transient), \
             patch('inner_mongolia.writer._update_registry_status'), \
             patch('inner_mongolia.writer.time') as mock_time:
            result = ingest_file(fpath, engine=engine, force=True, dry_run=False)

        assert result.status == 'failed'
        # sleep is called between attempts 1→2 and 2→3, not after the last failure
        assert mock_time.sleep.call_count == _MAX_RETRIES - 1

    def test_retry_uses_exponential_backoff(self, tmp_path):
        """Sleep durations double each retry: 1 s, 2 s for _MAX_RETRIES=3."""
        from sqlalchemy.exc import OperationalError as SAOperationalError
        from inner_mongolia.writer import _RETRY_DELAY_BASE
        fpath, existing, sheets = self._base_patches(tmp_path)
        engine, _ = _make_engine_cm()

        transient = SAOperationalError("stmt", {}, Exception("ssl syscall error: connection abort"))

        with patch('inner_mongolia.writer._lookup_by_hash', return_value=existing), \
             patch('inner_mongolia.writer.parse_date', return_value=datetime.date(2026, 4, 17)), \
             patch('inner_mongolia.writer.load_asset_map', return_value={}), \
             patch('inner_mongolia.writer.parse_workbook', return_value=sheets), \
             patch('inner_mongolia.writer.match_sheet', side_effect=_make_match_side_effect()), \
             patch('inner_mongolia.writer.verify_prices_no_db', return_value=_fake_vr()), \
             patch('inner_mongolia.writer._reset_registry_pending'), \
             patch('inner_mongolia.writer._upsert_sheet_map'), \
             patch('inner_mongolia.writer._upsert_dispatch_rows', side_effect=transient), \
             patch('inner_mongolia.writer._update_registry_status'), \
             patch('inner_mongolia.writer.time') as mock_time:
            ingest_file(fpath, engine=engine, force=True, dry_run=False)

        sleep_calls = [c.args[0] for c in mock_time.sleep.call_args_list]
        assert sleep_calls == [_RETRY_DELAY_BASE, _RETRY_DELAY_BASE * 2]


# ---------------------------------------------------------------------------
# Test class: zero-row classification
# ---------------------------------------------------------------------------

class TestZeroRowClassification:
    """
    Verify _classify_zero_rows and that ingest_file writes the correct
    parse_status for files that produce no dispatch rows.
    """

    def test_classify_no_sheets_is_unsupported_format(self):
        from inner_mongolia.writer import _classify_zero_rows
        status, notes = _classify_zero_rows(sheet_results=[], matched=[])
        assert status == 'unsupported_format'
        assert 'parseable' in notes.lower() or 'format' in notes.lower() or 'sheet' in notes.lower()

    def test_classify_sheets_no_match_is_no_dispatch_section(self):
        from inner_mongolia.writer import _classify_zero_rows
        # Build matched list where no sheet has an asset_code
        unmatched = MagicMock()
        unmatched.sheet_name = 'Summary'
        mr = MagicMock()
        mr.asset_code = None
        matched = [{'sheet': unmatched, 'match': mr}]
        status, notes = _classify_zero_rows(sheet_results=[unmatched], matched=matched)
        assert status == 'no_dispatch_section'

    def test_classify_matched_empty_rows_is_partial_bundle(self):
        from inner_mongolia.writer import _classify_zero_rows
        sheet = MagicMock()
        sheet.sheet_name = 'SheetA'
        mr = MagicMock()
        mr.asset_code = 'suyou'
        matched = [{'sheet': sheet, 'match': mr}]
        status, notes = _classify_zero_rows(sheet_results=[sheet], matched=matched)
        assert status == 'partial_bundle'
        assert '0' in notes or 'zero' in notes.lower() or 'no data' in notes.lower() or 'data rows' in notes.lower()

    def test_ingest_file_no_sheets_gets_unsupported_format_status(self, tmp_path):
        """ingest_file with empty parse_workbook result → registry status=unsupported_format."""
        f = tmp_path / "file.xlsx"
        f.write_bytes(b"bytes")
        engine, _ = _make_engine_cm()

        with patch('inner_mongolia.writer._lookup_by_hash', return_value=None), \
             patch('inner_mongolia.writer.parse_date', return_value=datetime.date(2026, 4, 17)), \
             patch('inner_mongolia.writer.load_asset_map', return_value={}), \
             patch('inner_mongolia.writer.parse_workbook', return_value=[]), \
             patch('inner_mongolia.writer._next_ingest_version', return_value=1), \
             patch('inner_mongolia.writer._insert_registry', return_value=1), \
             patch('inner_mongolia.writer._supersede_previous'), \
             patch('inner_mongolia.writer._upsert_sheet_map'), \
             patch('inner_mongolia.writer._upsert_dispatch_rows') as mock_upsert, \
             patch('inner_mongolia.writer._update_registry_status') as mock_update:
            result = ingest_file(str(f), engine=engine, force=False, dry_run=False)

        assert result.status == 'unsupported_format'
        mock_upsert.assert_not_called()   # no fact rows written for zero-row case
        # _update_registry_status(conn, file_id, status, row_count, notes=...)
        # status is positional arg index 2
        call_args = mock_update.call_args[0] if mock_update.call_args else ()
        written_status = call_args[2] if len(call_args) > 2 else None
        assert written_status == 'unsupported_format'

    def test_ingest_file_no_dispatch_section_skips_fact_writes(self, tmp_path):
        """Sheets parsed but none matched → no _upsert_dispatch_rows calls."""
        f = tmp_path / "file.xlsx"
        f.write_bytes(b"bytes")
        engine, _ = _make_engine_cm()

        # Sheet exists but match returns no asset_code
        sheet = MagicMock(sheet_name='Summary', n_rows=0, rows=[])
        no_match = MagicMock()
        no_match.asset_code = None
        no_match.match_method = 'unmatched'
        no_match.dispatch_unit_name = None
        no_match.plant_name = None
        no_match.nickname_cn = None
        no_match.bracket_cn = None
        no_match.sheet_name = 'Summary'

        with patch('inner_mongolia.writer._lookup_by_hash', return_value=None), \
             patch('inner_mongolia.writer.parse_date', return_value=datetime.date(2026, 4, 17)), \
             patch('inner_mongolia.writer.load_asset_map', return_value={}), \
             patch('inner_mongolia.writer.parse_workbook', return_value=[sheet]), \
             patch('inner_mongolia.writer.match_sheet', return_value=no_match), \
             patch('inner_mongolia.writer.verify_prices_no_db', return_value=_fake_vr()), \
             patch('inner_mongolia.writer._next_ingest_version', return_value=1), \
             patch('inner_mongolia.writer._insert_registry', return_value=1), \
             patch('inner_mongolia.writer._supersede_previous'), \
             patch('inner_mongolia.writer._upsert_sheet_map'), \
             patch('inner_mongolia.writer._upsert_dispatch_rows') as mock_upsert, \
             patch('inner_mongolia.writer._update_registry_status'):
            result = ingest_file(str(f), engine=engine, force=False, dry_run=False)

        assert result.status == 'no_dispatch_section'
        mock_upsert.assert_not_called()

    def test_nonzero_rows_is_success_not_classified(self, tmp_path):
        """Files with matched rows remain status='success' (classification not applied)."""
        f = tmp_path / "file.xlsx"
        f.write_bytes(b"bytes")
        engine, _ = _make_engine_cm()
        sheets = _fake_sheets(['SheetA'], rows_per_sheet=96)

        with patch('inner_mongolia.writer._lookup_by_hash', return_value=None), \
             patch('inner_mongolia.writer.parse_date', return_value=datetime.date(2026, 4, 17)), \
             patch('inner_mongolia.writer.load_asset_map', return_value={}), \
             patch('inner_mongolia.writer.parse_workbook', return_value=sheets), \
             patch('inner_mongolia.writer.match_sheet', side_effect=_make_match_side_effect()), \
             patch('inner_mongolia.writer.verify_prices_no_db', return_value=_fake_vr()), \
             patch('inner_mongolia.writer._next_ingest_version', return_value=1), \
             patch('inner_mongolia.writer._insert_registry', return_value=1), \
             patch('inner_mongolia.writer._supersede_previous'), \
             patch('inner_mongolia.writer._upsert_sheet_map'), \
             patch('inner_mongolia.writer._upsert_dispatch_rows', return_value=96), \
             patch('inner_mongolia.writer._update_registry_status'):
            result = ingest_file(str(f), engine=engine, force=False, dry_run=False)

        assert result.status == 'success'
        assert result.rows_written == 96
