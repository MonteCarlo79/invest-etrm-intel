# tests/citic_futures/test_digest_job.py
"""Tests for the ECS-side CITIC digest job plumbing (no LLM calls)."""
from unittest.mock import MagicMock, patch

from services.citic_futures import digest_job as dj


def _conn_mock(fetch_results):
    conn = MagicMock()
    cur = MagicMock()
    cur.fetchall.side_effect = fetch_results
    cur.__enter__ = lambda s: s
    cur.__exit__ = MagicMock(return_value=False)
    conn.cursor.return_value = cur
    conn.__enter__ = lambda s: s
    conn.__exit__ = MagicMock(return_value=False)
    return conn


def test_pending_folders_excludes_digested(monkeypatch):
    conn = _conn_mock([
        [("中信期货周报20260914", [1, 2, 3])],   # _PENDING_SQL result
    ])
    monkeypatch.setattr("psycopg2.connect", lambda *a, **k: conn)
    monkeypatch.setattr(dj, "ensure_tables", lambda pg: None)
    out = dj._pending_folders("postgresql://x")
    assert out == [("中信期货周报20260914", [1, 2, 3])]


def test_pending_folders_parses_jsonb_string(monkeypatch):
    conn = _conn_mock([
        [("中信期货周报20260914", "[1, 2]")],
    ])
    monkeypatch.setattr("psycopg2.connect", lambda *a, **k: conn)
    monkeypatch.setattr(dj, "ensure_tables", lambda pg: None)
    out = dj._pending_folders("postgresql://x")
    assert out == [("中信期货周报20260914", [1, 2])]


def test_collect_docs_regroups(monkeypatch):
    conn = _conn_mock([
        [(1, "【中信期货能源化工（原油）】a.pdf"), (2, "【中信期货黑色建材】b.pdf")],
    ])
    monkeypatch.setattr("psycopg2.connect", lambda *a, **k: conn)
    out = dj._collect_docs("postgresql://x", [1, 2])
    assert out == [(1, "【中信期货能源化工（原油）】a.pdf", "energy_chain")]


def test_run_citic_digest_logs_empty_folder(monkeypatch):
    monkeypatch.setattr(dj, "ensure_tables", lambda pg: None)
    monkeypatch.setattr(dj, "_pending_folders", lambda pg: [("中信期货周报20260914", [9])])
    monkeypatch.setattr(dj, "_collect_docs", lambda pg, ids: [])
    logged = []
    monkeypatch.setattr(dj, "_log_folder", lambda pg, f, n: logged.append((f, n)))
    out = dj.run_citic_digest("postgresql://x", feishu=None, owner_open_id="")
    assert out["sent"] == []
    assert logged == [("中信期货周报20260914", 0)]


def test_run_citic_digest_sends_and_logs(monkeypatch):
    monkeypatch.setattr(dj, "ensure_tables", lambda pg: None)
    monkeypatch.setattr(dj, "_pending_folders", lambda pg: [("中信期货周报20260914", [1])])
    monkeypatch.setattr(dj, "_collect_docs", lambda pg, ids: [(1, "原油.pdf", "energy_chain")])
    logged = []
    monkeypatch.setattr(dj, "_log_folder", lambda pg, f, n: logged.append((f, n)))
    with patch("services.citic_futures.weekly_digest.collect_relevant_text",
               return_value={"energy_chain": [("原油.pdf", "供应紧张")]}), \
         patch("services.citic_futures.weekly_digest.send_weekly_digest",
               return_value="摘要") as mock_send:
        out = dj.run_citic_digest("postgresql://x", feishu=None, owner_open_id="")
    assert out["sent"] == ["中信期货周报20260914"]
    assert logged == [("中信期货周报20260914", 1)]
    assert mock_send.call_args.args[1] == ""  # owner_open_id passthrough
