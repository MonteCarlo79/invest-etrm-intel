"""Unit tests for the single-term-GIN CJK anchor search (no DB needed).

2026-09-08 iteration 4: 2-term ANDs and count(*)-guided unions plan unreliably
(planner ILIKE estimates are useless — 24-191s in prod). New shape: one ILIKE
on the anchor's lead bigram (always GIN), re-check the rest on those rows.
Ladder: lead AND second → lead AND any-of-rest → legacy scan. No count queries.
"""
from unittest.mock import MagicMock, patch

import services.knowledge_pool.knowledge_docs as kd


class TestAnchorBigrams:
    def test_longest_run_first_four(self):
        assert kd._cjk_anchor_bigrams("蒙西电力现货市场规则") == ["蒙西", "西电", "电力", "力现"]

    def test_picks_longest_run_with_latin_mix(self):
        assert kd._cjk_anchor_bigrams("蒙西 BESS 容量电价") == ["容量", "量电", "电价"]

    def test_empty_when_no_cjk_run(self):
        assert kd._cjk_anchor_bigrams("BESS 2026") == []

    def test_skips_single_char_runs(self):
        assert kd._cjk_anchor_bigrams("电 BESS") == []


class _FakeCursor:
    """Captures execute() calls; each candidate search returns the next
    scripted row set (empty when exhausted)."""

    def __init__(self, search_row_sets):
        self.search_row_sets = list(search_row_sets)
        self.calls = []

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, sql, params=None):
        self.calls.append((sql, list(params or [])))

    def fetchall(self):
        return self.search_row_sets.pop(0) if self.search_row_sets else []

    @property
    def description(self):
        return [("doc_id",), ("file_name",), ("category",), ("app",),
                ("page_no",), ("chunk_text",), ("rank",)]


def _fake_conn(cur):
    conn = MagicMock()
    conn.cursor.return_value = cur
    cm = MagicMock()
    cm.__enter__.return_value = conn
    cm.__exit__.return_value = False
    return cm


_QUERY = "蒙西电力现货市场规则独立储能"
_BIGRAMS = kd._cjk_bigrams(_QUERY)
_ROWS3 = [(1, "f.pdf", "c", "shared", 1, "t1", 11.0),
          (2, "g.pdf", "c", "shared", 2, "t2", 11.0),
          (3, "h.pdf", "c", "shared", 3, "t3", 10.0)]


class TestTwoPhaseSearch:
    def test_step1_and_short_circuits(self):
        cur = _FakeCursor([_ROWS3])
        with patch.object(kd, "get_conn", return_value=_fake_conn(cur)):
            rows = kd._cjk_two_phase_search(
                _QUERY, _BIGRAMS, category=None, app="strategist",
                filename_contains=None, limit=5)
        assert rows is not None and len(rows) == 3
        # exactly ONE query; no count(*) anywhere
        assert len(cur.calls) == 1
        sql, params = cur.calls[0]
        assert "WITH cand AS" in sql and "count(*)" not in sql
        assert sql.count("%s") == len(params)
        n_bg = len(_BIGRAMS)
        # param order: lead anchor, CASE bigrams, AND-rest, app, limit
        assert params[0] == "%蒙西%"
        assert params[1:1 + n_bg] == [f"%{b}%" for b in _BIGRAMS]
        assert params[1 + n_bg] == "%西电%"
        assert params[-2] == "strategist"
        assert params[-1] == 5
        # step 1 is AND of the second anchor
        assert "c.chunk_text ILIKE %s\n" not in sql  # only one rest cond
        assert sql.count("c.chunk_text ILIKE %s") == n_bg + 1  # CASE + rest (CTE is unaliased)

    def test_step2_or_when_step1_thin(self):
        cur = _FakeCursor([[(1, "f.pdf", "c", "shared", 1, "t", 5.0)], _ROWS3])
        with patch.object(kd, "get_conn", return_value=_fake_conn(cur)):
            rows = kd._cjk_two_phase_search(
                _QUERY, _BIGRAMS, category=None, app=None,
                filename_contains=None, limit=5)
        assert rows is not None and len(rows) == 3
        assert len(cur.calls) == 2
        sql2, params2 = cur.calls[1]
        # step 2: OR across anchor[1:4]
        assert " OR ".join([]) == "" and " OR " in sql2
        n_bg = len(_BIGRAMS)
        assert params2[0] == "%蒙西%"
        assert params2[1 + n_bg: 1 + n_bg + 3] == ["%西电%", "%电力%", "%力现%"]

    def test_returns_none_when_all_thin(self):
        cur = _FakeCursor([[(1, "f.pdf", "c", "shared", 1, "t", 5.0)],
                           [(1, "f.pdf", "c", "shared", 1, "t", 5.0)]])
        with patch.object(kd, "get_conn", return_value=_fake_conn(cur)):
            rows = kd._cjk_two_phase_search(
                _QUERY, _BIGRAMS, category=None, app=None,
                filename_contains=None, limit=5)
        assert rows is None
        assert len(cur.calls) == 2

    def test_returns_none_when_anchor_too_short(self):
        cur = _FakeCursor([_ROWS3])
        with patch.object(kd, "get_conn", return_value=_fake_conn(cur)):
            rows = kd._cjk_two_phase_search(
                "BESS", _BIGRAMS, category=None, app=None,
                filename_contains=None, limit=5)
        assert rows is None
        assert len(cur.calls) == 0
