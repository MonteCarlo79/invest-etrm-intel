"""Unit tests for the two-phase CJK anchor search (no DB needed).

2026-09-07 iteration 3: OR-ing all bigrams seq-scans 2.8M chunks (~130-530s);
counting all bigrams costs ~60-90s cold; "rare" market bigrams still match
20-40K rows. The anchor AND (first 2 bigrams of the longest CJK run) intersects
to a few hundred rows via GIN. Ladder: anchor-AND → rare-union → legacy scan.
"""
from unittest.mock import MagicMock, patch

import services.knowledge_pool.knowledge_docs as kd


class TestAnchorBigrams:
    def test_longest_run_first_four(self):
        assert kd._cjk_anchor_bigrams("蒙西电力现货市场规则") == ["蒙西", "西电", "电力", "力现"]

    def test_picks_longest_run_with_latin_mix(self):
        assert kd._cjk_anchor_bigrams("蒙西 BESS 容量电价") == ["容量", "量电", "电价"]

    def test_caps_at_n(self):
        assert kd._cjk_anchor_bigrams("蒙西电力现货", n=2) == ["蒙西", "西电"]

    def test_empty_when_no_cjk_run(self):
        assert kd._cjk_anchor_bigrams("BESS 2026") == []

    def test_skips_single_char_runs(self):
        assert kd._cjk_anchor_bigrams("电 BESS") == []


class TestPickRareBigrams:
    def test_selective_only_two_rarest(self):
        rare, est = kd._pick_rare_bigrams(
            ["蒙西", "规则", "现货"], [344, 8880, 18525])
        assert rare == ["蒙西", "规则"]
        assert est == 344 + 8880

    def test_skips_common_and_zero(self):
        rare, est = kd._pick_rare_bigrams(["电力", "蒙西", "甲某"], [46472, 344, 0])
        assert rare == ["蒙西"]
        assert est == 344

    def test_all_common_falls_back_to_two_smallest(self):
        rare, est = kd._pick_rare_bigrams(
            ["电力", "市场", "结算"], [46472, 57512, 26246])
        assert rare == ["结算", "电力"]
        assert est == 26246 + 46472


class TestBigramCountCache:
    def setup_method(self):
        kd._count_cache.clear()

    def test_caches_within_ttl(self):
        cur = MagicMock()
        cur.fetchone.return_value = (344,)
        assert kd._bigram_count(cur, "蒙西") == 344
        assert kd._bigram_count(cur, "蒙西") == 344
        assert cur.execute.call_count == 1


class _FakeCursor:
    """Captures execute() calls; count queries return scripted counts; candidate
    searches return scripted rows per call."""

    def __init__(self, count_map, search_row_sets):
        self.count_map = count_map
        self.search_row_sets = list(search_row_sets)
        self.calls = []

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, sql, params=None):
        self.calls.append((sql, list(params or [])))
        self._current = sql

    def fetchone(self):
        bg = self.calls[-1][1][0].strip("%")
        return (self.count_map.get(bg, 100),)

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
    def setup_method(self):
        kd._count_cache.clear()

    def test_anchor_and_short_circuits(self):
        cur = _FakeCursor(count_map={}, search_row_sets=[_ROWS3])
        with patch.object(kd, "get_conn", return_value=_fake_conn(cur)):
            rows = kd._cjk_two_phase_search(
                _QUERY, _BIGRAMS, category=None, app="strategist",
                filename_contains=None, limit=5)
        assert rows is not None and len(rows) == 3
        # exactly ONE query (the AND search) — no count queries
        assert len(cur.calls) == 1
        sql, params = cur.calls[0]
        assert "ILIKE %s AND c.chunk_text ILIKE %s" in sql
        # CASE over all bigrams, then the 2 anchor params, then app, then limit
        n_bg = len(_BIGRAMS)
        assert params[:n_bg] == [f"%{b}%" for b in _BIGRAMS]
        assert params[n_bg:n_bg + 2] == ["%蒙西%", "%西电%"]
        assert params[-2] == "strategist"
        assert params[-1] == 5
        assert sql.count("%s") == len(params)

    def test_falls_to_rare_union_when_anchor_thin(self):
        cur = _FakeCursor(
            count_map={"蒙西": 344, "西电": 350, "电力": 46472, "力现": 900},
            search_row_sets=[[(1, "f.pdf", "c", "shared", 1, "t", 5.0)],  # thin AND
                             _ROWS3],                                     # union ok
        )
        with patch.object(kd, "get_conn", return_value=_fake_conn(cur)):
            rows = kd._cjk_two_phase_search(
                _QUERY, _BIGRAMS, category=None, app=None,
                filename_contains=None, limit=5)
        assert rows is not None and len(rows) == 3
        # AND search + 4 counts + union search
        assert len(cur.calls) == 1 + 4 + 1
        union_sql, union_params = cur.calls[-1]
        assert " OR " in union_sql
        # rarest two of the anchor: 蒙西(344), 西电(350)
        n_bg = len(_BIGRAMS)
        assert union_params[n_bg:n_bg + 2] == ["%蒙西%", "%西电%"]

    def test_returns_none_when_all_thin(self):
        cur = _FakeCursor(
            count_map={"蒙西": 344},
            search_row_sets=[[(1, "f.pdf", "c", "shared", 1, "t", 5.0)],
                             [(1, "f.pdf", "c", "shared", 1, "t", 5.0)]],
        )
        with patch.object(kd, "get_conn", return_value=_fake_conn(cur)):
            rows = kd._cjk_two_phase_search(
                _QUERY, _BIGRAMS, category=None, app=None,
                filename_contains=None, limit=5)
        assert rows is None
