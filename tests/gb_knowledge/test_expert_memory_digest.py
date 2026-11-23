"""KB digest must distinguish "no insights" from "extraction failed" (no network/DB).

Regression: a transient API/parse failure returned [] and the doc was stamped
with a permanent '_processed_no_insights' placeholder, so it was never retried
(2026-11-23: a 5,400-char GB-margins article lost to this; it yields 6 insights).
"""
import sys
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from services.gb_knowledge import expert_memory as em

DOC = {"url": "https://x/doc", "title": "t", "source": "timera", "doc_type": "article",
       "published_date": None, "content": "body"}


class _Client:
    def __init__(self, text=None, exc=None):
        self._text, self._exc = text, exc
        self.messages = SimpleNamespace(create=self._create)

    def _create(self, **kw):
        if self._exc:
            raise self._exc
        return SimpleNamespace(content=[SimpleNamespace(text=self._text)])


def test_parse_fenced_json_with_trailing_commentary():
    raw = '```json\n{"insights": [{"insight": "a", "type": "risk", "confidence": "high"}]}\n```\n\n**Rationale:** ok'
    assert em._parse_insights_json(raw) == [{"insight": "a", "type": "risk", "confidence": "high"}]


def test_parse_unfenced_json_with_trailing_text():
    assert em._parse_insights_json('{"insights": []}\n\nNote: nothing durable.') == []


def test_parse_garbage_is_none_not_empty():
    assert em._parse_insights_json("sorry, I can't do that") is None
    assert em._parse_insights_json('{"insights": "nope"}') is None


def test_extract_genuine_empty_is_empty_list():
    assert em._extract_insights_from_doc(_Client('```json\n{"insights": []}\n```'), DOC) == []


def test_extract_api_error_is_none():
    assert em._extract_insights_from_doc(_Client(exc=RuntimeError("overloaded")), DOC) is None


def test_extract_unparseable_is_none():
    assert em._extract_insights_from_doc(_Client("not json"), DOC) is None


class _Cur:
    def __init__(self, conn): self.c = conn
    def __enter__(self): return self
    def __exit__(self, *a): return False
    def execute(self, sql, params=None): self.c.sql.append(sql)
    def fetchall(self): return [dict(DOC)]


class _Conn:
    def __init__(self): self.sql = []
    def cursor(self, **kw): return _Cur(self)
    def commit(self): pass
    def rollback(self): pass
    def close(self): pass


def _digest(monkeypatch, client):
    conn = _Conn()
    monkeypatch.setattr(em, "get_db_conn", lambda: conn)
    import shared.anthropic_client as ac
    monkeypatch.setattr(ac, "make_client", lambda key: client)
    return em.digest_kb_docs("k", limit=5), conn


def test_failed_extraction_is_not_marked_processed(monkeypatch):
    n, conn = _digest(monkeypatch, _Client(exc=RuntimeError("overloaded")))
    assert n == 0
    assert not any("_processed_no_insights" in q for q in conn.sql)


def test_genuinely_empty_doc_is_marked_processed(monkeypatch):
    n, conn = _digest(monkeypatch, _Client('{"insights": []}'))
    assert n == 0
    assert any("_processed_no_insights" in q for q in conn.sql)
