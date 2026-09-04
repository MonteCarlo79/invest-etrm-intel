"""Tests for content-hash dedup in tab_settlement uploads."""
from apps.asset_risk.tab_settlement import _content_sha256, _already_ingested


def test_content_sha256_deterministic():
    assert _content_sha256(b"invoice") == _content_sha256(b"invoice")
    assert _content_sha256(b"invoice") != _content_sha256(b"invoice2")
    assert len(_content_sha256(b"x")) == 64


class _FakeConn:
    def __init__(self, found):
        self._found = found
        self.last_params = None

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def execute(self, query, params):
        self.last_params = params
        found = self._found

        class _Result:
            def first(self):
                return (1,) if found else None

        return _Result()


class _FakeEngine:
    def __init__(self, found):
        self.conn = _FakeConn(found)

    def connect(self):
        return self.conn


def test_already_ingested_true_when_hash_at_same_month():
    eng = _FakeEngine(found=True)
    assert _already_ingested(eng, 3, "2026-01-01", "abc123") is True
    assert eng.conn.last_params == {"bid": 3, "month": "2026-01-01", "h": "abc123"}


def test_already_ingested_false_when_absent():
    eng = _FakeEngine(found=False)
    assert _already_ingested(eng, 3, "2026-01-01", "abc123") is False
