# tests/knowledge_pool/test_intake_batch.py
from services.knowledge_pool import knowledge_docs as kd
from services.knowledge_pool.intake_extract import batch_hash


def test_batch_hash_order_independent():
    a, b, c = b"alpha", b"beta", b"gamma"
    assert batch_hash([a, b, c]) == batch_hash([c, a, b])
    assert batch_hash([a, b]) != batch_hash([a, b, c])


class _FakeCur:
    def __init__(self, store): self.store = store
    def __enter__(self): return self
    def __exit__(self, *a): return False
    def execute(self, sql, params=None): self.store.append(("execute", sql, params))
    def executemany(self, sql, rows): self.store.append(("executemany", sql, list(rows)))
    def fetchone(self): return self.store and self.store.pop(0) if self.store and isinstance(self.store[0], tuple) and self.store[0][0] == "row" else None


class _FakeConn:
    def __init__(self, store): self.store = store
    def __enter__(self): return self
    def __exit__(self, *a): return False
    def cursor(self): return _FakeCur(self.store)
    def commit(self): pass


def _patch(monkeypatch, store, fetchone_row=None):
    monkeypatch.setattr(kd, "get_conn", lambda: _FakeConn(store))
    monkeypatch.setattr(kd, "_TABLES_INITIALIZED", True)
    monkeypatch.setattr(kd, "_embed_chunks_for_doc", lambda doc_id: None)


def test_batch_insert_doc_and_chunks(monkeypatch):
    store = []
    _patch(monkeypatch, store)
    # dedup SELECT returns nothing (new doc); INSERT ... RETURNING id yields 42
    real_execute = _FakeCur.execute
    def execute(self, sql, params=None):
        self.store.append(("execute", sql, params))
        self._pending = ("row", (42,)) if "RETURNING id" in sql else None
    def fetchone(self):
        return getattr(self, "_pending", None) and self._pending[1]
    monkeypatch.setattr(_FakeCur, "execute", execute)
    monkeypatch.setattr(_FakeCur, "fetchone", fetchone)

    doc_id, is_new, cat = kd.ingest_document_batch(
        [(1, "page one text"), (2, "page two text")],
        file_name="宁夏储能分析", file_hash="abc123", title="宁夏储能分析",
        category="capacity_pipeline", province="宁夏", api_key=None, synthesize=False,
    )
    assert (doc_id, is_new, cat) == (42, True, "capacity_pipeline")
    doc_inserts = [s for kind, s, p in store if kind == "execute" and "INSERT INTO staging.spot_knowledge_docs" in s]
    assert len(doc_inserts) == 1
    params = [p for kind, s, p in store if kind == "execute" and "INSERT INTO staging.spot_knowledge_docs" in s][0]
    assert "宁夏" in params          # province bound
    chunk_rows = [rows for kind, sql, rows in store if kind == "executemany"]
    assert chunk_rows and all(r[0] == 42 for r in chunk_rows[0])


def test_batch_dedup_returns_existing(monkeypatch):
    store = []
    _patch(monkeypatch, store)
    monkeypatch.setattr(_FakeCur, "execute",
        lambda self, sql, params=None: self.store.append(("execute", sql, params)))
    monkeypatch.setattr(_FakeCur, "fetchone", lambda self: (7, "market_intel"))
    doc_id, is_new, cat = kd.ingest_document_batch(
        [(1, "x")], file_name="dup", file_hash="deadbeef", title="dup",
        category="market_intel", province=None, api_key=None, synthesize=False,
    )
    assert (doc_id, is_new, cat) == (7, False, "market_intel")
    assert not [s for kind, s, p in store if "INSERT INTO staging.spot_knowledge_docs" in s]
