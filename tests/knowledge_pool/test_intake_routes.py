# tests/knowledge_pool/test_intake_routes.py
from services.knowledge_pool import intake_routes as ir


class _FakeCur:
    def __init__(self, conn): self.conn = conn
    def __enter__(self): return self
    def __exit__(self, *a): return False
    def execute(self, sql, params=None):
        self.conn.log.append((sql, params))
        self._result = self.conn.results.pop(0) if self.conn.results else None
    def fetchone(self): return self._result


class _FakeConn:
    def __init__(self, results=None): self.log, self.results = [], list(results or [])
    def __enter__(self): return self
    def __exit__(self, *a): return False
    def cursor(self): return _FakeCur(self)
    def commit(self): pass


def _patch_conn(monkeypatch, conn):
    monkeypatch.setattr(ir, "get_conn", lambda: conn)


def test_pipeline_table_ddl_and_upsert(monkeypatch):
    conn = _FakeConn()
    _patch_conn(monkeypatch, conn)
    n = ir.write_pipeline_rows(
        [{"metric": "registry_gw", "value": 19.93, "unit": "GW",
          "as_of_date": "2026-07-21", "source": "国网宁电[2026]70号"}],
        province="宁夏", source_doc_id=42)
    assert n == 1
    assert any("CREATE TABLE IF NOT EXISTS marketdata.province_storage_pipeline" in s for s, _ in conn.log)
    ins = [p for s, p in conn.log if "INSERT INTO marketdata.province_storage_pipeline" in s][0]
    assert ins[0] == "宁夏" and ins[5] == "国网宁电[2026]70号" and ins[6] == 42


def test_memory_note_insert_then_update(monkeypatch):
    conn = _FakeConn(results=[None])          # no existing row → insert
    _patch_conn(monkeypatch, conn)
    assert ir.upsert_memory_note(app="bess_map", subject="宁夏 — 调频需求", content="AGC需求小") == "inserted"
    conn2 = _FakeConn(results=[(9,)])         # existing active row → update
    _patch_conn(monkeypatch, conn2)
    assert ir.upsert_memory_note(app="bess_map", subject="宁夏 — 调频需求", content="AGC需求小（修订）") == "updated"
    assert any("UPDATE marketdata.agent_memory SET content" in s for s, _ in conn2.log)
    assert not [s for s, _ in conn2.log if "INSERT INTO marketdata.agent_memory" in s]


def test_rate_writers_skip_confirmed(monkeypatch):
    # skip-guard uses SELECT EXISTS(... status IN ('confirmed','superseded')) —
    # deterministic even when a confirmed row and a draft twin (different
    # source_file) coexist under the same natural key; fetchone → (bool,).
    conn = _FakeConn(results=[(True,)])        # confirmed/superseded exists → skipped, no INSERT
    _patch_conn(monkeypatch, conn)
    assert ir.write_capcomp_draft(province="山东", effective_date="2026-01-01",
                                  cap_comp_yuan_kw=0.0705, peak_duration_hours=2,
                                  source="intake:test") == "skipped"
    assert not [s for s, _ in conn.log if "INSERT INTO marketdata.province_cap_comp" in s]
    # no confirmed/superseded row → re-written (upsert)
    conn2 = _FakeConn(results=[(False,)])
    _patch_conn(monkeypatch, conn2)
    assert ir.write_capcomp_draft(province="山东", effective_date="2026-01-01",
                                  cap_comp_yuan_kw=0.08, peak_duration_hours=2,
                                  source="intake:test") == "written"


def test_ancillary_draft_written_and_skipped(monkeypatch):
    conn = _FakeConn(results=[(False,)])
    _patch_conn(monkeypatch, conn)
    assert ir.write_ancillary_draft(province="新疆", month="2026-06-01",
                                    metric="调频补偿费用_独立储能", amount_yuan=5349900.0,
                                    source_file="intake:六月结算") == "written"
    ins = [p for s, p in conn.log if "INSERT INTO marketdata.province_ancillary_revenue" in s][0]
    assert ins[5] == "draft"
    # EXISTS guard: True even if only a draft twin would match a naive LIMIT 1
    conn2 = _FakeConn(results=[(True,)])
    _patch_conn(monkeypatch, conn2)
    assert ir.write_ancillary_draft(province="新疆", month="2026-06-01",
                                    metric="m", amount_yuan=1.0, source_file="x") == "skipped"


def test_read_storage_pipeline_latest_and_history():
    rows = [
        ("宁夏", "2026-05-30", "installed_new_storage_gw", 10.13, "GW", "央视", "confirmed", None),
        ("宁夏", "2026-04-30", "installed_new_storage_gw", 8.0, "GW", "能源局", "confirmed", None),
        ("宁夏", "2026-07-21", "registry_gw", 19.93, "GW", "国网宁电", "confirmed", None),
    ]

    class Cur:
        def __init__(self): self._rows = rows
        def __enter__(self): return self
        def __exit__(self, *a): return False
        def execute(self, sql, params=None): pass
        def fetchall(self): return self._rows

    class Conn:
        def cursor(self): return Cur()

    out = ir.read_storage_pipeline(Conn(), provinces=["宁夏"])
    assert out["count"] == 3
    latest = {(r["province"], r["metric"]): r for r in out["latest"]}
    assert latest[("宁夏", "installed_new_storage_gw")]["value"] == 10.13   # newest first
    assert len(out["history"]) == 3
