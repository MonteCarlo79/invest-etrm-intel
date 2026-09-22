# tests/nodal_trading/test_gridmap_extract.py
from services.nodal_forecast import gridmap_extract as gx

def test_parse_dedupes_by_substation_and_voltage():
    rows = [
        dict(name="汗海变电站", voltage_kv=500, substation="汗海", transformers=2,
             rated_mva=1500.0, n1_firm_mva=None, connected_plants=""),
        dict(name="汗海变电站", voltage_kv=500, substation="汗海", transformers=3,
             rated_mva=1500.0, n1_firm_mva=None, connected_plants=""),
    ]
    out = gx.parse_substation_rows(rows)
    assert len(out) == 1 and out[0]["transformers"] == 3   # later row wins on dedupe

def test_parse_normalizes_types_and_derives_substation():
    rows = [dict(name="谷山梁变电站", voltage_kv="500kV", substation="",
                 transformers="4", rated_mva="4800", n1_firm_mva=3600,
                 connected_plants=["凝光400", "洁能800"])]
    out = gx.parse_substation_rows(rows)
    assert len(out) == 1
    n = out[0]
    assert n["substation"] == "谷山梁"            # derived from name, 变电站 stripped
    assert n["voltage_kv"] == 500 and n["transformers"] == 4
    assert n["rated_mva"] == 4800.0 and n["n1_firm_mva"] == 3600.0
    assert n["connected_plants"] == "凝光400,洁能800"   # list joined for TEXT column

class _FakeCur:
    """Step-counting fake: execute #1 = registry rows (name, substation,
    voltage_kv), execute #2 = Fengxing node_names (1 col). Records all calls."""
    def __init__(self, reg_rows, fx_names):
        self.reg_rows, self.fx_names, self.calls, self.step = reg_rows, fx_names, [], 0
    def execute(self, s, p=None):
        self.step += 1; self.calls.append((s, p))
    def executemany(self, s, seq): self.calls.append((s, list(seq)))
    def fetchall(self):
        return self.reg_rows if self.step == 1 else self.fx_names

class _FakeConn:
    def __init__(self, reg_rows, fx_names): self.cur = _FakeCur(reg_rows, fx_names)
    def cursor(self): return self.cur
    def commit(self): pass

def _updates(conn):
    return [c for c in conn.cur.calls if c[0].lstrip().startswith("UPDATE")]

def test_seed_upserts_and_link_matches_fengxing():
    conn = _FakeConn(reg_rows=[("汗海变电站", "汗海", 500)],
                     fx_names=[("内蒙.汗海站/500kV.1M",)])
    nodes = [dict(name="汗海变电站", voltage_kv=500, substation="汗海", transformers=2,
                  rated_mva=1500.0, n1_firm_mva=None, zone=None,
                  fengxing_node_name=None, connected_plants="", source="网架图2025-12-22")]
    assert gx.seed_node_registry(conn, nodes) == 1
    out = gx.link_fengxing_names(conn)
    assert out["links"]["汗海"] == "内蒙.汗海站/500kV.1M"
    assert out["candidates"] == {}
    ups = _updates(conn)          # exact match IS persisted, per registry row (PK name)
    assert len(ups) == 1 and ups[0][1] == [("内蒙.汗海站/500kV.1M", "汗海变电站")]

def test_link_returns_none_when_unresolved():
    conn = _FakeConn(reg_rows=[("杜尔伯特变电站", "杜尔伯特", 500)],
                     fx_names=[("内蒙.汗海站/500kV.1M",)])
    out = gx.link_fengxing_names(conn)
    assert out["links"] == {"杜尔伯特": None} and out["candidates"] == {}
    assert not _updates(conn)

def test_substring_only_match_is_candidate_not_persisted():
    # Fengxing has ONLY a different substation whose name contains 汗海
    conn = _FakeConn(reg_rows=[("汗海变电站", "汗海", 500)],
                     fx_names=[("内蒙.汗海X站/500kV.1M",)])
    out = gx.link_fengxing_names(conn)
    assert out["links"]["汗海"] is None                       # no exact link
    assert out["candidates"]["汗海"] == ["内蒙.汗海X站/500kV.1M"]  # surfaced for review
    assert not _updates(conn)        # substring matches are NEVER written back

def test_multi_bus_exact_match_prefers_registry_voltage():
    conn = _FakeConn(
        reg_rows=[("某500变电站", "某", 500), ("某220变电站", "某", 220)],
        fx_names=[("内蒙.某站/220kV.1M",), ("内蒙.某站/500kV.1M",)])
    out = gx.link_fengxing_names(conn)
    assert out["links"]["某"] == "内蒙.某站/500kV.1M"   # highest-voltage row's pick
    ups = _updates(conn)
    assert len(ups) == 1                     # one executemany, per-row params
    params = ups[0][1]
    assert ("内蒙.某站/500kV.1M", "某500变电站") in params   # 500 row → 500 bus
    assert ("内蒙.某站/220kV.1M", "某220变电站") in params   # 220 row → 220 bus
