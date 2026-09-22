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

def test_seed_upserts_and_link_matches_fengxing():
    class Cur:
        def __init__(self): self.calls = []
        def execute(self, s, p=None): self.calls.append((s, p))
        def executemany(self, s, seq): self.calls.append((s, list(seq)))
        def fetchall(self): return [("内蒙.汗海站/500kV.1M",)]
    class Conn:
        def __init__(self): self.cur = Cur()
        def cursor(self): return self.cur
        def commit(self): pass
    conn = Conn()
    nodes = [dict(name="汗海变电站", voltage_kv=500, substation="汗海", transformers=2,
                  rated_mva=1500.0, n1_firm_mva=None, zone=None,
                  fengxing_node_name=None, connected_plants="", source="网架图2025-12-22")]
    assert gx.seed_node_registry(conn, nodes) == 1
    links = gx.link_fengxing_names(conn)
    assert links.get("汗海") == "内蒙.汗海站/500kV.1M"

def test_link_returns_original_substation_when_unresolved():
    class Cur:
        def __init__(self): self.step = 0
        def execute(self, s, p=None): self.step += 1
        def executemany(self, s, seq): pass
        def fetchall(self):
            # first query = registry substations, second = Fengxing node names
            return [("杜尔伯特",)] if self.step == 1 else [("内蒙.汗海站/500kV.1M",)]
    class Conn:
        def __init__(self): self.cur = Cur()
        def cursor(self): return self.cur
        def commit(self): pass
    links = gx.link_fengxing_names(Conn())
    assert links == {"杜尔伯特": "杜尔伯特"}   # no Fengxing match → original echoed back
