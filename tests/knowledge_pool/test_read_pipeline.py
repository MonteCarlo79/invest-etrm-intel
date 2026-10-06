# tests/knowledge_pool/test_read_pipeline.py
from services.knowledge_pool import intake_routes as ir


def test_read_pipeline_empty_returns_zero():
    class Cur:
        def __enter__(self): return self
        def __exit__(self, *a): return False
        def execute(self, sql, params=None): pass
        def fetchall(self): return []
    class Conn:
        def cursor(self): return Cur()
    out = ir.read_storage_pipeline(Conn())
    assert out == {"count": 0, "latest": [], "history": []}


def test_read_pipeline_metric_filter_param():
    captured = {}
    class Cur:
        def __enter__(self): return self
        def __exit__(self, *a): return False
        def execute(self, sql, params=None):
            captured["sql"], captured["params"] = sql, params
        def fetchall(self): return []
    class Conn:
        def cursor(self): return Cur()
    ir.read_storage_pipeline(Conn(), provinces=["宁夏", "甘肃"], metric="registry_gw")
    assert "province = ANY" in captured["sql"] and "metric = %s" in captured["sql"]
    assert captured["params"] == [["宁夏", "甘肃"], "registry_gw"]
