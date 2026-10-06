# tests/knowledge_pool/test_intake_taxonomy.py
from services.knowledge_pool import knowledge_docs as kd


def test_new_categories_have_keywords_and_labels():
    for key in ("market_intel", "capacity_pipeline", "ancillary_market"):
        assert key in kd.CATEGORIES and kd.CATEGORIES[key], key
        assert key in kd.CATEGORY_LABELS, key
        assert key in kd.CATEGORY_LABELS_ZH, key


def test_keyword_classification_ningxia_deck():
    assert kd._keyword_category("宁夏独立储能在库规模——储备与规划对比") == "capacity_pipeline"
    assert kd._keyword_category("宁夏调峰及一二次调频需求分析 AGC 调频容量价格") == "ancillary_market"
    assert kd._keyword_category("第三方电力市场情报月报") == "market_intel"


def test_init_adds_province_column(monkeypatch):
    executed = []

    class _Cur:
        def __enter__(self): return self
        def __exit__(self, *a): return False
        def execute(self, sql, *a): executed.append(sql)

    class _Conn:
        def __enter__(self): return self
        def __exit__(self, *a): return False
        def cursor(self): return _Cur()
        def commit(self): pass

    monkeypatch.setattr(kd, "get_conn", lambda: _Conn())
    monkeypatch.setattr(kd, "_TABLES_INITIALIZED", False)
    kd.init_knowledge_tables()
    assert any("ADD COLUMN IF NOT EXISTS province" in s for s in executed)
