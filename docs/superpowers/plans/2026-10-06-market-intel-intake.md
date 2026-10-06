# Market Intel Intake Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a batch "Intel Intake" flow to the spot-market Knowledge tab — N files become ONE knowledge doc with vision-extracted, user-reviewed routing to KB chunks, agent memory notes, rate-table drafts, and a new pipeline-statistics table.

**Architecture:** Three units per spec: `intake_extract.py` (batch → structured proposal via one Sonnet vision call), `intake_routes.py` (per-route DB writers, each in its own transaction), extensions to `knowledge_docs.py` (taxonomy, `province` column, `ingest_document_batch`) and `apps/spot-market/app.py` (Intake tab in the KB expander + `get_storage_pipeline` Strategist tool).

**Tech Stack:** Python 3, Streamlit, psycopg2-style `get_conn()`, Anthropic SDK (`shared.anthropic_client.make_client`), pytest.

**Spec:** `docs/superpowers/specs/2026-10-06-market-intel-intake-design.md`

## Global Constraints

- Commit messages end with `Co-Authored-By: Claude Code <noreply@anthropic.com>`.
- `git add` with **explicit file paths only** — never `git add -A` / `git add .`; never stage `infra/terraform/terraform.tfvars`.
- Do NOT touch: `apps/bess-map/`, `apps/portal/`, `services/hermes/`, `infra/`. (Parallel sessions have uncommitted work in `apps/bess-map/app.py`, `infra/terraform/` — leave them dirty.)
- Git push uses the SSH remote (`git@github.com:...`) — HTTPS is blocked on this network.
- If a git command hangs >2 min (OneDrive hydration), `rm -f .git/index.lock` and retry; commit via temp-index pattern if persistent.
- Province names are Chinese market names (`宁夏`, `山东`, `蒙西`…) — same convention as `province_cap_comp`.
- Rate-route writers write `status='draft'` and must SKIP rows already `confirmed`/`superseded` (hermes td:185 dedup lesson). Pipeline rows write `status='confirmed'` (intake panel IS the human review).
- No deployment in this plan — local verification only. Deploy is a separate, explicitly-confirmed step.
- Tests live in `tests/knowledge_pool/` (existing home of `test_register_url_wechat.py`, `test_cjk_two_phase.py`).

---

### Task 1: Taxonomy + `province` column in knowledge_docs.py

**Files:**
- Modify: `services/knowledge_pool/knowledge_docs.py` (CATEGORIES ~line 304, CATEGORY_LABELS ~line 331, CATEGORY_LABELS_ZH ~line 342, `init_knowledge_tables` ~line 391)
- Test: `tests/knowledge_pool/test_intake_taxonomy.py`

**Interfaces:**
- Consumes: nothing new.
- Produces: `CATEGORIES`, `CATEGORY_LABELS`, `CATEGORY_LABELS_ZH` each gaining keys `market_intel`, `capacity_pipeline`, `ancillary_market`; `staging.spot_knowledge_docs.province TEXT` column (idempotent ALTER).

- [ ] **Step 1: Write the failing test**

```python
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/knowledge_pool/test_intake_taxonomy.py -v`
Expected: FAIL — `market_intel` not in CATEGORIES; no province ALTER captured.

- [ ] **Step 3: Implement**

In `services/knowledge_pool/knowledge_docs.py`, append to `CATEGORIES` (after `research_report`):

```python
    "market_intel": [
        "市场情报", "市场分析", "市场调研", "行业动态", "第三方分析",
        "market intelligence", "market intel",
    ],
    "capacity_pipeline": [
        "装机", "在库", "备案", "项目储备", "并网装机", "规划目标",
        "建设清单", "接入缺口", "规划缺口",
        "installed capacity", "project pipeline", "grid connection queue",
    ],
    "ancillary_market": [
        "调频", "调峰", "辅助服务", "一次调频", "二次调频", "AGC",
        "容量补偿", "容量电价", "资金池",
        "frequency regulation", "ancillary service", "capacity payment",
    ],
```

Append to `CATEGORY_LABELS` and `CATEGORY_LABELS_ZH` respectively:

```python
    "market_intel":      "Market Intelligence",
    "capacity_pipeline": "Capacity & Pipeline",
    "ancillary_market":  "Ancillary Market",
```
```python
    "market_intel":      "市场情报",
    "capacity_pipeline": "装机与项目储备",
    "ancillary_market":  "辅助服务",
```

In `init_knowledge_tables()`, after the existing `app` column ALTER block, add:

```python
            cur.execute("""
                ALTER TABLE staging.spot_knowledge_docs
                ADD COLUMN IF NOT EXISTS province TEXT
            """)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/knowledge_pool/test_intake_taxonomy.py -v`
Expected: 3 PASS.

- [ ] **Step 5: Commit**

```bash
git add services/knowledge_pool/knowledge_docs.py tests/knowledge_pool/test_intake_taxonomy.py
git commit -m "Add intake taxonomy categories and province column to KB schema

Co-Authored-By: Claude Code <noreply@anthropic.com>"
```

---

### Task 2: `batch_hash` + `ingest_document_batch`

**Files:**
- Create: `services/knowledge_pool/intake_extract.py` (hash helper only — full extractor is Task 3)
- Modify: `services/knowledge_pool/knowledge_docs.py` (append `ingest_document_batch` after `register_and_ingest` ~line 612)
- Test: `tests/knowledge_pool/test_intake_batch.py`

**Interfaces:**
- Consumes: `get_conn`, `init_knowledge_tables`, `_chunk_text`, `_embed_chunks_for_doc`, `synthesis.synthesize_on_ingest` (all existing in knowledge_docs).
- Produces:
  - `intake_extract.batch_hash(pages_bytes: list[bytes]) -> str` — order-independent batch digest.
  - `knowledge_docs.ingest_document_batch(pages_text: list[tuple[int, str]], *, file_name: str, file_hash: str, title: str, category: str, province: str | None, app: str = "strategist", api_key: str | None = None, synthesize: bool = True) -> tuple[int, bool, str]` returning `(doc_id, is_new, category)`.

- [ ] **Step 1: Write the failing test**

```python
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/knowledge_pool/test_intake_batch.py -v`
Expected: FAIL — `intake_extract` module and `ingest_document_batch` do not exist.

- [ ] **Step 3: Implement**

Create `services/knowledge_pool/intake_extract.py`:

```python
"""Market intel intake — batch hashing (extractor lands in a later task)."""
from __future__ import annotations

import hashlib


def batch_hash(pages_bytes: list[bytes]) -> str:
    """Order-independent content digest of a whole upload batch."""
    h = hashlib.sha256()
    for digest in sorted(hashlib.sha256(b).hexdigest() for b in pages_bytes):
        h.update(digest.encode())
    return h.hexdigest()
```

Append to `services/knowledge_pool/knowledge_docs.py` (after `register_and_ingest`):

```python
def ingest_document_batch(
    pages_text: list[tuple[int, str]],
    *,
    file_name: str,
    file_hash: str,
    title: str,
    category: str,
    province: Optional[str],
    app: str = "strategist",
    api_key: Optional[str] = None,
    synthesize: bool = True,
) -> tuple[int, bool, str]:
    """
    Register ONE document from an intake batch (N pre-extracted pages).
    Text extraction happens upstream (intake_extract); this only persists.
    Returns (doc_id, is_new, category); is_new=False on duplicate file_hash.
    """
    init_knowledge_tables()

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, category FROM staging.spot_knowledge_docs WHERE file_hash = %s",
                (file_hash,),
            )
            row = cur.fetchone()
    if row:
        return row[0], False, row[1]

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO staging.spot_knowledge_docs
                    (file_name, file_hash, category, app, title, province,
                     file_size_bytes, page_count, ingest_status)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'parsed')
                RETURNING id
                """,
                (file_name, file_hash, category, app, title, province,
                 0, len(pages_text)),
            )
            doc_id = cur.fetchone()[0]

            inserts = []
            chunk_index = 0
            for page_no, text in pages_text:
                for chunk in _chunk_text(text):
                    inserts.append((doc_id, page_no, chunk_index, chunk))
                    chunk_index += 1
            if inserts:
                cur.executemany(
                    """
                    INSERT INTO staging.spot_knowledge_chunks
                        (doc_id, page_no, chunk_index, chunk_text)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (doc_id, chunk_index) DO NOTHING
                    """,
                    inserts,
                )
        conn.commit()

    threading_mod = __import__("threading")
    threading_mod.Thread(target=_embed_chunks_for_doc, args=(doc_id,), daemon=True).start()

    if api_key and synthesize:
        try:
            from .synthesis import synthesize_on_ingest
            threading_mod.Thread(
                target=synthesize_on_ingest, args=(doc_id, api_key), daemon=True,
            ).start()
        except Exception:
            pass

    return doc_id, True, category
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/knowledge_pool/test_intake_batch.py -v`
Expected: 3 PASS. Then run the existing KB tests to check no regression: `python -m pytest tests/knowledge_pool/ -v` — all PASS.

- [ ] **Step 5: Commit**

```bash
git add services/knowledge_pool/intake_extract.py services/knowledge_pool/knowledge_docs.py tests/knowledge_pool/test_intake_batch.py
git commit -m "Add batch document ingest with order-independent dedup hash

Co-Authored-By: Claude Code <noreply@anthropic.com>"
```

---

### Task 3: Extractor — `extract_batch` (vision → IntakeProposal)

**Files:**
- Modify: `services/knowledge_pool/intake_extract.py` (extend)
- Test: `tests/knowledge_pool/test_intake_extract.py`

**Interfaces:**
- Consumes: `shared.anthropic_client.make_client` (same as `_describe_image`), `batch_hash` (Task 2).
- Produces:
  - `IMAGE_EXTS: set[str]`, `MIME: dict[str, str]`
  - `PROVINCES: list[str]` — 29 LingFeng market names + `全国`
  - `ROUTE_TYPES: tuple` = `("capacity_comp_rate", "fr_market_params", "ancillary_revenue", "pipeline_stat", "spot_note", "quant_note")`
  - `CATEGORIES_ALLOWED: tuple` = existing KB category keys
  - `@dataclass Page(filename: str, data: bytes, kind: str, text: str = "", error: str | None = None)` — kind ∈ `"image" | "text"`
  - `@dataclass RouteProposal(type: str, province: str | None, content: str, structured: dict)`
  - `@dataclass IntakeProposal(title, province, category, summary, pages: list[tuple[int, str]], routes: list[RouteProposal], batch_hash: str, raw_json: dict)`
  - `extract_batch(pages: list[Page], api_key: str, *, _client=None) -> IntakeProposal` — `_client` seam for tests. Raises `IntakeError` on unparseable response. Image groups of ≤20 per vision call; groups merged into one proposal (title/category/province from first group; pages renumbered; routes concatenated).

- [ ] **Step 1: Write the failing test**

```python
# tests/knowledge_pool/test_intake_extract.py
import json
import pytest
from services.knowledge_pool.intake_extract import (
    IntakeError, Page, extract_batch, batch_hash,
)


_CANNED = {
    "title": "宁夏储能市场分析",
    "province": "宁夏",
    "category": "capacity_pipeline",
    "summary": "宁夏独立储能在库19.93GW，备案合计31.63GW，规划缺口11.46GW。",
    "pages": [{"page_no": 1, "text": "第一页转录"}],
    "routes": [
        {"type": "pipeline_stat", "province": "宁夏", "content": "在库19.93GW",
         "structured": {"rows": [{"metric": "registry_gw", "value": 19.93,
                                   "unit": "GW", "as_of_date": "2026-07-21",
                                   "source": "国网宁电[2026]70号"}]}},
        {"type": "quant_note", "province": "宁夏",
         "content": "AGC二次调频需求仅17.65~35.3万kW，调频收入空间受限。",
         "structured": {}},
        {"type": "nonsense_type", "province": None, "content": "dropped", "structured": {}},
    ],
}


class _FakeResp:
    def __init__(self, text): self.content = [type("B", (), {"text": text})()]


class _FakeClient:
    def __init__(self, text): self._text = text; self.calls = []
    @property
    def messages(self): return self
    def create(self, **kwargs): self.calls.append(kwargs); return _FakeResp(self._text)


def test_extract_batch_parses_and_filters_routes():
    client = _FakeClient(json.dumps(_CANNED, ensure_ascii=False))
    pages = [Page(filename="IMG_1.jpg", data=b"\xff\xd8img1", kind="image"),
             Page(filename="IMG_2.jpg", data=b"\xff\xd8img2", kind="image")]
    prop = extract_batch(pages, api_key="k", _client=client)
    assert prop.title == "宁夏储能市场分析"
    assert prop.province == "宁夏"
    assert prop.category == "capacity_pipeline"
    assert [r.type for r in prop.routes] == ["pipeline_stat", "quant_note"]  # unknown dropped
    assert prop.batch_hash == batch_hash([b"\xff\xd8img1", b"\xff\xd8img2"])
    assert len(client.calls) == 1                      # one vision call for 2 images
    content = client.calls[0]["messages"][0]["content"]
    assert sum(1 for b in content if b.get("type") == "image") == 2


def test_extract_batch_bad_json_raises():
    client = _FakeClient("not json at all")
    with pytest.raises(IntakeError):
        extract_batch([Page(filename="a.jpg", data=b"x", kind="image")], api_key="k", _client=client)


def test_extract_batch_groups_over_20_images():
    client = _FakeClient(json.dumps(_CANNED, ensure_ascii=False))
    pages = [Page(filename=f"IMG_{i}.jpg", data=f"img{i}".encode(), kind="image") for i in range(25)]
    prop = extract_batch(pages, api_key="k", _client=client)
    assert len(client.calls) == 2                      # 20 + 5
    assert prop.title == "宁夏储能市场分析"            # from first group


def test_extract_batch_text_pages_skip_vision():
    client = _FakeClient(json.dumps(_CANNED, ensure_ascii=False))
    pages = [Page(filename="doc.pdf", data=b"pdf", kind="text", text="政策全文……")]
    prop = extract_batch(pages, api_key="k", _client=client)
    content = client.calls[0]["messages"][0]["content"]
    assert not [b for b in content if b.get("type") == "image"]
    assert "政策全文" in content[-1]["text"]           # text pages inlined in prompt
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/knowledge_pool/test_intake_extract.py -v`
Expected: FAIL — names not defined.

- [ ] **Step 3: Implement**

Extend `services/knowledge_pool/intake_extract.py`:

```python
"""Market intel intake — batch → structured proposal via one vision call per ≤20 images."""
from __future__ import annotations

import base64
import hashlib
import json
from dataclasses import dataclass, field
from typing import Optional

IMAGE_EXTS = {"png", "jpg", "jpeg", "webp"}
MIME = {"png": "image/png", "jpg": "image/jpeg", "jpeg": "image/jpeg", "webp": "image/webp"}

# Mirrors services/lingfeng/run_daily.py::_ALL_MARKETS (+ 全国) — kept local to
# avoid importing the playwright-dependent lingfeng module into the app.
PROVINCES = [
    "河南", "新疆", "吉林", "海南", "湖北", "四川", "黑龙江", "福建", "浙江", "江苏",
    "广西", "安徽", "陕西", "贵州", "云南", "广东", "蒙东", "湖南", "宁夏", "辽宁",
    "河北南网", "甘肃", "蒙西", "山东", "山西", "冀北", "广州", "青海", "江西", "全国",
]

ROUTE_TYPES = ("capacity_comp_rate", "fr_market_params", "ancillary_revenue",
               "pipeline_stat", "spot_note", "quant_note")

CATEGORIES_ALLOWED = ("market_rules", "annual_report", "policy_doc", "technical_spec",
                      "research_report", "market_intel", "capacity_pipeline",
                      "ancillary_market", "other")

_GROUP = 20  # max images per vision call


class IntakeError(Exception):
    """Extraction failed (API error or unparseable response)."""


@dataclass
class Page:
    filename: str
    data: bytes
    kind: str                     # 'image' | 'text'
    text: str = ""                # pre-extracted for kind='text'
    error: Optional[str] = None


@dataclass
class RouteProposal:
    type: str
    province: Optional[str]
    content: str
    structured: dict = field(default_factory=dict)


@dataclass
class IntakeProposal:
    title: str
    province: Optional[str]
    category: str
    summary: str
    pages: list[tuple[int, str]]
    routes: list[RouteProposal]
    batch_hash: str
    raw_json: dict = field(default_factory=dict)


def batch_hash(pages_bytes: list[bytes]) -> str:
    h = hashlib.sha256()
    for digest in sorted(hashlib.sha256(b).hexdigest() for b in pages_bytes):
        h.update(digest.encode())
    return h.hexdigest()


_PROMPT_TMPL = """你在分析一组中文电力市场情报文件（同一主题，共 {n_pages} 页）。
输出严格 JSON（不要 markdown 围栏，不要任何额外文字）：
{{
  "title": "文档标题（主题+省份+日期，如可得）",
  "province": "省份（限选：{provinces}；无法判断为 null）",
  "category": "限选一个：{categories}",
  "summary": "150字内中文摘要",
  "pages": [{{"page_no": 页码整数, "text": "该页完整转录：表格逐行、图表读数与单位"}}],
  "routes": [{{"type": "...", "province": "省份或null", "content": "给对应分析师看的中文要点", "structured": {{...}}}}]
}}

路由类型（只输出文中真实存在的，宁缺毋滥）：
- capacity_comp_rate: 容量电价/容量补偿标准 → structured: {{"cap_comp_yuan_kw": 数值, "peak_duration_hours": 数值或null, "effective_date": "YYYY-MM-DD"}}
- fr_market_params: 调频容量价格(元/kW·h)/全省资金池 → structured: {{"fr_price_yuan_kw_h": 数值或null, "fr_pool_billion_yuan": 数值或null, "effective_date": "YYYY-MM-DD"}}
- ancillary_revenue: 已结算调频/辅助服务收入月度金额 → structured: {{"month": "YYYY-MM-01", "metric": "指标名", "amount_yuan": 数值（元）}}
- pipeline_stat: 装机/在库/备案/规划/缺口统计 → structured: {{"rows": [{{"metric": "英文蛇形名", "value": 数值, "unit": "GW|GWh|个|倍", "as_of_date": "YYYY-MM-DD", "source": "出处"}}]}}
- spot_note: 给现货策略分析师的供需/格局解读（structured 留空 {{}}）
- quant_note: 给储能量化分析师的收益机制/约束/需求体量解读（structured 留空 {{}}）

单位换算：万千瓦→GW 除以10；万元→元 乘10000；亿kWh→GWh 乘100。
页码从 {page_start} 开始连续编号。"""


def _call_vision(client, image_blocks: list[dict], text_pages: list[str],
                 page_start: int) -> dict:
    prompt = _PROMPT_TMPL.format(
        n_pages=len(image_blocks) + len(text_pages),
        provinces="、".join(PROVINCES), categories=" | ".join(CATEGORIES_ALLOWED),
        page_start=page_start,
    )
    if text_pages:
        prompt += "\n\n以下为文本页内容：\n" + "\n\n".join(text_pages)
    resp = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=4096,
        messages=[{"role": "user", "content": image_blocks + [{"type": "text", "text": prompt}]}],
    )
    raw = resp.content[0].text.strip()
    try:
        start, end = raw.index("{"), raw.rindex("}") + 1
        return json.loads(raw[start:end])
    except (ValueError, json.JSONDecodeError) as exc:
        raise IntakeError(f"Extractor returned unparseable JSON: {exc}; raw head: {raw[:200]}") from exc


def _routes_from(data: dict) -> list[RouteProposal]:
    out = []
    for r in data.get("routes") or []:
        if r.get("type") not in ROUTE_TYPES:
            continue
        out.append(RouteProposal(
            type=r["type"], province=r.get("province"),
            content=str(r.get("content", "")), structured=r.get("structured") or {},
        ))
    return out


def extract_batch(pages: list[Page], api_key: str, *, _client=None) -> IntakeProposal:
    """One vision call per ≤20 image pages; groups merged into one proposal."""
    if _client is None:
        from shared.anthropic_client import make_client
        _client = make_client(api_key)

    images = [p for p in pages if p.kind == "image" and p.error is None]
    texts = [p for p in pages if p.kind == "text" and p.error is None]
    text_blobs = [f"【{p.filename}】\n{p.text}" for p in texts]

    groups = [images[i:i + _GROUP] for i in range(0, len(images), _GROUP)] or [[]]
    merged_pages: list[tuple[int, str]] = []
    merged_routes: list[RouteProposal] = []
    first: dict | None = None
    page_cursor = 1

    for gi, group in enumerate(groups):
        blocks = [
            {"type": "image",
             "source": {"type": "base64",
                        "media_type": MIME.get(p.filename.rsplit(".", 1)[-1].lower(), "image/jpeg"),
                        "data": base64.standard_b64encode(p.data).decode()}}
            for p in group
        ]
        data = _call_vision(_client, blocks, text_blobs if gi == 0 else [], page_cursor)
        if first is None:
            first = data
        for p in data.get("pages") or []:
            merged_pages.append((int(p.get("page_no", page_cursor)), str(p.get("text", ""))))
            page_cursor = max(page_cursor + 1, merged_pages[-1][0] + 1)
        merged_routes.extend(_routes_from(data))

    first = first or {}
    category = first.get("category")
    if category not in CATEGORIES_ALLOWED:
        category = "other"
    return IntakeProposal(
        title=str(first.get("title") or pages[0].filename if pages else "intake"),
        province=first.get("province") if first.get("province") in PROVINCES else None,
        category=category,
        summary=str(first.get("summary", "")),
        pages=merged_pages,
        routes=merged_routes,
        batch_hash=batch_hash([p.data for p in pages]),
        raw_json=first,
    )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/knowledge_pool/test_intake_extract.py -v`
Expected: 4 PASS.

- [ ] **Step 5: Commit**

```bash
git add services/knowledge_pool/intake_extract.py tests/knowledge_pool/test_intake_extract.py
git commit -m "Add intake extractor: batch vision call to structured proposal

Co-Authored-By: Claude Code <noreply@anthropic.com>"
```

---

### Task 4: Route writers — `intake_routes.py`

**Files:**
- Create: `services/knowledge_pool/intake_routes.py`
- Test: `tests/knowledge_pool/test_intake_routes.py`

**Interfaces:**
- Consumes: `services.knowledge_pool.knowledge_docs.get_conn`; `RouteProposal` (Task 3); `source_doc_id` from `ingest_document_batch` (Task 2).
- Produces (each opens its own connection — route failures are isolated):
  - `ensure_pipeline_table() -> None`
  - `write_pipeline_rows(rows: list[dict], *, province: str | None, source_doc_id: int) -> int` — status `'confirmed'`, upsert on `(province, as_of_date, metric, COALESCE(source,''))`
  - `upsert_memory_note(*, app: str, subject: str, content: str, source: str = "intake") -> str` — `'updated' | 'inserted'`; category fixed `'province_note'`; update-in-place on existing active `(app, category, subject)` — never duplicate (register_url lesson)
  - `write_capcomp_draft(*, province, effective_date, cap_comp_yuan_kw, peak_duration_hours, source) -> str` — `'written' | 'skipped'`
  - `write_fr_market_draft(*, province, effective_date, fr_price_yuan_kw_h, fr_pool_billion_yuan, source) -> str` — `'written' | 'skipped'`
  - `write_ancillary_draft(*, province, month, metric, amount_yuan, source_file) -> str` — `'written' | 'skipped'`
  - `read_storage_pipeline(conn, provinces: list[str] | None = None, metric: str | None = None) -> dict` — `{"count", "latest", "history"}`; used by the Strategist tool in Task 5
  - Rate writers return `'skipped'` when an existing row is `confirmed` or `superseded` (never downgrade).

- [ ] **Step 1: Write the failing test**

```python
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
    # existing confirmed row → skipped, no INSERT
    conn = _FakeConn(results=[("confirmed",)])
    _patch_conn(monkeypatch, conn)
    assert ir.write_capcomp_draft(province="山东", effective_date="2026-01-01",
                                  cap_comp_yuan_kw=0.0705, peak_duration_hours=2,
                                  source="intake:test") == "skipped"
    assert not [s for s, _ in conn.log if "INSERT INTO marketdata.province_cap_comp" in s]
    # existing draft row → re-written (upsert)
    conn2 = _FakeConn(results=[("draft",)])
    _patch_conn(monkeypatch, conn2)
    assert ir.write_capcomp_draft(province="山东", effective_date="2026-01-01",
                                  cap_comp_yuan_kw=0.08, peak_duration_hours=2,
                                  source="intake:test") == "written"


def test_ancillary_draft_written_and_skipped(monkeypatch):
    conn = _FakeConn(results=[None])
    _patch_conn(monkeypatch, conn)
    assert ir.write_ancillary_draft(province="新疆", month="2026-06-01",
                                    metric="调频补偿费用_独立储能", amount_yuan=5349900.0,
                                    source_file="intake:六月结算") == "written"
    ins = [p for s, p in conn.log if "INSERT INTO marketdata.province_ancillary_revenue" in s][0]
    assert ins[5] == "draft"
    conn2 = _FakeConn(results=[("superseded",)])
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/knowledge_pool/test_intake_routes.py -v`
Expected: FAIL — module not found.

- [ ] **Step 3: Implement**

Create `services/knowledge_pool/intake_routes.py`:

```python
"""Intake route writers — one connection per writer so route failures stay isolated."""
from __future__ import annotations

from typing import Optional

from services.knowledge_pool.knowledge_docs import get_conn

_PIPELINE_DDL = """
CREATE TABLE IF NOT EXISTS marketdata.province_storage_pipeline (
    id            SERIAL PRIMARY KEY,
    province      TEXT   NOT NULL,
    as_of_date    DATE   NOT NULL,
    metric        TEXT   NOT NULL,
    value         NUMERIC,
    unit          TEXT,
    source        TEXT,
    source_doc_id INT REFERENCES staging.spot_knowledge_docs(id),
    status        TEXT   NOT NULL DEFAULT 'confirmed',
    notes         TEXT,
    ingested_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_psp_nat
    ON marketdata.province_storage_pipeline (province, as_of_date, metric, COALESCE(source, ''));
CREATE INDEX IF NOT EXISTS idx_psp_prov_date
    ON marketdata.province_storage_pipeline (province, as_of_date DESC);
"""


def ensure_pipeline_table() -> None:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(_PIPELINE_DDL)
        conn.commit()


def write_pipeline_rows(rows: list[dict], *, province: Optional[str], source_doc_id: int) -> int:
    ensure_pipeline_table()
    n = 0
    with get_conn() as conn:
        with conn.cursor() as cur:
            for r in rows:
                cur.execute(
                    """
                    INSERT INTO marketdata.province_storage_pipeline
                        (province, as_of_date, metric, value, unit, source, source_doc_id, status, notes)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, 'confirmed', %s)
                    ON CONFLICT (province, as_of_date, metric, COALESCE(source, ''))
                    DO UPDATE SET value = EXCLUDED.value, unit = EXCLUDED.unit,
                                  source_doc_id = EXCLUDED.source_doc_id, ingested_at = NOW()
                    """,
                    (r.get("province") or province, r["as_of_date"], r["metric"],
                     r.get("value"), r.get("unit"), r.get("source"), source_doc_id,
                     r.get("notes")),
                )
                n += 1
        conn.commit()
    return n


def upsert_memory_note(*, app: str, subject: str, content: str, source: str = "intake") -> str:
    """Update-in-place on existing active (app, 'province_note', subject); else insert."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id FROM marketdata.agent_memory
                WHERE active AND app = %s AND category = 'province_note' AND subject = %s
                ORDER BY id DESC LIMIT 1
                """,
                (app, subject),
            )
            row = cur.fetchone()
            if row:
                cur.execute(
                    "UPDATE marketdata.agent_memory SET content = %s, source = %s WHERE id = %s",
                    (content, source, row[0]),
                )
                action = "updated"
            else:
                cur.execute(
                    """
                    INSERT INTO marketdata.agent_memory (app, category, subject, content, source)
                    VALUES (%s, 'province_note', %s, %s, %s)
                    """,
                    (app, subject, content, source),
                )
                action = "inserted"
        conn.commit()
    return action


def _write_rate_draft(*, table: str, key_cols: dict, conflict_target: str,
                      update_cols: list[str], payload: dict) -> str:
    """Shared skip-guard: never touch confirmed/superseded rows; upsert drafts only.

    key_cols: natural-key lookup for the status check (keys used verbatim, so
              expressions like "COALESCE(source, '')" are valid).
    conflict_target: exact ON CONFLICT target matching the table's unique index.
    update_cols: payload columns refreshed on conflict (natural keys excluded).
    """
    where = " AND ".join(f"{c} = %s" for c in key_cols)
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT status FROM {table} WHERE {where} LIMIT 1",
                        tuple(key_cols.values()))
            row = cur.fetchone()
            if row and row[0] in ("confirmed", "superseded"):
                return "skipped"
            cols = ", ".join(payload)
            ph = ", ".join(["%s"] * len(payload))
            updates = ", ".join(f"{c} = EXCLUDED.{c}" for c in update_cols)
            cur.execute(
                f"INSERT INTO {table} ({cols}) VALUES ({ph}) "
                f"ON CONFLICT {conflict_target} DO UPDATE SET {updates}",
                tuple(payload.values()),
            )
        conn.commit()
    return "written"


def write_capcomp_draft(*, province, effective_date, cap_comp_yuan_kw,
                        peak_duration_hours, source) -> str:
    return _write_rate_draft(
        table="marketdata.province_cap_comp",
        key_cols={"province": province, "effective_date": effective_date,
                  "COALESCE(source, '')": source},
        conflict_target="(province, effective_date, COALESCE(source, ''))",
        update_cols=["cap_comp_yuan_kw", "peak_duration_hours", "status"],
        payload={"province": province, "effective_date": effective_date,
                 "cap_comp_yuan_kw": cap_comp_yuan_kw,
                 "peak_duration_hours": peak_duration_hours,
                 "source": source, "status": "draft"},
    )
```

The matching `fr_market` and `ancillary` writers use the same helper (ancillary's unique index includes `source_file`, so its conflict target carries it and its skip-guard checks `(province, month, metric)` only — matching `extract_ancillary.upsert_rows` semantics):

```python
def write_fr_market_draft(*, province, effective_date, fr_price_yuan_kw_h,
                          fr_pool_billion_yuan, source) -> str:
    return _write_rate_draft(
        table="marketdata.province_fr_market",
        key_cols={"province": province, "effective_date": effective_date,
                  "COALESCE(source, '')": source},
        conflict_target="(province, effective_date, COALESCE(source, ''))",
        update_cols=["fr_price_yuan_kw_h", "fr_pool_billion_yuan", "status"],
        payload={"province": province, "effective_date": effective_date,
                 "fr_price_yuan_kw_h": fr_price_yuan_kw_h,
                 "fr_pool_billion_yuan": fr_pool_billion_yuan,
                 "source": source, "status": "draft"},
    )


def write_ancillary_draft(*, province, month, metric, amount_yuan, source_file) -> str:
    return _write_rate_draft(
        table="marketdata.province_ancillary_revenue",
        key_cols={"province": province, "month": month, "metric": metric},
        conflict_target="(province, month, metric, COALESCE(source_file, ''))",
        update_cols=["amount_yuan", "status"],
        payload={"province": province, "month": month, "metric": metric,
                 "amount_yuan": amount_yuan, "source_file": source_file,
                 "status": "draft"},
    )


def read_storage_pipeline(conn, provinces: Optional[list[str]] = None,
                          metric: Optional[str] = None) -> dict:
    """Latest confirmed value per (province, metric) + history (≤200 rows)."""
    sql = ("SELECT province, as_of_date, metric, value, unit, source, status, notes "
           "FROM marketdata.province_storage_pipeline WHERE status = 'confirmed'")
    params: list = []
    if provinces:
        sql += " AND province = ANY(%s)"; params.append(provinces)
    if metric:
        sql += " AND metric = %s"; params.append(metric)
    sql += " ORDER BY province, metric, as_of_date DESC"
    with conn.cursor() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()
    cols = ["province", "as_of_date", "metric", "value", "unit", "source", "status", "notes"]
    recs = [dict(zip(cols, (str(v) if c == "as_of_date" else (float(v) if c == "value" and v is not None else v)
                            for c, v in zip(cols, r)))) for r in rows]
    seen, latest = set(), []
    for r in recs:
        k = (r["province"], r["metric"])
        if k not in seen:
            seen.add(k); latest.append(r)
    return {"count": len(recs), "latest": latest, "history": recs[:200]}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/knowledge_pool/test_intake_routes.py -v`
Expected: 5 PASS. Full suite: `python -m pytest tests/knowledge_pool/ -v` — all PASS.

- [ ] **Step 5: Commit**

```bash
git add services/knowledge_pool/intake_routes.py tests/knowledge_pool/test_intake_routes.py
git commit -m "Add intake route writers with confirmed-row skip guards

Co-Authored-By: Claude Code <noreply@anthropic.com>"
```

---

### Task 5: `get_storage_pipeline` Strategist tool

**Files:**
- Modify: `apps/spot-market/app.py` — tool list (~line 3068, after `get_market_fundamentals` schema), dispatch (~line 3171), `_SPOT_AGENT_BASE_SYSTEM` analytical-framework section
- Test: `tests/knowledge_pool/test_read_pipeline.py` (function-level; the tool wrapper is thin)

**Interfaces:**
- Consumes: `intake_routes.read_storage_pipeline(conn, provinces, metric)` (Task 4); app's `_conn()` helper.
- Produces: tool name `get_storage_pipeline`; inputs `{provinces?: string[], metric?: string}`; output `{"count", "latest", "history"}` (dates ISO strings, values floats).

- [ ] **Step 1: Write the failing test**

`read_storage_pipeline` is already fully tested in Task 4 — this task's test pins the wrapper contract the agent relies on:

```python
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/knowledge_pool/test_read_pipeline.py -v`
Expected: PASS already for empty case (implemented in Task 4) / FAIL only if Task 4 shape deviates — this test locks the contract. If both pass immediately, keep them as regression guards and move on (note: this is the one task where TDD's red step may be green by construction).

- [ ] **Step 3: Wire the tool in app.py**

In the tool schemas (after the `get_market_fundamentals` block, ~line 3091):

```python
        {
            "name": "get_storage_pipeline",
            "description": (
                "Fetch per-province energy-storage pipeline statistics collected from market "
                "intel: installed capacity (新型储能/电网侧储能 GW), project registry "
                "(在库 项目数/GW/GWh), filed-but-not-registered (备案未入库), planning "
                "targets and gaps (规划缺口/接入缺口). Returns latest value per metric "
                "plus dated history. Use for 储能装机/在库/备案/规划/缺口 questions."
            ),
            "input_schema": {
                "type": "object",
                "properties": {
                    "provinces": {
                        "type": "array", "items": {"type": "string"},
                        "description": "Chinese province names, e.g. ['宁夏','甘肃']. Omit for all.",
                    },
                    "metric": {
                        "type": "string",
                        "description": "Optional single metric filter, e.g. registry_gw.",
                    },
                },
                "required": [],
            },
        },
```

In the dispatch chain (after `elif name == "get_market_fundamentals":`, ~line 3172):

```python
            elif name == "get_storage_pipeline":
                from services.knowledge_pool.intake_routes import read_storage_pipeline as _rsp
                result = _rsp(_conn(), inputs.get("provinces"), inputs.get("metric"))
```

In `_SPOT_AGENT_BASE_SYSTEM`'s analytical framework, add one line alongside the existing question→tool mappings:

```
- 储能装机/在库/备案/规划缺口/接入缺口 → get_storage_pipeline（先查表，不足再 search_reference_docs）
```

- [ ] **Step 4: Verify**

Run: `python -m pytest tests/knowledge_pool/ -v` — all PASS.
Smoke (syntax + import): `python -c "import ast; ast.parse(open('apps/spot-market/app.py').read())"` — no output.

- [ ] **Step 5: Commit**

```bash
git add apps/spot-market/app.py tests/knowledge_pool/test_read_pipeline.py
git commit -m "Add get_storage_pipeline tool to Strategist

Co-Authored-By: Claude Code <noreply@anthropic.com>"
```

---

### Task 6: Intel Intake UI in the Knowledge tab

**Files:**
- Modify: `apps/spot-market/app.py` — KB expander tabs (~line 3802), i18n dicts (en ~line 160, zh ~line 471)
- Test: manual via Task 7 fixture (Streamlit widget flow; logic already unit-tested in Tasks 2–4)

**Interfaces:**
- Consumes: `intake_extract.{Page, extract_batch, PROVINCES, IMAGE_EXTS, IntakeProposal}` (Task 3); `knowledge_docs.{ingest_document_batch, get_conn, CATEGORY_LABELS_ZH}` (Tasks 1–2); `intake_routes` writers (Task 4); existing `_extract_pages(file_bytes, filename, api_key=...)` for text-doc pages.
- Produces: session-state keys `intake_proposal`, `intake_route_outcomes`; KB docs with `app='strategist'`, `province` set; route rows per §5 of the spec.

- [ ] **Step 1: Add i18n keys**

In the **en** dict (~line 160) and **zh** dict (~line 471), add (mirroring existing `kb_*` key style):

```python
        # en
        "intake_tab":            "🧠 Intel Intake",
        "intake_caption":        "Drop a whole intel batch (any number of images/docs of ONE topic). Claude reads every page, proposes classification + data routes; you review and commit.",
        "intake_upload_label":   "Intel files (images / pdf / pptx / docx / txt)",
        "intake_extract_btn":    "Extract & propose",
        "intake_commit_btn":     "Commit to knowledge base",
        "intake_retry_btn":      "Retry failed routes",
        "intake_dup_warn":       "This batch (identical content) is already in the KB as doc #{doc_id}《{title}》. Tick to ingest anyway.",
        "intake_dup_anyway":     "Ingest anyway",
        "intake_success":        "Committed as doc #{doc_id}. Routes: {report}",
        "intake_extract_fail":   "Extraction failed: {err}",
        "intake_routes_label":   "Proposed routes (uncheck to skip; click text to edit)",
```
```python
        # zh
        "intake_tab":            "🧠 情报录入",
        "intake_caption":        "整批拖入同一主题的情报文件（图片数量不限，可混合文档）。Claude 逐页阅读后给出分类与数据路由建议，你确认后一键入库。",
        "intake_upload_label":   "情报文件（图片 / pdf / pptx / docx / txt）",
        "intake_extract_btn":    "提取并生成建议",
        "intake_commit_btn":     "确认入库",
        "intake_retry_btn":      "重试失败路由",
        "intake_dup_warn":       "该批次（内容完全相同）已存在于知识库：文档 #{doc_id}《{title}》。勾选后仍可强制入库。",
        "intake_dup_anyway":     "强制入库",
        "intake_success":        "已入库，文档 #{doc_id}。路由结果：{report}",
        "intake_extract_fail":   "提取失败：{err}",
        "intake_routes_label":   "路由建议（取消勾选即跳过；文字可编辑）",
```

- [ ] **Step 2: Add the Intake tab**

Change the tabs line (~3802) to:

```python
        _kb_up_tab, _kb_url_tab, _kb_intake_tab = st.tabs(
            ["📂 Upload Files", "🌐 Fetch from URL", _t("intake_tab")])
```

Append after the URL tab block, inside the KB expander:

```python
        # ── Intel Intake tab ───────────────────────────────────────────────────
        with _kb_intake_tab:
            from services.knowledge_pool.intake_extract import (
                Page as _IntakePage, extract_batch as _ib_extract,
                PROVINCES as _IB_PROVINCES, IMAGE_EXTS as _IB_IMG_EXTS,
            )
            from services.knowledge_pool import intake_routes as _ir

            st.caption(_t("intake_caption"))
            _ib_files = st.file_uploader(
                _t("intake_upload_label"),
                type=["pdf", "pptx", "txt", "docx", "png", "jpg", "jpeg", "webp"],
                accept_multiple_files=True, key="intake_uploader",
            )
            _api_key_ib = _os.environ.get("ANTHROPIC_API_KEY")

            if st.button(_t("intake_extract_btn"), key="intake_extract_btn",
                         disabled=not _ib_files):
                _pages = []
                for _f in _ib_files:
                    _b = _f.read()
                    _ext = _f.name.rsplit(".", 1)[-1].lower()
                    if _ext in _IB_IMG_EXTS:
                        _pages.append(_IntakePage(filename=_f.name, data=_b, kind="image"))
                    else:
                        try:
                            from services.knowledge_pool.knowledge_docs import _extract_pages as _exp
                            _txts = _exp(_b, _f.name, api_key=_api_key_ib)
                            _pages.append(_IntakePage(
                                filename=_f.name, data=_b, kind="text",
                                text="\n".join(t for _, t in _txts)))
                        except Exception as _e:
                            _pages.append(_IntakePage(filename=_f.name, data=_b,
                                                      kind="text", error=str(_e)))
                with st.spinner("Claude reading…"):
                    try:
                        st.session_state["intake_proposal"] = _ib_extract(
                            _pages, api_key=_api_key_ib)
                        st.session_state.pop("intake_route_outcomes", None)
                    except Exception as _e:
                        st.error(_t("intake_extract_fail", err=_e))

            _prop = st.session_state.get("intake_proposal")
            if _prop:
                # duplicate-batch guard
                from services.knowledge_pool.knowledge_docs import get_conn as _kb_get_conn
                _dup = None
                try:
                    with _kb_get_conn() as _c:
                        with _c.cursor() as _cur:
                            _cur.execute(
                                "SELECT id, title FROM staging.spot_knowledge_docs WHERE file_hash = %s",
                                (_prop.batch_hash,))
                            _dup = _cur.fetchone()
                except Exception:
                    pass
                _anyway = False
                if _dup:
                    st.warning(_t("intake_dup_warn", doc_id=_dup[0], title=_dup[1]))
                    _anyway = st.checkbox(_t("intake_dup_anyway"), key="intake_anyway")

                _ib_title = st.text_input("Title", _prop.title, key="intake_title")
                _prov_opts = ["—"] + _IB_PROVINCES
                _ib_prov = st.selectbox("Province", _prov_opts,
                                        index=(_prov_opts.index(_prop.province)
                                               if _prop.province in _prov_opts else 0),
                                        key="intake_province")
                _cat_keys = list(_KB_CATS.keys())
                _ib_cat = st.selectbox(
                    "Category", _cat_keys,
                    index=_cat_keys.index(_prop.category) if _prop.category in _cat_keys else len(_cat_keys) - 1,
                    format_func=lambda k: f"{k} — {_KB_CATS.get(k, k)}",
                    key="intake_category")
                _ib_summary = st.text_area("Summary", _prop.summary, key="intake_summary")

                st.markdown(f"**{_t('intake_routes_label')}**")
                _sel_routes = []
                for _i, _r in enumerate(_prop.routes):
                    _c1, _c2 = st.columns([1, 11])
                    _on = _c1.checkbox("✓", value=True, key=f"intake_route_on_{_i}")
                    _txt = _c2.text_area(f"`{_r.type}` · {_r.province or '—'}",
                                         _r.content, key=f"intake_route_txt_{_i}")
                    if _on:
                        _sel_routes.append((_r, _txt))

                if st.button(_t("intake_commit_btn"), key="intake_commit_btn",
                             disabled=bool(_dup) and not _anyway):
                    from services.knowledge_pool.knowledge_docs import (
                        ingest_document_batch as _idb)
                    _doc_id, _is_new, _ = _idb(
                        _prop.pages,
                        file_name=_ib_title or _prop.title,
                        file_hash=_prop.batch_hash,
                        title=_ib_title, category=_ib_cat,
                        province=None if _ib_prov == "—" else _ib_prov,
                        app="strategist", api_key=_api_key_ib,
                    )
                    _outcomes = _commit_routes(_sel_routes, _doc_id, _ib_prov, _ib_title)
                    st.session_state["intake_route_outcomes"] = _outcomes
                    st.session_state["intake_last_commit"] = (
                        _doc_id, _ib_prov, _ib_title, _sel_routes)
                    if not any(o[1] == "failed" for o in _outcomes):
                        st.session_state.pop("intake_proposal", None)
                    st.success(_t("intake_success", doc_id=_doc_id,
                                  report="; ".join(f"{t}:{s}" for t, s, _ in _outcomes)))

                _outcomes = st.session_state.get("intake_route_outcomes")
                if _outcomes and any(o[1] == "failed" for o in _outcomes):
                    for _t_, _s_, _d_ in _outcomes:
                        (st.error if _s_ == "failed" else st.caption)(f"{_t_}: {_s_} {_d_}")
                    if st.button(_t("intake_retry_btn"), key="intake_retry_btn"):
                        _doc_id2, _prov2, _title2, _routes2 = st.session_state["intake_last_commit"]
                        _failed_pairs = [(r, txt) for (r, txt), (t, s, d)
                                         in zip(_routes2, _outcomes) if s == "failed"]
                        _new = _commit_routes(_failed_pairs, _doc_id2, _prov2, _title2)
                        _merged, _ni = [], iter(_new)
                        for o in _outcomes:
                            _merged.append(next(_ni) if o[1] == "failed" else o)
                        st.session_state["intake_route_outcomes"] = _merged
                        if not any(o[1] == "failed" for o in _merged):
                            st.session_state.pop("intake_proposal", None)
                        st.rerun()
```

Write `_commit_routes` as a module-level function in app.py near the KB helpers:

```python
def _commit_routes(sel_routes, doc_id: int, province: str | None, title: str) -> list[tuple[str, str, str]]:
    """Fire each selected route writer; return [(type, status, detail)]. Isolated per route."""
    from services.knowledge_pool import intake_routes as _ir
    outcomes = []
    for r, content in sel_routes:
        try:
            if r.type == "pipeline_stat":
                n = _ir.write_pipeline_rows(
                    r.structured.get("rows", []),
                    province=r.province or province, source_doc_id=doc_id)
                outcomes.append((r.type, "ok", f"{n} rows"))
            elif r.type == "spot_note":
                a = _ir.upsert_memory_note(app="spot_market",
                                           subject=f"{r.province or province} — {title[:40]}",
                                           content=content)
                outcomes.append((r.type, "ok", a))
            elif r.type == "quant_note":
                a = _ir.upsert_memory_note(app="bess_map",
                                           subject=f"{r.province or province} — {title[:40]}",
                                           content=content)
                outcomes.append((r.type, "ok", a))
            elif r.type == "capacity_comp_rate":
                s = _ir.write_capcomp_draft(
                    province=r.province or province,
                    effective_date=r.structured["effective_date"],
                    cap_comp_yuan_kw=r.structured.get("cap_comp_yuan_kw"),
                    peak_duration_hours=r.structured.get("peak_duration_hours"),
                    source=f"intake:{title[:60]}")
                outcomes.append((r.type, "ok", s))
            elif r.type == "fr_market_params":
                s = _ir.write_fr_market_draft(
                    province=r.province or province,
                    effective_date=r.structured["effective_date"],
                    fr_price_yuan_kw_h=r.structured.get("fr_price_yuan_kw_h"),
                    fr_pool_billion_yuan=r.structured.get("fr_pool_billion_yuan"),
                    source=f"intake:{title[:60]}")
                outcomes.append((r.type, "ok", s))
            elif r.type == "ancillary_revenue":
                s = _ir.write_ancillary_draft(
                    province=r.province or province,
                    month=r.structured["month"],
                    metric=r.structured.get("metric", "调频收入"),
                    amount_yuan=r.structured["amount_yuan"],
                    source_file=f"intake:{title[:60]}")
                outcomes.append((r.type, "ok", s))
        except Exception as exc:
            outcomes.append((r.type, "failed", str(exc)[:200]))
    return outcomes
```

- [ ] **Step 3: Verify**

`python -c "import ast; ast.parse(open('apps/spot-market/app.py').read())"` — no output.
Full unit suite: `python -m pytest tests/knowledge_pool/ -v` — all PASS.
Local launch check (does not commit anything): load `config/.env`, `streamlit run apps/spot-market/app.py --server.port 8505`, open the Knowledge expander → Intel Intake tab renders, uploader accepts multiple images.

- [ ] **Step 4: Commit**

```bash
git add apps/spot-market/app.py
git commit -m "Add Intel Intake tab to spot-market Knowledge Base

Co-Authored-By: Claude Code <noreply@anthropic.com>"
```

---

### Task 7: Live fixture — the 9-image 宁夏 deck

**Files:** none (verification only; produces DB rows)

**Interfaces:**
- Consumes: everything above; fixture files at `/Users/chenzhuqi/Library/Mobile Documents/com~apple~CloudDocs/IMG_382{8,9}.jpg` and `IMG_383{0..6}.jpg`.

- [ ] **Step 1: Run the app locally and extract**

Load `config/.env`, `streamlit run apps/spot-market/app.py --server.port 8505`. In Knowledge → 🧠 情报录入： select all 9 images → 提取并生成建议.
**Expected proposal:** category `capacity_pipeline` or `market_intel`; province 宁夏； a `pipeline_stat` route whose rows include `registry_gw=19.93`, `planning_gap_gw=11.46`, `installed_new_storage_gw=10.13 (2026-05-30)`; a `spot_note` (oversupply/缺口 interpretation); a `quant_note` (AGC 17.65~35.3万kW → 调频收入空间受限）; **zero** rate routes (deck has no compensation rates). If the category is wrong, fix it in the dropdown (this is the designed review path, not a failure).

- [ ] **Step 2: Commit and verify DB**

Click 确认入库. Then:

```sql
-- doc + chunks
SELECT id, title, category, province, page_count FROM staging.spot_knowledge_docs
ORDER BY id DESC LIMIT 1;
SELECT count(*) FROM staging.spot_knowledge_chunks WHERE doc_id = <doc_id>;   -- expect ≥ 9

-- pipeline rows with backlink
SELECT metric, value, unit, as_of_date, source FROM marketdata.province_storage_pipeline
WHERE source_doc_id = <doc_id> ORDER BY metric, as_of_date;
-- spot-check: registry_gw 19.93 | planning_gap_gw 11.46 | installed_new_storage_gw 10.13 @2026-05-30
--             target_gw 30 and 20 @2030-12-31 (two 口径 rows)

-- agent memory notes (both apps)
SELECT app, category, subject, left(content, 60) FROM marketdata.agent_memory
WHERE source = 'intake' ORDER BY id DESC LIMIT 4;   -- expect spot_market + bess_map, category province_note

-- no rate drafts from this deck
SELECT count(*) FROM marketdata.province_ancillary_revenue WHERE source_file LIKE 'intake:%';  -- expect 0
```

- [ ] **Step 3: Strategist end-to-end**

In the Strategist tab ask: `宁夏独立储能在库规模多大？规划缺口多少？`
**Expected:** a `get_storage_pipeline` tool call (visible in the tool expander) and an answer citing 19.93GW / 11.46GW from the table — not from training knowledge.

- [ ] **Step 4: Dedup check**

Re-upload the same 9 images → expect the duplicate warning naming the doc id from Step 2; do NOT tick 强制入库.

- [ ] **Step 5: Commit any fixes surfaced by the fixture run**

```bash
git add <only files actually fixed>
git commit -m "Fix issues found in Ningxia-deck intake fixture run

Co-Authored-By: Claude Code <noreply@anthropic.com>"
```

---

## Self-Review Notes

- **Spec coverage:** taxonomy (T1), province column (T1), batch ingest + dedup (T2), extractor incl. >20 grouping (T3), all six routes + skip guards + sysopfee-as-note (T4/T6 via quant_note), pipeline table + read path (T4/T5), UI review flow (T6), fixture verification incl. Strategist E2E (T7). Out-of-scope items (hermes, bess-map, sysopfee table, chart UI) untouched.
- **Type consistency:** `RouteProposal(type, province, content, structured)` — UI consumes `.structured["rows"]` / `["effective_date"]` / `["month"]` / `["amount_yuan"]` exactly as the extractor prompt emits them; writers' kwargs match `_commit_routes` calls.
- **Known seams:** `_client` injection (extractor), fake-conn patterns (writers/batch), `_TABLES_INITIALIZED` reset in tests.
