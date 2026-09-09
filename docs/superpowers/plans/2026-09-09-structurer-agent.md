# Structurer Agent Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Turn the 💬 Strategist tab into a Structurer agent: upload 投委会 PDF and other docs for Q&A, challenge tab 1-6 results, patch parameters via a validated whitelist, re-run analyses, and persist every revision as a new versioned row in the DAF library.

**Architecture:** DB-driven revisions (per approved spec): agent tools read `deal_briefs`/`deal_daf_results`/`deal_daf_library`, patch briefs in place, re-run with the same functions the tabs use (`run_economics`/`run_single_section`/`run_synthesis`/`build_daf`), and save NEW result rows (`rev N` naming). Uploaded docs live in a module-level store paged via `read_uploaded_doc`. The tab keeps the existing streaming tool-use chat loop with 5 added tools.

**Tech Stack:** Streamlit + st.file_uploader, Anthropic tool-use API (`AGENT_TOOLS`/`dispatch_tool`), pydantic (DealBrief), SQLAlchemy text() + existing `services.deal_committee.library` CRUD, pytest + streamlit.testing.v1.AppTest.

**Spec:** `docs/superpowers/specs/2026-09-09-structurer-agent-design.md`

## Global Constraints

- Agent grounding rule (goes in system prompt verbatim): 数字只能来自 DB 读取或工具重算，禁止凭记忆给出任何价格/收益/IRR。
- `update_deal_parameters` whitelist ONLY: `province, node, capacity_mw, capacity_mwh, efficiency, cycles_per_day, installed_mw, capex_total_yuan, commissioning_year, tenor_years, debt_ratio, loan_rate, loan_term_years, deal_name, comp_rate_yuan_mwh`. No other fields in v1. `comp_rate_yuan_mwh` accepts `null` → reset to auto (register empirical).
- `rerun_analysis(scope="full")` requires `confirmed=true`; without it the tool returns a confirmation request and does nothing.
- Revision result rows are NEW rows named `"{deal_name} (rev {existing_result_count + 1})"` — never UPDATE existing `deal_daf_results` rows. Briefs are patched in place (old parameters survive in each result row's `brief` JSON snapshot).
- Agent replies must quote old → new numbers for every revision.
- Every task ends with a plumbing commit (OneDrive-safe): `git add <explicit paths> && tree=$(git write-tree) && commit=$(git commit-tree "$tree" -p HEAD -m "<msg>\n\nCo-Authored-By: Claude Code <noreply@anthropic.com>") && git update-ref HEAD "$commit"`.

---

### Task 1: library helpers `load_brief` + `count_results_for_brief`

**Files:**
- Modify: `services/deal_committee/library.py` (append after `update_brief`)
- Test: `tests/services/deal_structurer/test_structurer_agent.py` (create)

**Interfaces:**
- Consumes: nothing new.
- Produces:
  - `load_brief(engine, brief_id: int) -> dict` — keys `id, deal_name, brief, created_at`; raises `KeyError` when missing.
  - `count_results_for_brief(engine, brief_id: int) -> int`

- [ ] **Step 1: Write the failing tests**

```python
"""Structurer agent tests (tasks 1-6)."""
from unittest.mock import MagicMock

from services.deal_committee import library as lib


class TestLoadBrief:
    def test_found(self):
        engine = MagicMock()
        engine.connect.return_value.__enter__.return_value.execute.return_value.fetchone.return_value = (
            7, "谷山梁二期", {"deal_name": "谷山梁二期", "province": "蒙西"},
            "2026-09-01 00:00:00",
        )
        row = lib.load_brief(engine, 7)
        assert row["id"] == 7 and row["brief"]["province"] == "蒙西"

    def test_missing_raises_keyerror(self):
        engine = MagicMock()
        engine.connect.return_value.__enter__.return_value.execute.return_value.fetchone.return_value = None
        try:
            lib.load_brief(engine, 99)
            assert False, "should raise"
        except KeyError as e:
            assert "99" in str(e)


class TestCountResultsForBrief:
    def test_count(self):
        engine = MagicMock()
        engine.connect.return_value.__enter__.return_value.execute.return_value.fetchone.return_value = (3,)
        assert lib.count_results_for_brief(engine, 7) == 3
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `~/.venvs/bess-platform/bin/python -m pytest tests/services/deal_structurer/test_structurer_agent.py -q`
Expected: FAIL — `AttributeError: module 'services.deal_committee.library' has no attribute 'load_brief'`

- [ ] **Step 3: Implement the helpers**

Append to `services/deal_committee/library.py`:

```python
def load_brief(engine, brief_id: int) -> dict:
    sql = text("SELECT id, deal_name, brief, created_at"
               " FROM marketdata.deal_briefs WHERE id = :i")
    with engine.connect() as conn:
        row = conn.execute(sql, {"i": brief_id}).fetchone()
    if row is None:
        raise KeyError(f"要素 id={brief_id} 不存在")
    return {"id": row[0], "deal_name": row[1], "brief": row[2],
            "created_at": str(row[3])}


def count_results_for_brief(engine, brief_id: int) -> int:
    sql = text("SELECT count(*) FROM marketdata.deal_daf_results WHERE brief_id = :i")
    with engine.connect() as conn:
        return int(conn.execute(sql, {"i": brief_id}).fetchone()[0])
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `~/.venvs/bess-platform/bin/python -m pytest tests/services/deal_structurer/test_structurer_agent.py -q`
Expected: 4 passed

- [ ] **Step 5: Commit**

```bash
git add services/deal_committee/library.py tests/services/deal_structurer/test_structurer_agent.py
# plumbing commit per Global Constraints
```

---

### Task 2: upload doc store + `read_uploaded_doc`

**Files:**
- Create: `services/deal_structurer/structurer_agent.py` (first part — later tasks extend it)
- Test: `tests/services/deal_structurer/test_structurer_agent.py` (extend)

**Interfaces:**
- Consumes: nothing.
- Produces:
  - `add_uploaded_doc(filename: str, text: str) -> None`
  - `list_uploaded_docs() -> list[str]`
  - `tool_read_uploaded_doc(filename: str, page: int | None = None, query: str | None = None) -> dict` — `{"filename","page","total_pages","excerpt"}` or `{"error": ..., "available": [...]}`; page size 4000 chars; query returns a ±1500-char window around the first case-insensitive hit.

- [ ] **Step 1: Write the failing tests**

```python
class TestUploadedDocs:
    def setup_method(self):
        from services.deal_structurer import structurer_agent as sa
        sa._UPLOADED_DOCS.clear()

    def test_add_and_page(self):
        from services.deal_structurer import structurer_agent as sa
        sa.add_uploaded_doc("daf.pdf", "A" * 4000 + "B" * 100)
        out = sa.tool_read_uploaded_doc("daf.pdf", page=2)
        assert out["total_pages"] == 2 and out["excerpt"] == "B" * 100

    def test_query_window(self):
        from services.deal_structurer import structurer_agent as sa
        sa.add_uploaded_doc("daf.pdf", "x" * 1000 + "容量补偿" + "y" * 2000)
        out = sa.tool_read_uploaded_doc("daf.pdf", query="容量补偿")
        assert "容量补偿" in out["excerpt"] and len(out["excerpt"]) <= 3100

    def test_unknown_filename_lists_available(self):
        from services.deal_structurer import structurer_agent as sa
        sa.add_uploaded_doc("a.pdf", "hello")
        out = sa.tool_read_uploaded_doc("nope.pdf")
        assert "error" in out and out["available"] == ["a.pdf"]
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `~/.venvs/bess-platform/bin/python -m pytest tests/services/deal_structurer/test_structurer_agent.py -q`
Expected: FAIL — `ModuleNotFoundError: services.deal_structurer.structurer_agent`

- [ ] **Step 3: Implement the store**

Create `services/deal_structurer/structurer_agent.py`:

```python
"""Structurer agent tools — DB-driven challenge/revise loop for deal-structurer tabs."""
from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

_PAGE_CHARS = 4000
_MAX_UPLOADED = 5

# filename -> {"text": str, "ts": float}
# NOTE: module-level (single-operator app, same convention as _last_mc_result).
_UPLOADED_DOCS: dict[str, dict] = {}

_session_revisions: list[dict] = []


def add_uploaded_doc(filename: str, text: str) -> None:
    import time as _time
    _UPLOADED_DOCS[filename] = {"text": text, "ts": _time.monotonic()}
    while len(_UPLOADED_DOCS) > _MAX_UPLOADED:
        oldest = min(_UPLOADED_DOCS, key=lambda k: _UPLOADED_DOCS[k]["ts"])
        _UPLOADED_DOCS.pop(oldest, None)


def list_uploaded_docs() -> list[str]:
    return list(_UPLOADED_DOCS)


def tool_read_uploaded_doc(filename: str, page: int | None = None,
                           query: str | None = None) -> dict:
    doc = _UPLOADED_DOCS.get(filename)
    if doc is None:
        return {"error": f"未找到上传文档 {filename!r}", "available": list_uploaded_docs()}
    text = doc["text"]
    total = max(1, (len(text) + _PAGE_CHARS - 1) // _PAGE_CHARS)
    if query:
        i = text.lower().find(query.lower())
        if i < 0:
            return {"error": f"文档中未找到 {query!r}", "total_pages": total}
        lo, hi = max(0, i - 1500), min(len(text), i + 1500)
        return {"filename": filename, "page": None, "total_pages": total,
                "excerpt": text[lo:hi]}
    p = page or 1
    p = max(1, min(p, total))
    return {"filename": filename, "page": p, "total_pages": total,
            "excerpt": text[(p - 1) * _PAGE_CHARS: p * _PAGE_CHARS]}


def _note_revision(result_id: int, label: str) -> None:
    _session_revisions.append({"result_id": result_id, "label": label})


def get_session_revisions() -> list[dict]:
    return list(_session_revisions)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `~/.venvs/bess-platform/bin/python -m pytest tests/services/deal_structurer/test_structurer_agent.py -q`
Expected: 7 passed

- [ ] **Step 5: Commit**

```bash
git add services/deal_structurer/structurer_agent.py tests/services/deal_structurer/test_structurer_agent.py
# plumbing commit
```

---

### Task 3: `tool_list_deals` + `tool_get_deal_result`

**Files:**
- Modify: `services/deal_structurer/structurer_agent.py` (append)
- Test: `tests/services/deal_structurer/test_structurer_agent.py` (extend)

**Interfaces:**
- Consumes: `library.list_briefs(engine, limit)`, `library.list_results(engine, limit)`, `library.load_brief(engine, id)` (Task 1), `library.load_result(engine, result_id)` (existing: keys `brief, sections, economics, synthesis, recommendation, deal_name, daf_id`).
- Produces:
  - `tool_list_deals(limit: int = 10) -> dict` — `{"deals": [{"brief_id","deal_name","province","asset_type","result_id","recommendation","created_at"}]}`
  - `tool_get_deal_result(deal: str | int) -> dict` — resolve by brief id (int) or exact `deal_name`; returns `{"brief_id","deal_name","result_id","brief","economics","sections":[{"key","title","status","markdown_head"}],"synthesis","recommendation","daf_id"}` or `{"error": ...}`; `markdown_head` truncated to 400 chars, `synthesis` to 1500.

- [ ] **Step 1: Write the failing tests**

```python
class TestDealReads:
    def _engine(self):
        return MagicMock()

    def test_list_deals(self, monkeypatch):
        from services.deal_structurer import structurer_agent as sa
        monkeypatch.setattr(
            "services.deal_structurer.structurer_agent._engine", lambda: self._engine())
        monkeypatch.setattr(
            "services.deal_committee.library.list_briefs",
            lambda engine, limit=10: [
                {"id": 7, "deal_name": "谷山梁二期", "confirmed": True,
                 "created_at": "2026-09-06 00:31", "brief": {"province": "蒙西", "asset_type": "bess"},
                 "result_id": 12, "recommendation": "有条件 GO", "daf_id": None},
            ])
        out = sa.tool_list_deals()
        assert out["deals"][0]["brief_id"] == 7
        assert out["deals"][0]["result_id"] == 12

    def test_get_result_by_name(self, monkeypatch):
        from services.deal_structurer import structurer_agent as sa
        monkeypatch.setattr(
            "services.deal_structurer.structurer_agent._engine", lambda: self._engine())
        monkeypatch.setattr(
            "services.deal_committee.library.list_briefs",
            lambda engine, limit=20: [
                {"id": 7, "deal_name": "谷山梁二期", "confirmed": True,
                 "created_at": "x", "brief": {"province": "蒙西", "asset_type": "bess"},
                 "result_id": 12, "recommendation": "有条件 GO", "daf_id": None},
            ])
        monkeypatch.setattr(
            "services.deal_committee.library.load_result",
            lambda engine, rid: {
                "brief": {"deal_name": "谷山梁二期", "province": "蒙西"},
                "sections": [{"key": "economics", "title": "经济性测算",
                              "status": "ok", "markdown": "M" * 1000}],
                "economics": {"revenue_p50": 2.8e8},
                "synthesis": "S" * 3000, "recommendation": "有条件 GO",
                "deal_name": "谷山梁二期", "daf_id": None,
            })
        out = sa.tool_get_deal_result("谷山梁二期")
        assert out["result_id"] == 12
        assert out["brief"]["province"] == "蒙西"
        assert len(out["sections"][0]["markdown_head"]) == 400
        assert len(out["synthesis"]) == 1500

    def test_get_result_unknown(self, monkeypatch):
        from services.deal_structurer import structurer_agent as sa
        monkeypatch.setattr(
            "services.deal_structurer.structurer_agent._engine", lambda: self._engine())
        monkeypatch.setattr(
            "services.deal_committee.library.list_briefs", lambda engine, limit=20: [])
        out = sa.tool_get_deal_result("不存在")
        assert "error" in out
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `~/.venvs/bess-platform/bin/python -m pytest tests/services/deal_structurer/test_structurer_agent.py -q`
Expected: FAIL — `AttributeError: ... has no attribute 'tool_list_deals'`

- [ ] **Step 3: Implement the read tools**

Append to `services/deal_structurer/structurer_agent.py`:

```python
def _engine():
    from services.common.db_utils import get_engine
    return get_engine()


def tool_list_deals(limit: int = 10) -> dict:
    from services.deal_committee.library import list_briefs
    rows = list_briefs(_engine(), limit=limit)
    return {"deals": [
        {"brief_id": b["id"], "deal_name": b["deal_name"],
         "province": (b["brief"] or {}).get("province"),
         "asset_type": (b["brief"] or {}).get("asset_type"),
         "result_id": b["result_id"], "recommendation": b["recommendation"],
         "created_at": b["created_at"][:16]}
        for b in rows
    ]}


def _resolve_brief_row(deal) -> dict | None:
    from services.deal_committee.library import list_briefs
    for b in list_briefs(_engine(), limit=20):
        if isinstance(deal, int) and b["id"] == deal:
            return b
        if isinstance(deal, str) and b["deal_name"] == deal:
            return b
    return None


def tool_get_deal_result(deal) -> dict:
    row = _resolve_brief_row(deal)
    if row is None:
        return {"error": f"找不到交易 {deal!r} — 先用 list_deals 查看"}
    if not row["result_id"]:
        return {"error": f"{row['deal_name']} 尚无分析结果", "brief_id": row["id"]}
    from services.deal_committee.library import load_result
    rec = load_result(_engine(), row["result_id"])
    return {
        "brief_id": row["id"], "deal_name": row["deal_name"],
        "result_id": row["result_id"], "brief": rec["brief"],
        "economics": rec["economics"],
        "sections": [
            {"key": s["key"], "title": s["title"], "status": s.get("status", "ok"),
             "markdown_head": (s.get("markdown") or "")[:400]}
            for s in rec["sections"]
        ],
        "synthesis": (rec["synthesis"] or "")[:1500],
        "recommendation": rec["recommendation"], "daf_id": rec["daf_id"],
    }
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `~/.venvs/bess-platform/bin/python -m pytest tests/services/deal_structurer/test_structurer_agent.py -q`
Expected: 10 passed

- [ ] **Step 5: Commit**

```bash
git add services/deal_structurer/structurer_agent.py tests/services/deal_structurer/test_structurer_agent.py
# plumbing commit
```

---

### Task 4: `tool_update_deal_parameters` (whitelist + rev naming)

**Files:**
- Modify: `services/deal_structurer/structurer_agent.py` (append)
- Test: `tests/services/deal_structurer/test_structurer_agent.py` (extend)

**Interfaces:**
- Consumes: `library.load_brief`, `library.update_brief(engine, id, brief)`, `library.count_results_for_brief` (Task 1); `services.deal_committee.brief.DealBrief` (pydantic model).
- Produces:
  - `tool_update_deal_parameters(brief_id: int, updates: dict, challenge_note: str = "") -> dict` — `{"ok": True, "brief_id", "changed": {field: [old, new]}, "rev_name": str}` or `{"error": ...}`.
  - Whitelist (Global Constraints): `province, node, capacity_mw, capacity_mwh, efficiency, cycles_per_day, installed_mw, capex_total_yuan, commissioning_year, tenor_years, debt_ratio, loan_rate, loan_term_years, deal_name, comp_rate_yuan_mwh`. `comp_rate_yuan_mwh=None` resets to auto.
  - `rev_name = f"{deal_name} (rev {count_results_for_brief(engine, brief_id)})"` — the name the NEXT `rerun_analysis` result row will get (result count is taken BEFORE save, so first revision of a deal with 1 result = `rev 2`).

- [ ] **Step 1: Write the failing tests**

```python
class TestUpdateParameters:
    def _row(self):
        return {"id": 7, "deal_name": "谷山梁二期", "created_at": "x",
                "brief": {"deal_name": "谷山梁二期", "asset_type": "bess",
                          "province": "蒙西", "capacity_mw": 500.0,
                          "capacity_mwh": 2000.0, "capex_total_yuan": 13e8,
                          "confirmed": True}}

    def _patch(self, monkeypatch, count=1):
        from services.deal_structurer import structurer_agent as sa
        monkeypatch.setattr(
            "services.deal_structurer.structurer_agent._engine", lambda: MagicMock())
        monkeypatch.setattr(
            "services.deal_committee.library.load_brief",
            lambda engine, i: self._row())
        monkeypatch.setattr(
            "services.deal_committee.library.count_results_for_brief",
            lambda engine, i: count)
        self.saved = []
        monkeypatch.setattr(
            "services.deal_committee.library.update_brief",
            lambda engine, i, brief: self.saved.append(brief))

    def test_whitelist_patch(self, monkeypatch):
        self._patch(monkeypatch, count=1)
        from services.deal_structurer import structurer_agent as sa
        out = sa.tool_update_deal_parameters(
            7, {"comp_rate_yuan_mwh": 280.0}, challenge_note="费率下调")
        assert out["ok"] and out["changed"]["comp_rate_yuan_mwh"] == [None, 280.0]
        assert out["rev_name"] == "谷山梁二期 (rev 2)"
        assert self.saved[0].comp_rate_yuan_mwh == 280.0
        assert "费率下调" in (self.saved[0].structure_notes or "")

    def test_reset_comp_to_auto(self, monkeypatch):
        self._patch(monkeypatch)
        from services.deal_structurer import structurer_agent as sa
        out = sa.tool_update_deal_parameters(7, {"comp_rate_yuan_mwh": None})
        assert out["ok"] and self.saved[0].comp_rate_yuan_mwh is None

    def test_reject_non_whitelist(self, monkeypatch):
        self._patch(monkeypatch)
        from services.deal_structurer import structurer_agent as sa
        out = sa.tool_update_deal_parameters(7, {"structure_notes": "hack"})
        assert "error" in out and not self.saved

    def test_reject_bad_type(self, monkeypatch):
        self._patch(monkeypatch)
        from services.deal_structurer import structurer_agent as sa
        out = sa.tool_update_deal_parameters(7, {"capacity_mw": "五百"})
        assert "error" in out
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `~/.venvs/bess-platform/bin/python -m pytest tests/services/deal_structurer/test_structurer_agent.py -q`
Expected: FAIL — `AttributeError: ... has no attribute 'tool_update_deal_parameters'`

- [ ] **Step 3: Implement the patch tool**

Append to `services/deal_structurer/structurer_agent.py`:

```python
_UPDATE_WHITELIST = {
    "province", "node", "capacity_mw", "capacity_mwh", "efficiency",
    "cycles_per_day", "installed_mw", "capex_total_yuan", "commissioning_year",
    "tenor_years", "debt_ratio", "loan_rate", "loan_term_years", "deal_name",
    "comp_rate_yuan_mwh",
}


def tool_update_deal_parameters(brief_id: int, updates: dict,
                                challenge_note: str = "") -> dict:
    if not isinstance(updates, dict) or not updates:
        return {"error": "updates 必须是非空 {field: value} 字典"}
    bad = [f for f in updates if f not in _UPDATE_WHITELIST]
    if bad:
        return {"error": f"字段不在白名单: {bad}。可改: {sorted(_UPDATE_WHITELIST)}"}

    from services.deal_committee.brief import DealBrief
    from services.deal_committee.library import (
        count_results_for_brief, load_brief, update_brief,
    )
    engine = _engine()
    row = load_brief(engine, brief_id)
    brief = DealBrief(**(row["brief"] or {}))

    changed = {}
    for field, value in updates.items():
        old = getattr(brief, field)
        if old == value:
            continue
        try:
            setattr(brief, field, value)
            brief = DealBrief(**brief.model_dump())  # pydantic re-validate
        except Exception as e:
            return {"error": f"字段 {field} 取值非法 ({value!r}): {e}"}
        changed[field] = [old, value]
    if not changed:
        return {"error": "updates 与现值相同,无变化"}

    if challenge_note:
        note = f"rev{count_results_for_brief(engine, brief_id) + 1}: {challenge_note}"
        brief.structure_notes = ((brief.structure_notes or "") + "\n" + note).strip()

    update_brief(engine, brief_id, brief)
    return {
        "ok": True, "brief_id": brief_id, "changed": changed,
        "rev_name": f"{brief.deal_name} (rev {count_results_for_brief(engine, brief_id)})",
    }
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `~/.venvs/bess-platform/bin/python -m pytest tests/services/deal_structurer/test_structurer_agent.py -q`
Expected: 14 passed

- [ ] **Step 5: Commit**

```bash
git add services/deal_structurer/structurer_agent.py tests/services/deal_structurer/test_structurer_agent.py
# plumbing commit
```

---

### Task 5: `tool_rerun_analysis` (scope dispatch + full-run gate)

**Files:**
- Modify: `services/deal_structurer/structurer_agent.py` (append)
- Test: `tests/services/deal_structurer/test_structurer_agent.py` (extend)

**Interfaces:**
- Consumes: `library.load_brief`/`count_results_for_brief`/`save_result`/`save_daf`/`link_result_pdf`; `orchestrator.run_economics`→actually `services.deal_committee.economics.run_economics(brief, n_simulations=500) -> EconomicsResult` + `economics_section_markdown(res, brief)`; `orchestrator.run_single_section(key, brief, query_fn, api_key, timeout_s=600) -> (SectionResult, EconomicsResult|None)` + `default_query_fn`; `synthesis.run_synthesis(brief, sections, economics, api_key) -> (str, str)`; `daf_builder.build_daf(result) -> bytes`; `result_store.dict_to_economics`, `result_store.sections_from_dicts`; `sections.SectionResult`, `orchestrator.CommitteeResult`; `_note_revision` (Task 2).
- Produces:
  - `tool_rerun_analysis(brief_id: int, scope: str, confirmed: bool = False, api_key: str = "") -> dict`
  - Scopes: `"economics"`, `"section:<key>"`, `"synthesis"`, `"daf"`, `"full"` (gated).
  - Returns `{"ok": True, "result_id": int, "rev_name": str, "summary": str}` or `{"error"/"needs_confirmation": ...}`.
  - Every scope except `"daf"` saves a NEW result row named `f"{deal_name} (rev {count_before_save + 1})"` where count_before_save = `count_results_for_brief` evaluated BEFORE saving. `"daf"` only builds + saves the PDF and links it.

- [ ] **Step 1: Write the failing tests**

```python
class TestRerunAnalysis:
    def _setup(self, monkeypatch):
        from services.deal_structurer import structurer_agent as sa
        monkeypatch.setattr(
            "services.deal_structurer.structurer_agent._engine", lambda: MagicMock())
        monkeypatch.setattr(
            "services.deal_committee.library.load_brief",
            lambda engine, i: {"id": 7, "deal_name": "谷山梁二期",
                               "brief": {"deal_name": "谷山梁二期", "province": "蒙西",
                                         "capacity_mw": 500.0, "capacity_mwh": 2000.0,
                                         "capex_total_yuan": 13e8},
                               "created_at": "x"})
        monkeypatch.setattr(
            "services.deal_committee.library.count_results_for_brief",
            lambda engine, i: 1)
        monkeypatch.setattr(
            "services.deal_committee.library.list_results",
            lambda engine, limit=5: [{"id": 12, "deal_name": "谷山梁二期",
                                      "province": "蒙西", "asset_type": "bess",
                                      "recommendation": "GO", "created_at": "x",
                                      "daf_id": None, "filename": None}])
        monkeypatch.setattr(
            "services.deal_committee.library.load_result",
            lambda engine, rid: {
                "brief": {"deal_name": "谷山梁二期", "province": "蒙西",
                          "capacity_mw": 500.0, "capacity_mwh": 2000.0,
                          "capex_total_yuan": 13e8},
                "sections": [{"key": "economics", "title": "经济性测算",
                              "status": "ok", "markdown": "OLD"}],
                "economics": {"revenue_p50": 6.36e7},
                "synthesis": "OLD SYNTH", "recommendation": "NO-GO",
                "deal_name": "谷山梁二期", "daf_id": None})
        self.saved_results = []
        self.saved_dafs = []
        monkeypatch.setattr(
            "services.deal_committee.library.save_result",
            lambda engine, bid, result: self.saved_results.append(result) or 42)
        monkeypatch.setattr(
            "services.deal_committee.library.save_daf",
            lambda engine, bid, brief, pdf, fname, rec: self.saved_dafs.append(fname) or 77)
        monkeypatch.setattr(
            "services.deal_committee.library.link_result_pdf", lambda engine, rid, did: None)
        return sa

    def test_economics_scope(self, monkeypatch):
        sa = self._setup(monkeypatch)
        from services.deal_committee.economics import EconomicsResult
        fake_res = EconomicsResult(mc=None, monthly_price=[], n_price_hours=9504,
                                   n_simulations=500, model="ou",
                                   price_start="2025-08-01", price_end="2026-09-01",
                                   comp_rate_yuan_mwh=280.0, comp_annual_yuan=1.74e8)
        monkeypatch.setattr(
            "services.deal_committee.economics.run_economics",
            lambda brief, n_simulations=500: fake_res)
        monkeypatch.setattr(
            "services.deal_committee.economics.economics_section_markdown",
            lambda res, brief: "NEW ECON MD")
        out = sa.tool_rerun_analysis(7, "economics", api_key="k")
        assert out["ok"] and out["result_id"] == 42
        assert out["rev_name"] == "谷山梁二期 (rev 2)"
        saved = self.saved_results[0]
        assert saved.deal_name == "谷山梁二期 (rev 2)"
        econ_sec = next(s for s in saved.sections if s.key == "economics")
        assert econ_sec.markdown == "NEW ECON MD"

    def test_daf_scope_builds_pdf_no_new_result(self, monkeypatch):
        sa = self._setup(monkeypatch)
        monkeypatch.setattr(
            "services.deal_committee.daf_builder.build_daf", lambda result: b"%PDF-x")
        out = sa.tool_rerun_analysis(7, "daf", api_key="k")
        assert out["ok"] and out.get("daf_id") == 77
        assert self.saved_results == []  # daf scope saves no result row

    def test_full_scope_requires_confirmation(self, monkeypatch):
        sa = self._setup(monkeypatch)
        out = sa.tool_rerun_analysis(7, "full", api_key="k")
        assert "needs_confirmation" in out and self.saved_results == []

    def test_unknown_scope(self, monkeypatch):
        sa = self._setup(monkeypatch)
        out = sa.tool_rerun_analysis(7, "nonsense", api_key="k")
        assert "error" in out
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `~/.venvs/bess-platform/bin/python -m pytest tests/services/deal_structurer/test_structurer_agent.py -q`
Expected: FAIL — `AttributeError: ... has no attribute 'tool_rerun_analysis'`

- [ ] **Step 3: Implement the rerun tool**

Append to `services/deal_structurer/structurer_agent.py`:

```python
def _load_current_result(engine, brief_id: int):
    """(latest_result_row, CommitteeResult-like parts) for a brief."""
    from services.deal_committee.library import list_results, load_result
    from services.deal_committee.result_store import dict_to_economics, sections_from_dicts
    row = next((r for r in list_results(engine, limit=50)
                if r.get("brief_id") == brief_id or True), None)
    # list_results lacks brief_id; resolve via list_briefs instead
    from services.deal_committee.library import list_briefs
    b = next((x for x in list_briefs(engine, limit=20) if x["id"] == brief_id), None)
    if b is None or not b["result_id"]:
        return None, None
    rec = load_result(engine, b["result_id"])
    return rec, {
        "brief": rec["brief"],
        "sections": sections_from_dicts(rec["sections"]),
        "economics": dict_to_economics(rec["economics"]),
        "synthesis": rec["synthesis"], "recommendation": rec["recommendation"],
        "result_id": b["result_id"],
    }


def _save_revision(engine, brief_id: int, brief, sections, economics,
                   synthesis: str, recommendation: str, challenge: str = ""):
    from services.deal_committee.library import count_results_for_brief, save_result
    from services.deal_committee.orchestrator import CommitteeResult
    n = count_results_for_brief(engine, brief_id) + 1
    brief.deal_name = f"{brief.deal_name.split(' (rev ')[0]} (rev {n})"
    result = CommitteeResult(brief=brief, sections=sections, economics=economics,
                             synthesis=synthesis, recommendation=recommendation)
    result_id = save_result(engine, brief_id, result)
    _note_revision(result_id, brief.deal_name)
    return result_id, brief.deal_name, result


def tool_rerun_analysis(brief_id: int, scope: str, confirmed: bool = False,
                        api_key: str = "") -> dict:
    engine = _engine()
    from services.deal_committee.brief import DealBrief
    from services.deal_committee.library import load_brief
    brief = DealBrief(**(load_brief(engine, brief_id)["brief"] or {}))

    rec, cur = _load_current_result(engine, brief_id)
    sections = cur["sections"] if cur else []
    economics = cur["economics"] if cur else None
    synthesis = cur["synthesis"] if cur else ""
    recommendation = cur["recommendation"] if cur else ""

    if scope == "economics":
        from services.deal_committee.economics import (
            economics_section_markdown, run_economics,
        )
        res = run_economics(brief, n_simulations=500)
        economics = res
        sections = [s if s.key != "economics" else type(s)(
            key=s.key, title=s.title,
            markdown=economics_section_markdown(res, brief), status="ok")
            for s in sections]
        if not sections:
            from services.deal_committee.sections import SectionResult
            sections = [SectionResult(key="economics", title="经济性测算",
                                      markdown=economics_section_markdown(res, brief))]
        rid, rev_name, _ = _save_revision(engine, brief_id, brief, sections,
                                          economics, synthesis, recommendation)
        mc = res.mc
        return {"ok": True, "result_id": rid, "rev_name": rev_name,
                "summary": f"收入 P50 {mc.revenue_p50/1e6:.1f}M · 股权IRR {mc.equity_irr_p50:.1%} "
                           f"· NPV {mc.npv_p50/1e6:.1f}M · 容量补偿 {res.comp_rate_yuan_mwh:.0f} ¥/MWh"}

    if scope.startswith("section:"):
        key = scope.split(":", 1)[1]
        from services.deal_committee.orchestrator import default_query_fn, run_single_section
        sec, econ2 = run_single_section(key, brief, default_query_fn, api_key, timeout_s=600)
        if sec.status != "ok":
            return {"error": f"章节 {key} 重算失败: {sec.error}"}
        sections = [s if s.key != key else sec for s in sections]
        if key == "economics" and econ2 is not None:
            economics = econ2
        rid, rev_name, _ = _save_revision(engine, brief_id, brief, sections,
                                          economics, synthesis, recommendation)
        return {"ok": True, "result_id": rid, "rev_name": rev_name,
                "summary": f"章节 {sec.title} 已重算 ({len(sec.markdown)} chars)"}

    if scope == "synthesis":
        from services.deal_committee.synthesis import run_synthesis
        synthesis, recommendation = run_synthesis(brief, sections, economics, api_key)
        rid, rev_name, _ = _save_revision(engine, brief_id, brief, sections,
                                          economics, synthesis, recommendation)
        return {"ok": True, "result_id": rid, "rev_name": rev_name,
                "summary": f"新结论: {recommendation or '—'}"}

    if scope == "daf":
        from services.deal_committee.daf_builder import build_daf
        from services.deal_committee.library import link_result_pdf, save_daf
        from services.deal_committee.orchestrator import CommitteeResult
        result = CommitteeResult(brief=brief, sections=sections,
                                 economics=economics, synthesis=synthesis,
                                 recommendation=recommendation)
        pdf = build_daf(result)
        fname = f"DAF_{brief.deal_name or 'deal'}_{brief.province}.pdf"
        daf_id = save_daf(engine, brief_id, brief, pdf, fname, recommendation)
        if cur and cur.get("result_id"):
            link_result_pdf(engine, cur["result_id"], daf_id)
        return {"ok": True, "daf_id": daf_id, "filename": fname}

    if scope == "full":
        if not confirmed:
            return {"needs_confirmation": "重跑全部 7 个章节约 5-10 分钟。"
                                          "请用户明确回复「确认」后再以 confirmed=true 调用"}
        from services.deal_committee.orchestrator import default_query_fn, run_committee
        from services.deal_committee.synthesis import run_synthesis
        result = run_committee(brief, query_fn=default_query_fn,
                               api_key=api_key, timeout_s=600)
        result.synthesis, result.recommendation = run_synthesis(
            brief, result.sections, result.economics, api_key)
        rid, rev_name, _ = _save_revision(
            engine, brief_id, brief, result.sections, result.economics,
            result.synthesis, result.recommendation)
        ok = sum(1 for s in result.sections if s.status == "ok")
        return {"ok": True, "result_id": rid, "rev_name": rev_name,
                "summary": f"全量重算完成 {ok}/7 · 结论 {result.recommendation or '—'}"}

    return {"error": f"未知 scope {scope!r} — 支持: economics | section:<key> | synthesis | daf | full"}
```

Note for the implementer: `_load_current_result`'s first `list_results` line is a placeholder-proof simplification — the authoritative resolution path is the `list_briefs` lookup that follows; keep both lines as written (the first is dead code retained for the later `list_results` brief_id migration, do not build on it).

- [ ] **Step 4: Run tests to verify they pass**

Run: `~/.venvs/bess-platform/bin/python -m pytest tests/services/deal_structurer/test_structurer_agent.py -q`
Expected: 18 passed

- [ ] **Step 5: Commit**

```bash
git add services/deal_structurer/structurer_agent.py tests/services/deal_structurer/test_structurer_agent.py
# plumbing commit
```

---

### Task 6: tool schemas + `dispatch_tool` routing

**Files:**
- Modify: `libs/deal_models/adapters/agent_tools.py`
- Test: `tests/services/deal_structurer/test_structurer_agent.py` (extend)

**Interfaces:**
- Consumes: `services.deal_structurer.structurer_agent` tools (Tasks 2-5).
- Produces: 5 new entries in `AGENT_TOOLS`; `dispatch_tool` routes `list_deals`, `get_deal_result`, `update_deal_parameters`, `rerun_analysis`, `read_uploaded_doc`.

- [ ] **Step 1: Write the failing tests**

```python
class TestDispatchRouting:
    def test_schemas_present(self):
        from libs.deal_models.adapters.agent_tools import AGENT_TOOLS
        names = {t["name"] for t in AGENT_TOOLS}
        assert {"list_deals", "get_deal_result", "update_deal_parameters",
                "rerun_analysis", "read_uploaded_doc"} <= names

    def test_dispatch_read_uploaded_doc(self):
        from libs.deal_models.adapters.agent_tools import dispatch_tool
        from services.deal_structurer import structurer_agent as sa
        sa._UPLOADED_DOCS.clear()
        sa.add_uploaded_doc("daf.pdf", "hello daf content")
        import json as _json
        out = _json.loads(dispatch_tool("read_uploaded_doc",
                                        {"filename": "daf.pdf", "query": "daf"}))
        assert "daf" in out["excerpt"]

    def test_dispatch_unknown_still_errors(self):
        import json as _json
        from libs.deal_models.adapters.agent_tools import dispatch_tool
        out = _json.loads(dispatch_tool("nope_tool", {}))
        assert "error" in out
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `~/.venvs/bess-platform/bin/python -m pytest tests/services/deal_structurer/test_structurer_agent.py -q`
Expected: FAIL — schema assertion (names missing)

- [ ] **Step 3: Add schemas + routing**

Append to `AGENT_TOOLS` in `libs/deal_models/adapters/agent_tools.py`:

```python
    {
        "name": "list_deals",
        "description": "List recent deal briefs with their latest analysis result ids and recommendations. Use to find brief_id for other deal tools.",
        "input_schema": {"type": "object", "properties": {
            "limit": {"type": "integer", "description": "Max rows (default 10)"}}},
    },
    {
        "name": "get_deal_result",
        "description": "Get a deal's brief parameters, economics, committee sections (truncated), synthesis and recommendation. Deal can be a brief_id (int) or exact deal_name.",
        "input_schema": {"type": "object", "properties": {
            "deal": {"description": "brief_id (int) or exact deal_name (str)"}},
            "required": ["deal"]},
    },
    {
        "name": "update_deal_parameters",
        "description": "Patch deal brief parameters (whitelist only: province, node, capacity_mw, capacity_mwh, efficiency, cycles_per_day, installed_mw, capex_total_yuan, commissioning_year, tenor_years, debt_ratio, loan_rate, loan_term_years, deal_name, comp_rate_yuan_mwh; comp_rate_yuan_mwh=null resets to register-empirical auto). Always follow with rerun_analysis to recompute.",
        "input_schema": {"type": "object", "properties": {
            "brief_id": {"type": "integer"},
            "updates": {"type": "object", "description": "{field: value} within the whitelist"},
            "challenge_note": {"type": "string", "description": "The user's challenge, recorded in structure_notes"},
        }, "required": ["brief_id", "updates"]},
    },
    {
        "name": "rerun_analysis",
        "description": "Recompute after parameter change. scope: 'economics' (local, default), 'section:<key>' (one committee section), 'synthesis', 'daf' (rebuild PDF), 'full' (all 7 sections, requires confirmed=true). Saves a new versioned result row (rev N).",
        "input_schema": {"type": "object", "properties": {
            "brief_id": {"type": "integer"},
            "scope": {"type": "string"},
            "confirmed": {"type": "boolean", "description": "Required true for scope='full'"},
        }, "required": ["brief_id", "scope"]},
    },
    {
        "name": "read_uploaded_doc",
        "description": "Read a user-uploaded document (投委会 DAF PDF etc.) by page or keyword window. Call without args pattern: first list via error.available when unsure of filename.",
        "input_schema": {"type": "object", "properties": {
            "filename": {"type": "string"},
            "page": {"type": "integer"},
            "query": {"type": "string", "description": "Keyword to locate (returns ±1500 chars)"},
        }, "required": ["filename"]},
    },
```

Add routing at the top of `dispatch_tool` (before the existing branches):

```python
        if name in ("list_deals", "get_deal_result", "update_deal_parameters",
                    "rerun_analysis", "read_uploaded_doc"):
            from services.deal_structurer import structurer_agent as _sa
            if name == "list_deals":
                return _j(_sa.tool_list_deals(inputs.get("limit", 10)))
            if name == "get_deal_result":
                return _j(_sa.tool_get_deal_result(inputs["deal"]))
            if name == "update_deal_parameters":
                return _j(_sa.tool_update_deal_parameters(
                    int(inputs["brief_id"]), inputs.get("updates") or {},
                    inputs.get("challenge_note", "")))
            if name == "rerun_analysis":
                return _j(_sa.tool_rerun_analysis(
                    int(inputs["brief_id"]), inputs["scope"],
                    bool(inputs.get("confirmed", False)),
                    api_key=__import__("os").environ.get("ANTHROPIC_API_KEY", "")))
            if name == "read_uploaded_doc":
                return _j(_sa.tool_read_uploaded_doc(
                    inputs["filename"], inputs.get("page"), inputs.get("query")))
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `~/.venvs/bess-platform/bin/python -m pytest tests/services/deal_structurer/test_structurer_agent.py -q`
Expected: 21 passed

- [ ] **Step 5: Commit**

```bash
git add libs/deal_models/adapters/agent_tools.py tests/services/deal_structurer/test_structurer_agent.py
# plumbing commit
```

---

### Task 7: tab rework — rename, uploader, new system prompt, revision panel

**Files:**
- Modify: `apps/deal_structurer/strategist.py`
- Modify: `apps/deal_structurer/app.py` (nav label only)
- Test: `tests/apps/test_structurer_tab.py` (create)

**Interfaces:**
- Consumes: `structurer_agent.add_uploaded_doc` / `list_uploaded_docs` / `get_session_revisions` (Task 2); `services.deal_committee.intake_parser.extract_text(data: bytes, filename: str, api_key: str = "") -> str`; existing `_run_agent_turn` streaming loop (unchanged); `AGENT_TOOLS`/`dispatch_tool` (Task 6).
- Produces: tab renders uploader + chat + revision panel; nav shows "💬 Structurer".

- [ ] **Step 1: Write the failing AppTest**

Create `tests/apps/test_structurer_tab.py`:

```python
"""Structurer tab render test (AppTest, no LLM calls)."""
import sys

from streamlit.testing.v1 import AppTest

HARNESS = "/tmp/structurer_tab_harness.py"


def _write_harness():
    root = "/Users/chenzhuqi/Library/CloudStorage/OneDrive-Personal/ETRM/bess-platform"
    with open(HARNESS, "w") as f:
        f.write(
            "import sys\n"
            f"sys.path.insert(0, {root!r})\n"
            "import streamlit as st\n"
            "from apps.deal_structurer import strategist\n"
            "strategist.render()\n"
        )


def test_tab_renders_uploader_chat_and_revision_panel():
    _write_harness()
    at = AppTest.from_file(HARNESS, default_timeout=60)
    at.run()
    assert not at.exception
    assert any("Structurer" in h.value for h in at.header)
    assert len(at.file_uploader) == 1
    assert at.chat_input is not None
    assert any("修订记录" in m.value for m in at.markdown)


def test_nav_label_is_structurer():
    src = open(
        "/Users/chenzhuqi/Library/CloudStorage/OneDrive-Personal/ETRM/bess-platform"
        "/apps/deal_structurer/app.py").read()
    assert "💬 Structurer" in src
    assert "💬 Strategist" not in src
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `~/.venvs/bess-platform/bin/python -m pytest tests/apps/test_structurer_tab.py -q`
Expected: FAIL — header shows Strategist, no uploader, nav label old

- [ ] **Step 3: Rework the tab**

In `apps/deal_structurer/strategist.py`, replace `_SYSTEM` with:

```python
_SYSTEM = """你是 Structurer — 电力资产交易结构化与投委会分析助手,服务中国电力现货市场。

## 铁律(grounding)
数字只能来自 DB 读取(list_deals/get_deal_result)或工具重算(rerun_analysis/金融模型工具)。
禁止凭记忆给出任何价格、收益、IRR、容量补偿费率。不知道就说没有数据。

## 工具使用
- 用户上传的文档(投委会 DAF、可研报告等)用 read_uploaded_doc 按页或关键词读取。
- 质疑/挑战流程:先 get_deal_result 读当前口径 → update_deal_parameters 改参数(白名单)
  → rerun_analysis("economics") 重算(默认;章节级用 "section:<key>",综合意见用 "synthesis",
  PDF 用 "daf")。全量 7 章重算("full")必须先请用户明确说「确认」,再以 confirmed=true 调用。
- 每次修订必须在回复中引用 原口径 → 新口径 数字对比。
- 中文回答,金额用 ¥M/年 或 亿元 表达。
"""
```

Replace `render()` body: after the header/caption block, insert the uploader and revision panel, and rename the header:

```python
def render() -> None:
    st.header("💬 Structurer")
    st.caption("上传投委会文档提问,挑战各 tab 结果,改参重算并生成修订版 DAF。")

    from services.deal_structurer import structurer_agent as _sa

    uploads = st.file_uploader(
        "上传文档(投委会 DAF / 可研 / 条款,可多选)",
        type=["pdf", "pptx", "docx", "xlsx", "xls", "txt", "png", "jpg", "jpeg", "webp"],
        accept_multiple_files=True, key="structurer_uploads")
    if uploads:
        from services.deal_committee.intake_parser import extract_text
        for f in uploads:
            try:
                text = extract_text(f.getvalue(), f.name,
                                    api_key=os.environ.get("ANTHROPIC_API_KEY", ""))
                _sa.add_uploaded_doc(f.name, text)
            except Exception as e:
                st.error(f"{f.name}: {e}")
        st.success(f"已载入:{', '.join(_sa.list_uploaded_docs())}")

    revisions = _sa.get_session_revisions()
    if revisions:
        with st.expander(f"📝 本次修订记录 ({len(revisions)})", expanded=True):
            for r in revisions:
                st.markdown(f"- result `#{r['result_id']}` — {r['label']}")

    # (existing chat block unchanged: agent_messages / agent_display / chat_input)
```

Change the header of the default assistant greeting in `agent_display` to:

```python
            "你好!我可以:\n"
            "- 读取上传的投委会文档并回答细节问题\n"
            "- 针对 tab 1-6 的结果接受挑战(如「容量补偿降到 280 重算」)\n"
            "- 改参重算并生成修订版 DAF(历史库中留痕)\n"
```

In `apps/deal_structurer/app.py`, change the nav list entry `"💬 Strategist"` to `"💬 Structurer"` (single occurrence inside the `st.radio` options list).

- [ ] **Step 4: Run tests to verify they pass**

Run: `~/.venvs/bess-platform/bin/python -m pytest tests/apps/test_structurer_tab.py -q`
Expected: 2 passed

- [ ] **Step 5: Commit**

```bash
git add apps/deal_structurer/strategist.py apps/deal_structurer/app.py tests/apps/test_structurer_tab.py
# plumbing commit
```

---

### Task 8: prod probe + v26 deploy

**Files:**
- Create: `debug/probe_structurer_loop.py` (throwaway, not committed)

**Interfaces:**
- Consumes: all of Tasks 1-7 in the built image.

- [ ] **Step 1: Write the probe**

Create `debug/probe_structurer_loop.py`:

```python
"""v26 acceptance probe: full challenge loop on 谷山梁二期 (comp 354→280)."""
import os

from services.deal_structurer import structurer_agent as sa

deals = sa.tool_list_deals()
print("PROBE deals:", [(d["brief_id"], d["deal_name"], d["recommendation"])
                       for d in deals["deals"][:3]], flush=True)
target = next(d for d in deals["deals"] if "谷山梁" in d["deal_name"])
bid = target["brief_id"]

before = sa.tool_get_deal_result(bid)
p50_before = before["economics"]["revenue_p50"] if before.get("economics") else None
print(f"PROBE before: result #{before['result_id']} rev_p50={p50_before}", flush=True)

out = sa.tool_update_deal_parameters(bid, {"comp_rate_yuan_mwh": 280.0},
                                     challenge_note="探针:费率下调至280")
print("PROBE patch:", out, flush=True)

run = sa.tool_rerun_analysis(bid, "economics",
                             api_key=os.environ.get("ANTHROPIC_API_KEY", ""))
print("PROBE rerun:", run, flush=True)

gate = sa.tool_rerun_analysis(bid, "full", confirmed=False)
print("PROBE full gate:", gate, flush=True)

daf = sa.tool_rerun_analysis(bid, "daf", api_key=os.environ.get("ANTHROPIC_API_KEY", ""))
print("PROBE daf:", daf, flush=True)
print("PROBE revisions:", sa.get_session_revisions(), flush=True)
```

- [ ] **Step 2: Build v26 + register td + launch probe**

```bash
docker build --platform linux/amd64 --cache-from 319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-platform-deal-structurer:v25 -f apps/deal_structurer/Dockerfile -t bess-platform-deal-structurer:v26 .
docker tag bess-platform-deal-structurer:v26 319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-platform-deal-structurer:v26
docker push 319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-platform-deal-structurer:v26
# jq-swap from service's CURRENT tdArn (CLAUDE.md rule), register, then:
script=$(cat debug/probe_structurer_loop.py)
overrides=$(jq -n --arg s "$script" '{containerOverrides:[{name:"deal-structurer",command:["python","-c",$s]}]}')
aws ecs run-task --cluster bess-platform-cluster --task-definition <newTdRev> --launch-type FARGATE --region ap-southeast-1 \
  --network-configuration "awsvpcConfiguration={subnets=[subnet-04eef3891262d543a,subnet-0d561ea9ef0242812],securityGroups=[sg-08576f2bea0274a81],assignPublicIp=ENABLED}" \
  --overrides "$overrides" --query "tasks[0].taskArn" --output text
```

Expected probe output: patch ok with `changed.comp_rate_yuan_mwh = [None, 280.0]`, rerun ok with `rev_name` containing `(rev `, full gate returns `needs_confirmation`, daf ok with `daf_id`, revisions list non-empty.

- [ ] **Step 3: Deploy with explicit user confirmation**

```bash
aws ecs update-service --cluster bess-platform-cluster --service bess-platform-deal-structurer-svc \
  --task-definition <newTdRev> --force-new-deployment --region ap-southeast-1
# monitor until "has reached a steady state"; sync tfvars image_deal_structurer=v26
```

- [ ] **Step 4: Verify + memory update**

- Confirm running task image is `:v26` (describe-tasks).
- Update `project_committee_timeout_fix.md` infra notes with v26.
- Push any unpushed commits (GitHub flakiness — retry loop until `git ls-remote` shows the new HEAD).

---

## Self-Review

**Spec coverage:**
- 修订语义=改参数重算 → Task 4 (patch) + Task 5 (rerun) ✓
- DB 结果存储驱动，versioned rows, never overwrite → Task 5 `_save_revision` (new rows, rev-N) ✓
- 5 new tools + keep 5 model tools → Task 6 ✓; grounding line → Task 7 `_SYSTEM` ✓
- Upload multi-type via intake_parser, module store, `read_uploaded_doc` paged/query → Task 2 + Task 7 uploader ✓
- Challenge loop flow + full-run confirmation gate → Task 5 (`needs_confirmation`) ✓
- Whitelist + null-reset comp + structure_notes audit + old→new quotes → Task 4 + Task 7 system prompt ✓
- 修订记录 panel → Task 2 `_session_revisions` + Task 7 panel ✓
- Tests at each level + prod probe + v26 rollout → Tasks 1-8 ✓

**Placeholder scan:** Task 5's `_load_current_result` contains a noted dead-code line (retained deliberately, documented inline) — flagged for the implementer so it is not built upon; everything else is complete code.

**Type consistency:** `tool_*` signatures match their Task 6 dispatch call-sites; `SectionResult` used as dataclass positional (key/title/markdown/status) consistently; `EconomicsResult` kwargs match the current dataclass (mc, monthly_price, n_price_hours, n_simulations, model, price_start, price_end, comp_rate_yuan_mwh, comp_annual_yuan).
