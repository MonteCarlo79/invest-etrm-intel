# Deal Committee Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an investment-committee layer to deal-structurer: upload deal docs → extract a confirmed Deal Brief → run a 7-section cross-pillar analysis via the existing hermes headless-agent machinery → synthesize a go/no-go recommendation → generate a Chinese DAF PDF persisted to RDS.

**Architecture:** New pure-Python module `services/deal_committee/` (no Streamlit imports) + two thin Streamlit tabs in `apps/deal_structurer/`. Section analysis reuses `services/hermes/market_agent_bridge.run_market_query` in-process; economics runs the existing `libs/deal_models` engine; PDF via reportlab reusing the CJK font pattern from `services/hermes/export_utils.py`.

**Tech Stack:** Python 3.11, pydantic v2, SQLAlchemy 2.x + psycopg2, reportlab, matplotlib (Agg), Streamlit 1.58, pytest 9.

**Spec:** `docs/superpowers/specs/2026-09-04-deal-committee-design.md`

## Global Constraints

- `services/deal_committee/` must NOT import streamlit (same rule as `libs/deal_models` having no I/O).
- All LLM calls via `shared/anthropic_client.make_client`; extraction/synthesis model string: `"claude-sonnet-4-6"`.
- Synthesis and DAF narrative in **Chinese**; chart axis labels in **English** (matplotlib CJK fonts unavailable in slim images).
- Synthesis is grounded: prompt must forbid inventing numbers not present in section outputs.
- `solar` / `solar_bess` map onto the `wind` / `wind_bess` dispatch models (July spec: same model, different profile — v1 accepts this approximation).
- Fixed O&M defaults to ¥3M/yr (matches `cashflow_tab.py` default) — not a brief field in v1.
- Run tests from repo root with the Mac venv: `~/.venvs/bess-platform/bin/python -m pytest services/deal_committee/tests/ -v`
- Git: commit after every task; use explicit paths in `git add`; NEVER stage `infra/terraform/terraform.tfvars`.
- No deployment without the user's explicit in-session "yes".

---

### Task 1: Package scaffold + `brief.py` (DealBrief schema)

**Files:**
- Create: `services/deal_committee/__init__.py` (empty)
- Create: `services/deal_committee/brief.py`
- Test: `services/deal_committee/tests/__init__.py` (empty), `services/deal_committee/tests/test_brief.py`

**Interfaces:**
- Consumes: nothing (pydantic only).
- Produces: `DealBrief` (pydantic model, fields below), `CORE_FIELDS`, `parse_brief_json(payload: dict, source_files: list[str] | None = None) -> DealBrief`, `low_confidence_fields(brief: DealBrief, threshold: float = 0.6) -> list[str]`. Tasks 3, 4, 5, 7, 9, 11, 12 rely on these exact names.

- [ ] **Step 1: Write the failing test**

```python
# services/deal_committee/tests/test_brief.py
from services.deal_committee.brief import DealBrief, parse_brief_json, low_confidence_fields


def test_defaults_are_sane():
    b = DealBrief()
    assert b.asset_type == "bess"
    assert b.confirmed is False
    assert b.efficiency == 0.85
    assert b.debt_ratio == 0.70
    assert b.field_confidence == {}


def test_parse_brief_json_tolerates_missing_and_extra():
    b = parse_brief_json(
        {"deal_name": "蒙西储能一期", "province": "蒙西", "capacity_mw": 100,
         "capacity_mwh": 200, "unknown_field": "ignored",
         "field_confidence": {"province": 0.95, "capacity_mw": 0.3}},
        source_files=["deal.docx"],
    )
    assert b.deal_name == "蒙西储能一期"
    assert b.province == "蒙西"
    assert b.capacity_mw == 100.0
    assert b.source_files == ["deal.docx"]
    assert b.field_confidence["province"] == 0.95


def test_parse_brief_json_coerces_numeric_strings():
    b = parse_brief_json({"capex_total_yuan": "1200000000", "debt_ratio": "0.7"})
    assert b.capex_total_yuan == 1.2e9
    assert b.debt_ratio == 0.7


def test_low_confidence_fields_only_core_below_threshold():
    b = parse_brief_json({"field_confidence": {"province": 0.2, "structure_notes": 0.1,
                                               "capacity_mw": 0.9}})
    low = low_confidence_fields(b, threshold=0.6)
    assert "province" in low
    assert "capacity_mw" not in low
    assert "structure_notes" not in low  # not a core field


def test_asset_type_literal_validated():
    import pytest
    with pytest.raises(Exception):
        DealBrief(asset_type="nuclear")
```

- [ ] **Step 2: Run test to verify it fails**

Run: `~/.venvs/bess-platform/bin/python -m pytest services/deal_committee/tests/test_brief.py -v`
Expected: FAIL — `ModuleNotFoundError: services.deal_committee`

- [ ] **Step 3: Write the implementation**

```python
# services/deal_committee/brief.py
"""DealBrief — confirmed deal parameters driving the committee analysis."""
from __future__ import annotations

from typing import Literal, Optional

from pydantic import BaseModel, Field

# Fields the intake form warns about when extraction confidence is low.
CORE_FIELDS: tuple[str, ...] = (
    "deal_name", "asset_type", "province", "capacity_mw", "capacity_mwh",
    "installed_mw", "capex_total_yuan", "commissioning_year",
)


class DealBrief(BaseModel):
    # Identity
    deal_name: str = ""
    asset_type: Literal["bess", "wind", "solar", "wind_bess", "solar_bess"] = "bess"
    # Site
    province: str = ""
    node: Optional[str] = None
    # Technical
    capacity_mw: float = 0.0
    capacity_mwh: float = 0.0
    efficiency: float = Field(0.85, gt=0, le=1.0)
    cycles_per_day: float = Field(1.0, gt=0)
    installed_mw: float = 0.0
    # Commercial
    capex_total_yuan: Optional[float] = None
    commissioning_year: int = 2027
    tenor_years: int = Field(20, ge=1, le=40)
    counterparty: str = ""
    structure_notes: str = ""
    # Financing
    debt_ratio: float = Field(0.70, ge=0.0, le=0.95)
    loan_rate: float = Field(0.05, ge=0.0, le=0.30)
    loan_term_years: int = Field(10, ge=1, le=30)
    # Meta
    field_confidence: dict[str, float] = Field(default_factory=dict)
    confirmed: bool = False
    source_files: list[str] = Field(default_factory=list)


def parse_brief_json(payload: dict, source_files: list[str] | None = None) -> DealBrief:
    """Tolerant parser for LLM extraction output: unknown keys dropped, missing → defaults."""
    known = {k: v for k, v in (payload or {}).items() if k in DealBrief.model_fields}
    conf = known.pop("field_confidence", {}) or {}
    brief = DealBrief(**known)
    brief.field_confidence = {str(k): float(v) for k, v in conf.items()
                              if isinstance(v, (int, float))}
    if source_files:
        brief.source_files = list(source_files)
    return brief


def low_confidence_fields(brief: DealBrief, threshold: float = 0.6) -> list[str]:
    """Core fields whose extraction confidence is below threshold (or unrecorded-but-empty)."""
    low = [f for f in CORE_FIELDS
           if f in brief.field_confidence and brief.field_confidence[f] < threshold]
    return sorted(low)
```

- [ ] **Step 4: Run tests**

Run: `~/.venvs/bess-platform/bin/python -m pytest services/deal_committee/tests/test_brief.py -v`
Expected: 5 PASS

- [ ] **Step 5: Commit**

```bash
git add services/deal_committee/
git commit -m "Add deal_committee package with DealBrief schema"
```

---

### Task 2: `intake_parser.py` (document → plain text)

**Files:**
- Create: `services/deal_committee/intake_parser.py`
- Test: `services/deal_committee/tests/test_intake_parser.py`

**Interfaces:**
- Consumes: `services.knowledge_pool.knowledge_docs._extract_pages(file_bytes, filename, api_key=None) -> list[tuple[int, str]]` (exists).
- Produces: `SUPPORTED_EXTS`, `MAX_CHARS`, `extract_text(file_bytes: bytes, filename: str, api_key: str | None = None) -> str`. Used by Task 11 (intake tab) and Task 3 tests.

- [ ] **Step 1: Write the failing test**

```python
# services/deal_committee/tests/test_intake_parser.py
import io

import pytest

from services.deal_committee.intake_parser import MAX_CHARS, extract_text


def test_txt_roundtrip():
    text = extract_text("蒙西 100MW/200MWh 储能项目,总投资 12 亿元。".encode("utf-8"), "deal.txt")
    assert "蒙西" in text
    assert "100MW" in text


def test_docx_roundtrip():
    import docx
    doc = docx.Document()
    doc.add_paragraph("山东 200MW 风电项目建议书")
    buf = io.BytesIO()
    doc.save(buf)
    text = extract_text(buf.getvalue(), "proposal.docx")
    assert "山东" in text


def test_unsupported_extension_raises():
    with pytest.raises(ValueError, match="不支持"):
        extract_text(b"MZ", "archive.zip")


def test_empty_content_raises():
    with pytest.raises(ValueError, match="提取"):
        extract_text(b"   ", "empty.txt")


def test_truncates_to_max_chars():
    text = extract_text(("长" * (MAX_CHARS + 5000)).encode("utf-8"), "long.txt")
    assert len(text) == MAX_CHARS
```

- [ ] **Step 2: Run test to verify it fails**

Run: `~/.venvs/bess-platform/bin/python -m pytest services/deal_committee/tests/test_intake_parser.py -v`
Expected: FAIL — `ModuleNotFoundError: services.deal_committee.intake_parser`

- [ ] **Step 3: Write the implementation**

```python
# services/deal_committee/intake_parser.py
"""Turn uploaded deal documents into plain text for brief extraction.

Wraps services.knowledge_pool.knowledge_docs._extract_pages (the same loaders the
knowledge pool uses) and joins pages into one truncated string.
"""
from __future__ import annotations

SUPPORTED_EXTS: tuple[str, ...] = ("docx", "pptx", "pdf", "xlsx", "xls", "txt")
MAX_CHARS = 30_000  # extraction prompt budget


def extract_text(file_bytes: bytes, filename: str, api_key: str | None = None) -> str:
    ext = filename.rsplit(".", 1)[-1].lower() if "." in filename else ""
    if ext not in SUPPORTED_EXTS:
        raise ValueError(f"不支持的文件类型: .{ext}(支持: {', '.join(SUPPORTED_EXTS)})")

    from services.knowledge_pool.knowledge_docs import _extract_pages  # lazy: heavy parsers

    pages = _extract_pages(file_bytes, filename, api_key=api_key)
    text = "\n\n".join(t.strip() for _, t in pages if t and t.strip())
    if not text:
        raise ValueError(f"无法从 {filename} 提取文本(可能是扫描件或无文字内容)")
    return text[:MAX_CHARS]
```

- [ ] **Step 4: Run tests**

Run: `~/.venvs/bess-platform/bin/python -m pytest services/deal_committee/tests/test_intake_parser.py -v`
Expected: 5 PASS. If `services.knowledge_pool.knowledge_docs` fails to import in the venv (missing module-level dep), install that dep into the venv and note it for Task 13's requirements.

- [ ] **Step 5: Commit**

```bash
git add services/deal_committee/intake_parser.py services/deal_committee/tests/test_intake_parser.py
git commit -m "Add intake parser wrapping knowledge pool document loaders"
```

---

### Task 3: `brief.py` LLM extraction (`extract_brief`)

**Files:**
- Modify: `services/deal_committee/brief.py` (append)
- Test: `services/deal_committee/tests/test_extract_brief.py`

**Interfaces:**
- Consumes: `parse_brief_json` (Task 1), `shared.anthropic_client.make_client` (exists).
- Produces: `EXTRACTION_MODEL = "claude-sonnet-4-6"`, `build_extraction_prompt(text: str) -> str`, `extract_brief(text: str, filenames: list[str], api_key: str, client=None) -> DealBrief`. The `client` parameter is the test seam — when provided, `make_client` is not called. Used by Task 11.

- [ ] **Step 1: Write the failing test**

```python
# services/deal_committee/tests/test_extract_brief.py
import json
from types import SimpleNamespace

import pytest

from services.deal_committee.brief import build_extraction_prompt, extract_brief


class _FakeClient:
    """Mimics anthropic client: .messages.create(...) → content[0].text"""
    def __init__(self, text: str):
        self._text = text
    @property
    def messages(self):
        return self
    def create(self, **kwargs):
        return SimpleNamespace(content=[SimpleNamespace(text=self._text)])


def test_prompt_contains_fields_and_text():
    p = build_extraction_prompt("蒙西 100MW/200MWh 储能,总投资 12 亿元")
    for field in ("deal_name", "asset_type", "province", "capex_total_yuan", "field_confidence"):
        assert field in p
    assert "蒙西 100MW/200MWh" in p
    assert "12 亿元" in p


def test_extract_brief_parses_fenced_json():
    payload = {"deal_name": "蒙西储能一期", "province": "蒙西", "asset_type": "bess",
               "capacity_mw": 100, "capacity_mwh": 200, "capex_total_yuan": 1.2e9,
               "field_confidence": {"province": 0.95}}
    client = _FakeClient("```json\n" + json.dumps(payload) + "\n```")
    brief = extract_brief("无关文本", ["deal.docx"], api_key="", client=client)
    assert brief.deal_name == "蒙西储能一期"
    assert brief.province == "蒙西"
    assert brief.capex_total_yuan == 1.2e9
    assert brief.source_files == ["deal.docx"]


def test_extract_brief_invalid_json_raises():
    client = _FakeClient("这不是 JSON")
    with pytest.raises(ValueError, match="JSON"):
        extract_brief("文本", [], api_key="", client=client)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `~/.venvs/bess-platform/bin/python -m pytest services/deal_committee/tests/test_extract_brief.py -v`
Expected: FAIL — `ImportError: cannot import name 'build_extraction_prompt'`

- [ ] **Step 3: Append to `services/deal_committee/brief.py`**

```python
# ── LLM extraction ────────────────────────────────────────────────────────────
import json
import re

EXTRACTION_MODEL = "claude-sonnet-4-6"


def build_extraction_prompt(text: str) -> str:
    return f"""你是一名新能源投资分析师。从以下交易背景材料中提取交易要素,以严格 JSON 返回,不要任何解释。

字段(缺失或不确定时给 null):
- deal_name 项目名称
- asset_type 资产类型:bess 独立储能 / wind 风电 / solar 光伏 / wind_bess 风光储一体 / solar_bess 光储一体
- province 省份(中文,如 蒙西、山东、山西)
- node 并网点/节点(无则 null)
- capacity_mw 储能额定功率(MW);capacity_mwh 储能容量(MWh)
- efficiency 储能综合效率(0-1);cycles_per_day 日均循环次数
- installed_mw 风电/光伏装机(MW)
- capex_total_yuan 总投资(单位:元。注意换算:万元×1e4,亿元×1e8)
- commissioning_year 投运年份(4 位整数);tenor_years 项目期限(年)
- counterparty 对手方/卖方;structure_notes 交易结构要点(PPA/保底/托底/容量租赁等,≤200 字)
- debt_ratio 负债率(0-1);loan_rate 贷款利率(0-1);loan_term_years 贷款期限(年)
- field_confidence 对象:对每个字段给 0-1 置信度

材料:
{text}"""


def extract_brief(text: str, filenames: list[str], api_key: str, client=None) -> DealBrief:
    """Extract a DealBrief from document text. `client` is the test seam (skips make_client)."""
    if client is None:
        from shared.anthropic_client import make_client
        client = make_client(api_key)
    resp = client.messages.create(
        model=EXTRACTION_MODEL, max_tokens=2000,
        messages=[{"role": "user", "content": build_extraction_prompt(text)}],
    )
    raw = resp.content[0].text.strip()
    raw = re.sub(r"^```(?:json)?\s*|\s*```$", "", raw, flags=re.MULTILINE).strip()
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as e:
        raise ValueError(f"LLM 返回的不是有效 JSON: {e}; 原文开头: {raw[:120]}") from e
    if not isinstance(payload, dict):
        raise ValueError(f"LLM 返回的 JSON 不是对象: {raw[:120]}")
    return parse_brief_json(payload, source_files=filenames)
```

- [ ] **Step 4: Run tests**

Run: `~/.venvs/bess-platform/bin/python -m pytest services/deal_committee/tests/ -v`
Expected: all PASS (5 + 3)

- [ ] **Step 5: Commit**

```bash
git add services/deal_committee/brief.py services/deal_committee/tests/test_extract_brief.py
git commit -m "Add LLM deal brief extraction with injectable client"
```

---

### Task 4: `sections.py` (section definitions + question builders)

**Files:**
- Create: `services/deal_committee/sections.py`
- Test: `services/deal_committee/tests/test_sections.py`

**Interfaces:**
- Consumes: `DealBrief` (Task 1).
- Produces:
  - `SectionDef` dataclass `(key: str, title: str, agent: str)` — `agent` is a `market_agent_bridge` key, or `""` for non-agent sections.
  - `SECTION_DEFS: tuple[SectionDef, ...]` — 7 entries in fixed order: `market_background`(spot), `policy`(spot), `economics`(""), `ops_mengxi`(mengxi), `ops_asset_risk`(asset-risk), `ops_retail_risk`(retail-risk), `risk`("").
  - `SectionResult` dataclass `(key: str, title: str, markdown: str = "", status: str = "ok", error: str = "")`.
  - `build_question(key: str, brief: DealBrief) -> str` — raises `KeyError` for `economics`/`risk`.
  - `_asset_desc(brief: DealBrief) -> str`.
  Tasks 6, 7, 9, 12 rely on these.

- [ ] **Step 1: Write the failing test**

```python
# services/deal_committee/tests/test_sections.py
import pytest

from services.deal_committee.brief import DealBrief
from services.deal_committee.sections import SECTION_DEFS, build_question, _asset_desc

BRIEF = DealBrief(deal_name="蒙西储能一期", asset_type="bess", province="蒙西",
                  capacity_mw=100, capacity_mwh=200)


def test_seven_sections_in_order():
    assert [s.key for s in SECTION_DEFS] == [
        "market_background", "policy", "economics",
        "ops_mengxi", "ops_asset_risk", "ops_retail_risk", "risk",
    ]


def test_agent_keys_match_bridge():
    agents = {s.key: s.agent for s in SECTION_DEFS}
    assert agents["market_background"] == "spot"
    assert agents["policy"] == "spot"
    assert agents["ops_mengxi"] == "mengxi"
    assert agents["ops_asset_risk"] == "asset-risk"
    assert agents["ops_retail_risk"] == "retail-risk"
    assert agents["economics"] == "" and agents["risk"] == ""


def test_questions_contain_province_and_asset():
    for key in ("market_background", "policy", "ops_mengxi", "ops_asset_risk", "ops_retail_risk"):
        q = build_question(key, BRIEF)
        assert "蒙西" in q, key
    assert "100" in build_question("market_background", BRIEF)  # capacity in MW


def test_non_agent_sections_raise_keyerror():
    with pytest.raises(KeyError):
        build_question("economics", BRIEF)
    with pytest.raises(KeyError):
        build_question("risk", BRIEF)


def test_asset_desc_variants():
    assert _asset_desc(BRIEF) == "100MW/200MWh 储能"
    w = DealBrief(asset_type="wind", province="山东", installed_mw=200)
    assert _asset_desc(w) == "200MW 风电"
    wb = DealBrief(asset_type="wind_bess", province="山西", installed_mw=150,
                   capacity_mw=50, capacity_mwh=100)
    assert "风电" in _asset_desc(wb) and "储能" in _asset_desc(wb)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `~/.venvs/bess-platform/bin/python -m pytest services/deal_committee/tests/test_sections.py -v`
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: Write the implementation**

```python
# services/deal_committee/sections.py
"""Committee section definitions and per-section question builders."""
from __future__ import annotations

from dataclasses import dataclass

from services.deal_committee.brief import DealBrief


@dataclass(frozen=True)
class SectionDef:
    key: str
    title: str
    agent: str  # market_agent_bridge key; "" = non-agent section (economics / risk)


@dataclass
class SectionResult:
    key: str
    title: str
    markdown: str = ""
    status: str = "ok"  # "ok" | "failed"
    error: str = ""


SECTION_DEFS: tuple[SectionDef, ...] = (
    SectionDef("market_background", "市场背景", "spot"),
    SectionDef("policy", "政策与规则环境", "spot"),
    SectionDef("economics", "经济性测算", ""),
    SectionDef("ops_mengxi", "运营实证 · 蒙西储能", "mengxi"),
    SectionDef("ops_asset_risk", "运营实证 · 资产风险台账", "asset-risk"),
    SectionDef("ops_retail_risk", "运营实证 · 零售风险台账", "retail-risk"),
    SectionDef("risk", "风险数据", ""),
)


def _asset_desc(brief: DealBrief) -> str:
    parts = []
    if brief.asset_type in ("bess", "wind_bess", "solar_bess"):
        parts.append(f"{brief.capacity_mw:g}MW/{brief.capacity_mwh:g}MWh 储能")
    if brief.asset_type in ("wind", "wind_bess"):
        parts.append(f"{brief.installed_mw:g}MW 风电")
    if brief.asset_type in ("solar", "solar_bess"):
        parts.append(f"{brief.installed_mw:g}MW 光伏")
    return " + ".join(parts) or brief.asset_type


def build_question(key: str, brief: DealBrief) -> str:
    """Question sent to the headless agent for this section. KeyError for non-agent sections."""
    asset = _asset_desc(brief)
    node_clause = f",并网点/节点:{brief.node}" if brief.node else ""
    site = f"{brief.province}{node_clause}"
    questions = {
        "market_background": (
            f"作为电力市场分析师,评估{site}电力现货市场对新建{asset}项目的吸引力。"
            "请用数据回答:1) 近12个月日前/实时价格水平与走势;2) 价格波动率与峰谷价差;"
            "3) 省间送受电格局;4) 市场成熟度(结算试运行/正式运行)。中文回答。"
        ),
        "policy": (
            f"梳理{brief.province}电力市场关于{asset}的最新政策与交易规则:"
            "1) 现货市场结算规则要点;2) 独立储能/新能源参与现货与辅助服务的方式;"
            "3) 容量补偿/容量电价机制;4) 未来1-2年的政策风险点。"
            "请检索知识库文档并注明出处,中文回答。"
        ),
        "ops_mengxi": (
            "总结蒙西在运储能电站的实际运营表现:现货捕获率、日均循环次数、等效利用小时、"
            "结算均价水平、主要运营问题;并说明这些实证数据对评估新建"
            f"{asset}项目({brief.province})的参考意义。中文回答。"
        ),
        "ops_asset_risk": (
            f"汇总资产风险台账中与{brief.province}及同类({asset})资产相关的在运项目"
            "结算与 P&L 表现、最新 VaR 水平;如台账中无该省资产,请给出现有组合的基准数据并明确说明。"
            "中文回答。"
        ),
        "ops_retail_risk": (
            f"汇总零售风险台账中{brief.province}售电业务的批零价差、结算与保证金风险表现,"
            "评估该省市场流动性与零售侧价格信号;如该省无零售业务,请明确说明。中文回答。"
        ),
    }
    if key not in questions:
        raise KeyError(f"section {key!r} 不是 agent 章节(economics/risk 在本地生成)")
    return questions[key]
```

- [ ] **Step 4: Run tests**

Run: `~/.venvs/bess-platform/bin/python -m pytest services/deal_committee/tests/test_sections.py -v`
Expected: 5 PASS

- [ ] **Step 5: Commit**

```bash
git add services/deal_committee/sections.py services/deal_committee/tests/test_sections.py
git commit -m "Add committee section definitions and agent question builders"
```

---

### Task 5: `economics.py` (in-process economics section)

**Files:**
- Create: `services/deal_committee/economics.py`
- Test: `services/deal_committee/tests/test_economics.py`

**Interfaces:**
- Consumes: `DealBrief` (Task 1); `libs.deal_models.contracts.{PriceSimRequest, DispatchRequest, ProjectFinancials, MCRequest, MCResult}`; `libs.deal_models.price_simulator.simulate_prices`; `libs.deal_models.dispatch_valuation.dispatch_annual`; `libs.deal_models.monte_carlo.run_monte_carlo`; `services.deal_engine.price_data.fetch_price_history(province, start_date, end_date, price_col="da_price")`; `services.common.db_utils.get_engine`.
- Produces:
  - `EconomicsResult` dataclass `(mc: MCResult, monthly_price: list[tuple[str, float]], n_price_hours: int, n_simulations: int, model: str)`.
  - `run_economics(brief: DealBrief, n_simulations: int = 1000, fetch_fn=None, monthly_fn=None) -> EconomicsResult` — both fns injectable for tests.
  - `economics_section_markdown(res: EconomicsResult, brief: DealBrief) -> str`.
  Tasks 6, 7, 8, 9 rely on these.

- [ ] **Step 1: Write the failing test**

```python
# services/deal_committee/tests/test_economics.py
import math

import numpy as np
import pytest

from services.deal_committee.brief import DealBrief
from services.deal_committee.economics import economics_section_markdown, run_economics

BRIEF = DealBrief(deal_name="蒙西储能一期", asset_type="bess", province="蒙西",
                  capacity_mw=100, capacity_mwh=200, capex_total_yuan=1.2e9,
                  commissioning_year=2027, tenor_years=15)


def _fake_prices(province, start, end):
    rng = np.random.default_rng(7)
    hours = 370 * 24
    return (300 + 60 * np.sin(np.arange(hours) / 24 / 15) + rng.normal(0, 40, hours)).tolist()


def _fake_monthly(engine, province):
    return [(f"2026-{m:02d}", 280.0 + m * 5) for m in range(1, 13)]


def test_run_economics_returns_mc_result():
    res = run_economics(BRIEF, n_simulations=50, fetch_fn=_fake_prices, monthly_fn=_fake_monthly)
    assert res.n_simulations == 50
    assert res.n_price_hours == 370 * 24
    assert len(res.monthly_price) == 12
    assert math.isfinite(res.mc.revenue_p50)
    assert res.mc.revenue_p10 < res.mc.revenue_p50 < res.mc.revenue_p90
    assert 0.0 <= res.mc.irr_prob_below_hurdle <= 1.0


def test_run_economics_requires_capex():
    bad = BRIEF.model_copy(update={"capex_total_yuan": None})
    with pytest.raises(ValueError, match="总投资"):
        run_economics(bad, fetch_fn=_fake_prices, monthly_fn=_fake_monthly)


def test_run_economics_requires_province():
    bad = BRIEF.model_copy(update={"province": ""})
    with pytest.raises(ValueError, match="省份"):
        run_economics(bad, fetch_fn=_fake_prices, monthly_fn=_fake_monthly)


def test_markdown_contains_kpis():
    res = run_economics(BRIEF, n_simulations=50, fetch_fn=_fake_prices, monthly_fn=_fake_monthly)
    md = economics_section_markdown(res, BRIEF)
    assert "P50" in md and "股权 IRR" in md
    assert "蒙西" in md


def test_solar_maps_to_wind_dispatch():
    solar = BRIEF.model_copy(update={"asset_type": "solar", "installed_mw": 200.0,
                                     "capacity_mw": 0.0, "capacity_mwh": 0.0})
    res = run_economics(solar, n_simulations=50, fetch_fn=_fake_prices, monthly_fn=_fake_monthly)
    assert math.isfinite(res.mc.revenue_p50)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `~/.venvs/bess-platform/bin/python -m pytest services/deal_committee/tests/test_economics.py -v`
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: Write the implementation**

```python
# services/deal_committee/economics.py
"""Economics section — runs the libs/deal_models engine in-process (not an agent)."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta

from libs.deal_models.contracts import (
    DispatchRequest, MCRequest, MCResult, PriceSimRequest, ProjectFinancials,
)
from libs.deal_models.dispatch_valuation import dispatch_annual
from libs.deal_models.monte_carlo import run_monte_carlo
from libs.deal_models.price_simulator import simulate_prices
from services.deal_committee.brief import DealBrief

# solar/solar_bess reuse the wind dispatch models (July spec: same model, different profile)
_DISPATCH_TYPE = {"bess": "bess", "wind": "wind", "solar": "wind",
                  "wind_bess": "wind_bess", "solar_bess": "wind_bess"}
_FIXED_OM_YUAN = 3e6  # matches cashflow_tab default


@dataclass
class EconomicsResult:
    mc: MCResult
    monthly_price: list[tuple[str, float]]  # (YYYY-MM, avg yuan/MWh), 12 entries
    n_price_hours: int
    n_simulations: int
    model: str


def _default_fetch(province: str, start: str, end: str) -> list[float]:
    from services.deal_engine.price_data import fetch_price_history
    return fetch_price_history(province, start, end)


def _default_monthly(engine, province: str) -> list[tuple[str, float]]:
    from sqlalchemy import text
    from services.common.db_utils import get_engine
    engine = engine or get_engine()
    sql = text("""
        SELECT TO_CHAR(DATE_TRUNC('month', datetime), 'YYYY-MM') AS month,
               AVG(CASE WHEN da_price IS NOT NULL AND da_price != 0
                        THEN da_price ELSE rt_price END) AS avg_price
        FROM marketdata.spot_prices_hourly
        WHERE province = :p
          AND datetime >= DATE_TRUNC('month', NOW()) - INTERVAL '12 months'
        GROUP BY 1 ORDER BY 1
    """)
    with engine.connect() as conn:
        rows = conn.execute(sql, {"p": province}).fetchall()
    vals = [float(r[1]) for r in rows if r[1] is not None]
    scale = 1000.0 if vals and sorted(vals)[len(vals) // 2] < 5.0 else 1.0  # kWh → MWh
    return [(r[0], float(r[1]) * scale) for r in rows if r[1] is not None]


def run_economics(brief: DealBrief, n_simulations: int = 1000,
                  fetch_fn=None, monthly_fn=None) -> EconomicsResult:
    if not brief.capex_total_yuan:
        raise ValueError("经济性测算需要总投资额(capex_total_yuan)——请在交易要素表中填写")
    if not brief.province:
        raise ValueError("经济性测算需要省份(province)")

    fetch_fn = fetch_fn or _default_fetch
    monthly_fn = monthly_fn or _default_monthly

    today = date.today()
    end = today.replace(day=1)
    start = (end - timedelta(days=370)).replace(day=1)
    prices = fetch_fn(brief.province, start.isoformat(), end.isoformat())

    at = brief.asset_type
    dispatch_req = DispatchRequest(
        asset_type=_DISPATCH_TYPE[at],
        capacity_mwh=brief.capacity_mwh if "bess" in at else 0.0,
        power_mw=brief.capacity_mw if "bess" in at else 0.0,
        roundtrip_eff=brief.efficiency,
        cycles_per_day=brief.cycles_per_day,
        installed_mw=brief.installed_mw if at != "bess" else 0.0,
    )
    price_req = PriceSimRequest(
        province=brief.province, n_simulations=n_simulations, n_years=1,
        model="ou", price_history_yuan_mwh=prices,
    )
    paths = simulate_prices(price_req, seed=42)
    base_rev = dispatch_annual(paths, dispatch_req).p50
    fin = ProjectFinancials(
        capex_total_yuan=brief.capex_total_yuan,
        commissioning_year=brief.commissioning_year,
        project_life_years=brief.tenor_years,
        debt_ratio=brief.debt_ratio, loan_term_years=brief.loan_term_years,
        interest_rate=brief.loan_rate,
        annual_revenue_yuan=[base_rev] * brief.tenor_years,
        annual_om_yuan=_FIXED_OM_YUAN,
    )
    mc = run_monte_carlo(MCRequest(price_sim=price_req, dispatch=dispatch_req,
                                   financials=fin, n_simulations=n_simulations))
    return EconomicsResult(
        mc=mc, monthly_price=monthly_fn(None, brief.province),
        n_price_hours=len(prices), n_simulations=n_simulations, model="ou",
    )


def economics_section_markdown(res: EconomicsResult, brief: DealBrief) -> str:
    mc = res.mc
    return f"""**测算口径**:{brief.province} · {res.model.upper()} 模型 · {res.n_simulations} 条路径 · 历史价格 {res.n_price_hours} 小时 · 固定运维 ¥{_FIXED_OM_YUAN/1e6:.1f}M/年

| 指标 | P10 | P50 | P90 |
|---|---|---|---|
| 年收入 (¥M) | {mc.revenue_p10/1e6:.1f} | {mc.revenue_p50/1e6:.1f} | {mc.revenue_p90/1e6:.1f} |
| 股权 IRR | {mc.equity_irr_p10:.1%} | {mc.equity_irr_p50:.1%} | {mc.equity_irr_p90:.1%} |
| NPV (¥M) | {mc.npv_p10/1e6:.1f} | {mc.npv_p50/1e6:.1f} | {mc.npv_p90/1e6:.1f} |

- 收入 VaR(5%):¥{mc.revenue_var_5pct/1e6:.1f}M · CVaR:¥{mc.revenue_cvar_5pct/1e6:.1f}M
- 股权 IRR 低于基准(8%)概率:{mc.irr_prob_below_hurdle:.0%}
"""
```

- [ ] **Step 4: Run tests**

Run: `~/.venvs/bess-platform/bin/python -m pytest services/deal_committee/tests/test_economics.py -v`
Expected: 5 PASS

- [ ] **Step 5: Commit**

```bash
git add services/deal_committee/economics.py services/deal_committee/tests/test_economics.py
git commit -m "Add in-process economics section using deal_models engine"
```

---

### Task 6: `orchestrator.py` (committee pipeline)

**Files:**
- Create: `services/deal_committee/orchestrator.py`
- Test: `services/deal_committee/tests/test_orchestrator.py`

**Interfaces:**
- Consumes: `DealBrief` (T1); `SECTION_DEFS, SectionDef, SectionResult, build_question` (T4); `run_economics, EconomicsResult, economics_section_markdown` (T5); `services.common.db_utils.get_engine`; `services.hermes.market_agent_bridge.run_market_query` (lazy, inside `default_query_fn` only).
- Produces:
  - `QueryFn = Callable[[str, str, str], str]` — `(agent_key, question, api_key) -> markdown`.
  - `default_query_fn(market, question, api_key) -> str`.
  - `CommitteeResult` dataclass `(brief, sections: list[SectionResult], economics: EconomicsResult | None = None, synthesis: str = "", recommendation: str = "")`.
  - `run_risk_section(brief, engine=None) -> SectionResult`.
  - `run_single_section(key, brief, query_fn, api_key, econ_fn=None, risk_fn=None, timeout_s=180) -> tuple[SectionResult, EconomicsResult | None]`.
  - `run_committee(brief, query_fn=default_query_fn, api_key="", econ_fn=None, risk_fn=None, on_section_done=None, timeout_s=180) -> CommitteeResult`.
  Tasks 7, 9, 12 rely on these.

- [ ] **Step 1: Write the failing test**

```python
# services/deal_committee/tests/test_orchestrator.py
import time

from services.deal_committee.economics import EconomicsResult
from services.deal_committee.orchestrator import run_committee, run_single_section
from services.deal_committee.brief import DealBrief
from libs.deal_models.contracts import MCResult
import numpy as np

BRIEF = DealBrief(deal_name="蒙西储能一期", asset_type="bess", province="蒙西",
                  capacity_mw=100, capacity_mwh=200, capex_total_yuan=1.2e9)


def _fake_mc():
    return MCResult(revenue_p10=8e7, revenue_p50=1e8, revenue_p90=1.2e8,
                    revenue_var_5pct=2e7, revenue_cvar_5pct=3e7,
                    equity_irr_p10=0.05, equity_irr_p50=0.09, equity_irr_p90=0.13,
                    irr_prob_below_hurdle=0.3, npv_p10=-1e7, npv_p50=2e7, npv_p90=5e7,
                    tornado=[], revenue_paths=np.zeros(10),
                    equity_irr_paths=np.zeros(10), npv_paths=np.zeros(10))


def _fake_econ(brief, **kw):
    return EconomicsResult(mc=_fake_mc(), monthly_price=[("2026-08", 300.0)],
                           n_price_hours=8760, n_simulations=10, model="ou")


def _fake_risk(brief, engine=None):
    from services.deal_committee.sections import SectionResult
    return SectionResult(key="risk", title="风险数据", markdown="| 台账 | VaR |\n|---|---|\n| A | 1M |")


def _fake_query(market, question, api_key):
    return f"[{market}] 回答:数据良好"


def test_run_committee_assembles_all_sections():
    res = run_committee(BRIEF, query_fn=_fake_query, econ_fn=_fake_econ, risk_fn=_fake_risk)
    assert [s.key for s in res.sections] == [
        "market_background", "policy", "economics",
        "ops_mengxi", "ops_asset_risk", "ops_retail_risk", "risk"]
    assert all(s.status == "ok" for s in res.sections)
    assert res.economics is not None
    assert "[spot]" in res.sections[0].markdown
    assert "P50" in res.sections[2].markdown  # economics markdown from real formatter


def test_failing_agent_marks_section_failed_and_continues():
    def boom(market, question, api_key):
        if market == "mengxi":
            raise RuntimeError("agent exploded")
        return "ok"
    res = run_committee(BRIEF, query_fn=boom, econ_fn=_fake_econ, risk_fn=_fake_risk)
    mengxi = next(s for s in res.sections if s.key == "ops_mengxi")
    assert mengxi.status == "failed" and "agent exploded" in mengxi.error
    others = [s for s in res.sections if s.key != "ops_mengxi"]
    assert all(s.status == "ok" for s in others)


def test_section_timeout_marks_failed():
    def slow(market, question, api_key):
        time.sleep(1.0)
        return "too late"
    res = run_committee(BRIEF, query_fn=slow, econ_fn=_fake_econ,
                        risk_fn=_fake_risk, timeout_s=0.1)
    agents = [s for s in res.sections if s.key.startswith(("market", "policy", "ops"))]
    assert all(s.status == "failed" for s in agents)
    assert all("超时" in s.error for s in agents)


def test_failing_economics_keeps_economics_none():
    def bad_econ(brief, **kw):
        raise ValueError("总投资")
    res = run_committee(BRIEF, query_fn=_fake_query, econ_fn=bad_econ, risk_fn=_fake_risk)
    econ = next(s for s in res.sections if s.key == "economics")
    assert econ.status == "failed"
    assert res.economics is None


def test_on_section_done_callback_fires_in_order():
    seen = []
    run_committee(BRIEF, query_fn=_fake_query, econ_fn=_fake_econ,
                  risk_fn=_fake_risk, on_section_done=lambda s: seen.append(s.key))
    assert seen == ["market_background", "policy", "economics",
                    "ops_mengxi", "ops_asset_risk", "ops_retail_risk", "risk"]


def test_run_single_section_agent():
    sec, econ = run_single_section("policy", BRIEF, _fake_query, api_key="")
    assert sec.status == "ok" and econ is None
    sec2, econ2 = run_single_section("economics", BRIEF, _fake_query, "", econ_fn=_fake_econ)
    assert sec2.status == "ok" and econ2 is not None
```

- [ ] **Step 2: Run test to verify it fails**

Run: `~/.venvs/bess-platform/bin/python -m pytest services/deal_committee/tests/test_orchestrator.py -v`
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: Write the implementation**

```python
# services/deal_committee/orchestrator.py
"""Committee pipeline — runs the 7 sections sequentially and assembles a CommitteeResult."""
from __future__ import annotations

import concurrent.futures
from dataclasses import dataclass, field
from typing import Callable, Optional

from services.deal_committee.brief import DealBrief
from services.deal_committee.economics import (
    EconomicsResult, economics_section_markdown, run_economics,
)
from services.deal_committee.sections import SECTION_DEFS, SectionResult, build_question

QueryFn = Callable[[str, str, str], str]  # (agent_key, question, api_key) -> markdown


def default_query_fn(market: str, question: str, api_key: str) -> str:
    from services.hermes.market_agent_bridge import run_market_query  # lazy: hermes deps
    return run_market_query(market, question, api_key=api_key)


@dataclass
class CommitteeResult:
    brief: DealBrief
    sections: list[SectionResult]
    economics: Optional[EconomicsResult] = None
    synthesis: str = ""
    recommendation: str = ""


def run_risk_section(brief: DealBrief, engine=None) -> SectionResult:
    """Pull latest rm_* snapshots as the risk-benchmark table (no agent)."""
    from sqlalchemy import text
    from services.common.db_utils import get_engine
    engine = engine or get_engine()
    sql = text("""
        SELECT b.name AS book, p.snapshot_date,
               p.realized_cny, p.unrealized_mtm_cny,
               p.curtailment_rate_pct, p.equivalent_hours,
               v.var_1d_95_cny, v.var_10d_95_cny
        FROM marketdata.rm_pnl_snapshots p
        JOIN marketdata.rm_books b ON b.id = p.book_id
        LEFT JOIN marketdata.rm_var_snapshots v
               ON v.book_id = p.book_id AND v.snapshot_date = p.snapshot_date
              AND v.method = 'historical'
        WHERE p.snapshot_date >= CURRENT_DATE - INTERVAL '6 months'
        ORDER BY p.snapshot_date DESC, b.name
        LIMIT 60
    """)
    with engine.connect() as conn:
        rows = conn.execute(sql).fetchall()
    if not rows:
        return SectionResult(key="risk", title="风险数据",
                             markdown="近 6 个月无风险台账(rm_*)快照数据。")
    lines = ["| 台账 | 日期 | 已实现P&L(¥M) | 未实现MtM(¥M) | 限电率 | VaR(1d,95%,¥M) |",
             "|---|---|---|---|---|---|"]
    for r in rows:
        lines.append(
            f"| {r[0]} | {r[1]} | {(r[2] or 0)/1e6:.2f} | {(r[3] or 0)/1e6:.2f} "
            f"| {('—' if r[4] is None else f'{float(r[4]):.1f}%')} "
            f"| {('—' if r[6] is None else f'{float(r[6])/1e6:.2f}')} |"
        )
    return SectionResult(key="risk", title="风险数据", markdown="\n".join(lines))


def _run_agent_with_timeout(query_fn: QueryFn, agent: str, question: str,
                            api_key: str, timeout_s: int) -> str:
    ex = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    try:
        fut = ex.submit(query_fn, agent, question, api_key)
        return fut.result(timeout=timeout_s)
    except concurrent.futures.TimeoutError:
        raise TimeoutError(f"章节超时(>{timeout_s}s)")
    finally:
        ex.shutdown(wait=False)  # leaked thread finishes in background; cannot kill threads


def run_single_section(key: str, brief: DealBrief, query_fn: QueryFn, api_key: str,
                       econ_fn=None, risk_fn=None, timeout_s: int = 180,
                       ) -> tuple[SectionResult, Optional[EconomicsResult]]:
    """Run one section. Returns (SectionResult, EconomicsResult|None for the economics key)."""
    title = next(s.title for s in SECTION_DEFS if s.key == key)
    try:
        if key == "economics":
            res = (econ_fn or run_economics)(brief)
            return SectionResult(key, title, economics_section_markdown(res, brief)), res
        if key == "risk":
            return (risk_fn or run_risk_section)(brief), None
        q = build_question(key, brief)
        agent = next(s.agent for s in SECTION_DEFS if s.key == key)
        md = _run_agent_with_timeout(query_fn, agent, q, api_key, timeout_s)
        return SectionResult(key, title, md), None
    except Exception as e:
        return SectionResult(key, title, status="failed", error=str(e)), None


def run_committee(brief: DealBrief, query_fn: QueryFn = default_query_fn, api_key: str = "",
                  econ_fn=None, risk_fn=None,
                  on_section_done: Optional[Callable[[SectionResult], None]] = None,
                  timeout_s: int = 180) -> CommitteeResult:
    result = CommitteeResult(brief=brief, sections=[])
    for sdef in SECTION_DEFS:
        sec, econ = run_single_section(sdef.key, brief, query_fn, api_key,
                                       econ_fn=econ_fn, risk_fn=risk_fn, timeout_s=timeout_s)
        if econ is not None:
            result.economics = econ
        result.sections.append(sec)
        if on_section_done:
            on_section_done(sec)
    return result
```

- [ ] **Step 4: Run tests**

Run: `~/.venvs/bess-platform/bin/python -m pytest services/deal_committee/tests/test_orchestrator.py -v`
Expected: 6 PASS (the timeout test's leaked 1s sleeper finishes at interpreter exit — harmless)

- [ ] **Step 5: Commit**

```bash
git add services/deal_committee/orchestrator.py services/deal_committee/tests/test_orchestrator.py
git commit -m "Add committee orchestrator with per-section failure isolation"
```

---

### Task 7: `synthesis.py` (go/no-go synthesis)

**Files:**
- Create: `services/deal_committee/synthesis.py`
- Test: `services/deal_committee/tests/test_synthesis.py`

**Interfaces:**
- Consumes: `DealBrief` (T1), `SectionResult` (T4), `EconomicsResult` (T5), `shared.anthropic_client.make_client`.
- Produces:
  - `SYNTHESIS_MODEL = "claude-sonnet-4-6"`.
  - `build_synthesis_prompt(brief, sections, economics) -> str`.
  - `parse_recommendation(text: str) -> str` — returns `"GO"` / `"有条件 GO"` / `"NO-GO"` / `""`.
  - `run_synthesis(brief, sections, economics, api_key: str, client=None) -> tuple[str, str]` — `(synthesis_markdown, recommendation)`; `client` is the test seam.
  Task 9, 12 rely on these.

- [ ] **Step 1: Write the failing test**

```python
# services/deal_committee/tests/test_synthesis.py
from types import SimpleNamespace

from services.deal_committee.brief import DealBrief
from services.deal_committee.sections import SectionResult
from services.deal_committee.synthesis import (
    build_synthesis_prompt, parse_recommendation, run_synthesis,
)

BRIEF = DealBrief(deal_name="蒙西储能一期", asset_type="bess", province="蒙西",
                  capacity_mw=100, capacity_mwh=200, capex_total_yuan=1.2e9)
SECTIONS = [
    SectionResult("market_background", "市场背景", "近12个月均价 320 元/MWh"),
    SectionResult("policy", "政策与规则环境", "容量补偿政策已落地", status="ok"),
    SectionResult("ops_mengxi", "运营实证 · 蒙西储能", "捕获率 82%", status="failed",
                  error="agent exploded"),
]


class _FakeClient:
    def __init__(self, text): self._text = text
    @property
    def messages(self): return self
    def create(self, **kw): return SimpleNamespace(content=[SimpleNamespace(text=self._text)])


def test_prompt_contains_sections_kpis_and_grounding():
    p = build_synthesis_prompt(BRIEF, SECTIONS, None)
    assert "蒙西储能一期" in p
    assert "320 元/MWh" in p
    assert "容量补偿政策已落地" in p
    assert "agent exploded" in p          # failures are visible to the synthesizer
    assert "不得编造" in p                # grounding rule
    assert "GO" in p and "NO-GO" in p     # output contract


def test_parse_recommendation_variants():
    assert parse_recommendation("## 投资建议\n结论:GO\n理由…") == "GO"
    assert parse_recommendation("## 投资建议\n结论:有条件 GO,前提是…") == "有条件 GO"
    assert parse_recommendation("## 投资建议\n结论:NO-GO") == "NO-GO"
    assert parse_recommendation("结论:谨慎推进(有条件 GO)") == "有条件 GO"
    assert parse_recommendation("没有结论") == ""


def test_run_synthesis_returns_text_and_label():
    md = "## 交易摘要\n……\n## 风险分析\n……\n## 投资建议\n结论:有条件 GO\n……"
    text, rec = run_synthesis(BRIEF, SECTIONS, None, api_key="", client=_FakeClient(md))
    assert "交易摘要" in text
    assert rec == "有条件 GO"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `~/.venvs/bess-platform/bin/python -m pytest services/deal_committee/tests/test_synthesis.py -v`
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: Write the implementation**

```python
# services/deal_committee/synthesis.py
"""Committee synthesis — one grounded LLM call producing summary, risks, recommendation."""
from __future__ import annotations

import re
from typing import Optional

from services.deal_committee.brief import DealBrief
from services.deal_committee.economics import EconomicsResult
from services.deal_committee.sections import SectionResult

SYNTHESIS_MODEL = "claude-sonnet-4-6"


def build_synthesis_prompt(brief: DealBrief, sections: list[SectionResult],
                           economics: Optional[EconomicsResult]) -> str:
    blocks = []
    for s in sections:
        body = s.markdown if s.status == "ok" else f"[本节生成失败: {s.error}]"
        blocks.append(f"### {s.title}\n{body}")
    if economics is not None:
        mc = economics.mc
        blocks.append(
            "### 经济性核心指标\n"
            f"收入 P10/P50/P90: ¥{mc.revenue_p10/1e6:.1f}M / ¥{mc.revenue_p50/1e6:.1f}M / ¥{mc.revenue_p90/1e6:.1f}M\n"
            f"股权 IRR P10/P50/P90: {mc.equity_irr_p10:.1%} / {mc.equity_irr_p50:.1%} / {mc.equity_irr_p90:.1%}\n"
            f"IRR 低于 8% 基准概率: {mc.irr_prob_below_hurdle:.0%}\n"
            f"NPV P50: ¥{mc.npv_p50/1e6:.1f}M"
        )
    return f"""你是投资决策委员会秘书。基于以下交易要素与各章节分析材料,撰写 DAF 的三个章节。

【交易要素】
{brief.model_dump_json(exclude={"field_confidence"})}

【章节材料】
{chr(10).join(blocks)}

【硬性要求】
1. 全程中文;技术术语可用英文(IRR、VaR、capture rate)。
2. 不得编造数字:所有数字必须来自上述材料;材料缺失或章节失败时,明确写出"数据缺失"及其对结论的影响。
3. 输出恰好三节,标题严格使用:
## 交易摘要
(项目、市场、经济性一句话概括,150 字内)
## 风险分析
(3-6 条编号风险,每条含:风险描述 / 可能性(高/中/低) / 影响 / 缓释措施)
## 投资建议
(首行写"结论:GO"或"结论:有条件 GO"或"结论:NO-GO";随后列出条件、风险缓释建议与核心假设)"""


def parse_recommendation(text: str) -> str:
    m = re.search(r"结论\s*[:：]\s*(?:谨慎推进[（(]?)?(有条件\s*GO|NO[-\s]?GO|GO)", text, re.I)
    if not m:
        m = re.search(r"(有条件\s*GO|NO[-\s]?GO)", text, re.I)
    if not m:
        return "GO" if re.search(r"\bGO\b", text) else ""
    label = m.group(1).upper().replace(" ", " ")
    label = re.sub(r"\s+", " ", label).replace("NO GO", "NO-GO")
    return "有条件 GO" if "有条件" in label else label


def run_synthesis(brief: DealBrief, sections: list[SectionResult],
                  economics: Optional[EconomicsResult], api_key: str,
                  client=None) -> tuple[str, str]:
    if client is None:
        from shared.anthropic_client import make_client
        client = make_client(api_key)
    resp = client.messages.create(
        model=SYNTHESIS_MODEL, max_tokens=4000,
        messages=[{"role": "user", "content": build_synthesis_prompt(brief, sections, economics)}],
    )
    text = resp.content[0].text.strip()
    return text, parse_recommendation(text)
```

- [ ] **Step 4: Run tests**

Run: `~/.venvs/bess-platform/bin/python -m pytest services/deal_committee/tests/test_synthesis.py -v`
Expected: 3 PASS

- [ ] **Step 5: Commit**

```bash
git add services/deal_committee/synthesis.py services/deal_committee/tests/test_synthesis.py
git commit -m "Add grounded committee synthesis with recommendation parsing"
```

---

### Task 8: `charts.py` (PDF chart PNGs)

**Files:**
- Create: `services/deal_committee/charts.py`
- Test: `services/deal_committee/tests/test_charts.py`

**Interfaces:**
- Consumes: matplotlib (Agg backend), numpy.
- Produces: `chart_monthly_price(monthly_price: list[tuple[str, float]]) -> bytes`, `chart_revenue_distribution(revenue_paths) -> bytes`, `chart_irr_distribution(equity_irr_paths, hurdle_rate: float = 0.08) -> bytes`. All return PNG bytes; axis labels in English (Global Constraints). Used by Task 9.

- [ ] **Step 1: Write the failing test**

```python
# services/deal_committee/tests/test_charts.py
import numpy as np

from services.deal_committee.charts import (
    chart_irr_distribution, chart_monthly_price, chart_revenue_distribution,
)

PNG_MAGIC = b"\x89PNG"


def test_chart_monthly_price():
    png = chart_monthly_price([(f"2026-{m:02d}", 280.0 + m * 5) for m in range(1, 13)])
    assert png.startswith(PNG_MAGIC) and len(png) > 5_000


def test_chart_revenue_distribution():
    png = chart_revenue_distribution(np.random.default_rng(1).normal(1e8, 2e7, 500))
    assert png.startswith(PNG_MAGIC) and len(png) > 5_000


def test_chart_irr_distribution():
    png = chart_irr_distribution(np.random.default_rng(2).normal(0.09, 0.03, 500),
                                 hurdle_rate=0.08)
    assert png.startswith(PNG_MAGIC) and len(png) > 5_000


def test_empty_input_raises_valueerror():
    import pytest
    with pytest.raises(ValueError):
        chart_monthly_price([])
```

- [ ] **Step 2: Run test to verify it fails**

Run: `~/.venvs/bess-platform/bin/python -m pytest services/deal_committee/tests/test_charts.py -v`
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: Write the implementation**

```python
# services/deal_committee/charts.py
"""Matplotlib PNG charts for the DAF PDF. Axis labels in English (no CJK font in slim images)."""
from __future__ import annotations

from io import BytesIO

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np


def _png(fig) -> bytes:
    buf = BytesIO()
    fig.savefig(buf, format="png", dpi=150, bbox_inches="tight")
    plt.close(fig)
    return buf.getvalue()


def chart_monthly_price(monthly_price: list[tuple[str, float]]) -> bytes:
    if not monthly_price:
        raise ValueError("monthly_price is empty")
    months, vals = zip(*monthly_price)
    fig, ax = plt.subplots(figsize=(8, 3.2))
    ax.plot(months, vals, marker="o", color="#1f77b4", lw=1.8)
    ax.set_ylabel("¥/MWh")
    ax.set_title("Monthly Average DA Price — Last 12 Months")
    ax.grid(alpha=0.3)
    plt.setp(ax.get_xticklabels(), rotation=45, ha="right")
    return _png(fig)


def chart_revenue_distribution(revenue_paths) -> bytes:
    arr = np.asarray(revenue_paths, dtype=float)
    if arr.size == 0:
        raise ValueError("revenue_paths is empty")
    p10, p50, p90 = np.percentile(arr, [10, 50, 90])
    fig, ax = plt.subplots(figsize=(8, 3.2))
    ax.hist(arr / 1e6, bins=40, color="#2ca02c", alpha=0.75)
    for v, c, lbl in ((p10, "#d62728", "P10"), (p50, "#1f77b4", "P50"), (p90, "#9467bd", "P90")):
        ax.axvline(v / 1e6, color=c, ls="--", lw=1.5, label=f"{lbl} ¥{v/1e6:.1f}M")
    ax.set_xlabel("Annual Revenue (¥M)")
    ax.set_ylabel("Frequency")
    ax.set_title("Annual Revenue Distribution")
    ax.legend()
    return _png(fig)


def chart_irr_distribution(equity_irr_paths, hurdle_rate: float = 0.08) -> bytes:
    arr = np.asarray(equity_irr_paths, dtype=float)
    if arr.size == 0:
        raise ValueError("equity_irr_paths is empty")
    p10, p50, p90 = np.percentile(arr, [10, 50, 90])
    fig, ax = plt.subplots(figsize=(8, 3.2))
    ax.hist(arr * 100, bins=40, color="#ff7f0e", alpha=0.75)
    ax.axvline(hurdle_rate * 100, color="#d62728", lw=2, label=f"Hurdle {hurdle_rate:.0%}")
    for v, lbl in ((p10, "P10"), (p50, "P50"), (p90, "P90")):
        ax.axvline(v * 100, color="#1f77b4", ls="--", lw=1.2, label=f"{lbl} {v:.1%}")
    ax.set_xlabel("Equity IRR (%)")
    ax.set_ylabel("Frequency")
    ax.set_title("Equity IRR Distribution")
    ax.legend(fontsize=8)
    return _png(fig)
```

- [ ] **Step 4: Run tests**

Run: `~/.venvs/bess-platform/bin/python -m pytest services/deal_committee/tests/test_charts.py -v`
Expected: 4 PASS

- [ ] **Step 5: Commit**

```bash
git add services/deal_committee/charts.py services/deal_committee/tests/test_charts.py
git commit -m "Add DAF chart generators (monthly price, revenue, IRR distributions)"
```

---

### Task 9: `daf_builder.py` (reportlab DAF PDF)

**Files:**
- Create: `services/deal_committee/daf_builder.py`
- Test: `services/deal_committee/tests/test_daf_builder.py`

**Interfaces:**
- Consumes: `CommitteeResult` (T6), `SectionResult` (T4), charts (T8), `services.hermes.export_utils._register_cjk_font` (exists — if that import pulls unexpected deps, copy the function body into `daf_builder._register_cjk_font` instead and note the deviation in the commit message).
- Produces: `split_synthesis(synthesis_md: str) -> dict[str, str]`, `build_daf(result: CommitteeResult) -> bytes`. Used by Task 12.

- [ ] **Step 1: Write the failing test**

```python
# services/deal_committee/tests/test_daf_builder.py
from services.deal_committee.brief import DealBrief
from services.deal_committee.daf_builder import build_daf, split_synthesis
from services.deal_committee.orchestrator import CommitteeResult
from services.deal_committee.sections import SectionResult
from services.deal_committee.tests.test_orchestrator import _fake_econ, BRIEF

SYNTH = ("## 交易摘要\n蒙西 100MW/200MWh 储能项目,经济性达标。\n"
         "## 风险分析\n1. 价格下行风险 / 可能性:中 / 影响:收入-15% / 缓释:签订保底\n"
         "## 投资建议\n结论:有条件 GO\n核心假设:均价 ≥ 300 元/MWh")


def test_split_synthesis_three_parts():
    parts = split_synthesis(SYNTH)
    assert "蒙西 100MW" in parts["交易摘要"]
    assert "价格下行风险" in parts["风险分析"]
    assert "有条件 GO" in parts["投资建议"]


def test_split_synthesis_missing_sections_default_empty():
    parts = split_synthesis("## 交易摘要\n只有摘要")
    assert parts["交易摘要"] and parts["风险分析"] == "" and parts["投资建议"] == ""


def test_build_daf_produces_valid_pdf():
    result = CommitteeResult(
        brief=BRIEF,
        sections=[
            SectionResult("market_background", "市场背景", "近12个月均价 320 元/MWh"),
            SectionResult("policy", "政策与规则环境", "容量补偿政策已落地"),
            SectionResult("economics", "经济性测算", "见下表"),
            SectionResult("ops_mengxi", "运营实证 · 蒙西储能", "捕获率 82%"),
            SectionResult("ops_asset_risk", "运营实证 · 资产风险台账", "组合 VaR 稳定"),
            SectionResult("ops_retail_risk", "运营实证 · 零售风险台账", "无该省零售业务",
                          status="failed", error="no data"),
            SectionResult("risk", "风险数据", "| 台账 | VaR |\n|---|---|\n| A | 1.0 |"),
        ],
        economics=_fake_econ(BRIEF),
        synthesis=SYNTH,
        recommendation="有条件 GO",
    )
    pdf = build_daf(result)
    assert pdf.startswith(b"%PDF")
    assert len(pdf) > 10_000


def test_build_daf_handles_failed_sections_and_no_economics():
    result = CommitteeResult(
        brief=DealBrief(deal_name="最小案例", province="山东"),
        sections=[SectionResult("market_background", "市场背景", "失败",
                                status="failed", error="timeout")],
        economics=None, synthesis="", recommendation="",
    )
    pdf = build_daf(result)
    assert pdf.startswith(b"%PDF")
```

- [ ] **Step 2: Run test to verify it fails**

Run: `~/.venvs/bess-platform/bin/python -m pytest services/deal_committee/tests/test_daf_builder.py -v`
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: Write the implementation**

```python
# services/deal_committee/daf_builder.py
"""Build the DAF PDF (A4, Chinese, reportlab) from a CommitteeResult."""
from __future__ import annotations

import io
import re
from datetime import datetime

from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import cm
from reportlab.lib.utils import ImageReader
from reportlab.platypus import (
    Image, Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle,
)

from services.deal_committee.charts import (
    chart_irr_distribution, chart_monthly_price, chart_revenue_distribution,
)
from services.deal_committee.orchestrator import CommitteeResult

_ACCENT = colors.HexColor("#1f4e79")
_GREY = colors.HexColor("#444444")


def split_synthesis(synthesis_md: str) -> dict[str, str]:
    """Split the synthesis markdown into its three mandated sections."""
    parts = {"交易摘要": "", "风险分析": "", "投资建议": ""}
    current = None
    for line in (synthesis_md or "").splitlines():
        m = re.match(r"^##\s*(交易摘要|风险分析|投资建议)\s*$", line.strip())
        if m:
            current = m.group(1)
            continue
        if current:
            parts[current] += line + "\n"
    return {k: v.strip() for k, v in parts.items()}


def _register_font() -> str:
    from services.hermes.export_utils import _register_cjk_font
    return _register_cjk_font()


def _styles(font: str) -> dict:
    ss = getSampleStyleSheet()
    return {
        "title": ParagraphStyle("daf_title", parent=ss["Title"], fontName=font,
                                fontSize=18, textColor=_ACCENT, spaceAfter=4),
        "h1": ParagraphStyle("daf_h1", parent=ss["Heading1"], fontName=font,
                             fontSize=13, textColor=_ACCENT, spaceBefore=12, spaceAfter=4),
        "body": ParagraphStyle("daf_body", parent=ss["Normal"], fontName=font,
                               fontSize=9.5, leading=14),
        "cell": ParagraphStyle("daf_cell", parent=ss["Normal"], fontName=font,
                               fontSize=8.5, leading=11),
        "caption": ParagraphStyle("daf_cap", parent=ss["Normal"], fontName=font,
                                  fontSize=8, textColor=_GREY, spaceAfter=6),
    }


def _esc(text: str) -> str:
    return (text or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _md_inline(text: str) -> str:
    return re.sub(r"\*\*(.+?)\*\*", r"<b>\1</b>", _esc(text))


def _md_to_flowables(md: str, styles: dict) -> list:
    """Minimal markdown → flowables: headings, bullets, pipe tables, paragraphs."""
    flow, lines, i = [], (md or "").splitlines(), 0
    while i < len(lines):
        line = lines[i].rstrip()
        if not line.strip():
            i += 1
            continue
        if line.lstrip().startswith("|"):  # pipe table block
            rows = []
            while i < len(lines) and lines[i].lstrip().startswith("|"):
                cells = [c.strip() for c in lines[i].strip().strip("|").split("|")]
                if not all(set(c) <= set(":- ") for c in cells):  # skip separator row
                    rows.append([Paragraph(_md_inline(c), styles["cell"]) for c in cells])
                i += 1
            if rows:
                ncol = max(len(r) for r in rows)
                rows = [r + [Paragraph("", styles["cell"])] * (ncol - len(r)) for r in rows]
                t = Table(rows, hAlign="LEFT")
                t.setStyle(TableStyle([
                    ("GRID", (0, 0), (-1, -1), 0.4, colors.HexColor("#999999")),
                    ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#e8eef5")),
                    ("VALIGN", (0, 0), (-1, -1), "TOP"),
                    ("LEFTPADDING", (0, 0), (-1, -1), 4),
                    ("RIGHTPADDING", (0, 0), (-1, -1), 4),
                ]))
                flow += [t, Spacer(1, 6)]
            continue
        if line.startswith("### "):
            flow.append(Paragraph(_md_inline(line[4:]), styles["h1"]))
        elif line.startswith("## "):
            flow.append(Paragraph(_md_inline(line[3:]), styles["h1"]))
        elif line.lstrip().startswith(("- ", "• ")):
            flow.append(Paragraph("• " + _md_inline(line.lstrip()[2:]), styles["body"]))
        else:
            flow.append(Paragraph(_md_inline(line), styles["body"]))
        i += 1
    return flow


def _png_flowable(png: bytes, width_cm: float = 15.5) -> Image:
    ir = ImageReader(io.BytesIO(png))
    w, h = ir.getSize()
    width = width_cm * cm
    return Image(io.BytesIO(png), width=width, height=width * h / w)


def _kv_table(pairs: list[tuple[str, str]], styles: dict) -> Table:
    rows = [[Paragraph(f"<b>{_esc(k)}</b>", styles["cell"]), Paragraph(_esc(v), styles["cell"])]
            for k, v in pairs]
    t = Table(rows, colWidths=[4.2 * cm, 11.8 * cm], hAlign="LEFT")
    t.setStyle(TableStyle([
        ("GRID", (0, 0), (-1, -1), 0.4, colors.HexColor("#999999")),
        ("BACKGROUND", (0, 0), (0, -1), colors.HexColor("#e8eef5")),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("LEFTPADDING", (0, 0), (-1, -1), 4),
    ]))
    return t


def build_daf(result: CommitteeResult) -> bytes:
    font = _register_font()
    styles = _styles(font)
    brief = result.brief
    sections = {s.key: s for s in result.sections}
    syn = split_synthesis(result.synthesis)

    buf = io.BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=A4, leftMargin=2 * cm, rightMargin=2 * cm,
                            topMargin=2 * cm, bottomMargin=2 * cm,
                            title=f"DAF - {brief.deal_name or 'untitled'}")
    flow = [
        Paragraph("投资决策建议书 (Deal Advice Form)", styles["title"]),
        Paragraph(f"{brief.deal_name or '未命名项目'} · 生成时间 {datetime.now():%Y-%m-%d %H:%M}",
                  styles["caption"]),
        Spacer(1, 6),
    ]

    # 1. 交易概要表
    flow.append(Paragraph("一、交易概要", styles["h1"]))
    capex = f"¥{brief.capex_total_yuan/1e8:.2f} 亿" if brief.capex_total_yuan else "—"
    flow.append(_kv_table([
        ("项目名称", brief.deal_name or "—"),
        ("资产类型", brief.asset_type),
        ("省份 / 节点", f"{brief.province or '—'} / {brief.node or '—'}"),
        ("规模", f"{brief.capacity_mw:g} MW / {brief.capacity_mwh:g} MWh(储能)· "
                 f"{brief.installed_mw:g} MW(新能源装机)"),
        ("总投资", capex),
        ("期限 / 投运", f"{brief.tenor_years} 年 / {brief.commissioning_year}"),
        ("对手方", brief.counterparty or "—"),
        ("交易结构", brief.structure_notes or "—"),
        ("融资", f"负债率 {brief.debt_ratio:.0%} · 利率 {brief.loan_rate:.1%} · "
                 f"{brief.loan_term_years} 年"),
        ("投资结论", result.recommendation or "(未生成)"),
    ], styles))
    flow.append(Spacer(1, 6))

    # 2. 交易摘要
    flow.append(Paragraph("二、交易摘要", styles["h1"]))
    flow += _md_to_flowables(syn["交易摘要"] or "(未生成综合意见)", styles)

    # 3-6. section markdowns in fixed order
    section_headings = [
        ("market_background", "三、市场背景"),
        ("policy", "四、政策与规则环境"),
        ("economics", "五、经济性分析"),
        ("ops_mengxi", "六、运营实证"),
        ("ops_asset_risk", None),      # appended under 六
        ("ops_retail_risk", None),     # appended under 六
        ("risk", "七、风险数据基准"),
    ]
    for key, heading in section_headings:
        if heading:
            flow.append(Paragraph(heading, styles["h1"]))
        sec = sections.get(key)
        if sec is None:
            continue
        if sec.status != "ok":
            flow.append(Paragraph(f"〔本节数据缺失:{_esc(sec.error)}〕", styles["body"]))
            continue
        flow += _md_to_flowables(sec.markdown, styles)
        if key == "market_background" and result.economics and result.economics.monthly_price:
            flow.append(_png_flowable(chart_monthly_price(result.economics.monthly_price)))
        if key == "economics" and result.economics:
            flow.append(_png_flowable(chart_revenue_distribution(result.economics.mc.revenue_paths)))
            flow.append(_png_flowable(chart_irr_distribution(result.economics.mc.equity_irr_paths)))

    # 7. 风险分析 + 8. 投资建议
    flow.append(Paragraph("八、风险分析", styles["h1"]))
    flow += _md_to_flowables(syn["风险分析"] or "(未生成)", styles)
    flow.append(Paragraph("九、投资建议", styles["h1"]))
    flow += _md_to_flowables(syn["投资建议"] or "(未生成)", styles)

    # 10. 附录
    flow.append(Paragraph("十、附录", styles["h1"]))
    sources = "、".join(brief.source_files) or "手工录入"
    flow += _md_to_flowables(
        f"- 输入材料:{sources}\n"
        "- 数据来源:spot / bess-map / mengxi / asset-risk / retail-risk 无头代理,"
        "marketdata.rm_* 台账,libs/deal_models 经济性引擎(OU 模型)\n"
        f"- 生成时间:{datetime.now():%Y-%m-%d %H:%M} · 模型:claude-sonnet-4-6",
        styles)

    doc.build(flow)
    return buf.getvalue()
```

- [ ] **Step 4: Run tests**

Run: `~/.venvs/bess-platform/bin/python -m pytest services/deal_committee/tests/ -v`
Expected: all PASS (21 tests). If `services.hermes.export_utils` import fails, apply the documented fallback (copy `_register_cjk_font` into `daf_builder.py`).

- [ ] **Step 5: Commit**

```bash
git add services/deal_committee/daf_builder.py services/deal_committee/tests/test_daf_builder.py
git commit -m "Add reportlab DAF PDF builder with CJK support"
```

---

### Task 10: `library.py` (RDS persistence) + DDL file

**Files:**
- Create: `services/deal_committee/library.py`
- Create: `db/ddl/marketdata/deal_committee.sql`
- Test: `services/deal_committee/tests/test_library.py`

**Interfaces:**
- Consumes: `DealBrief` (T1), SQLAlchemy engine.
- Produces: `ensure_tables(engine) -> None`, `save_brief(engine, brief: DealBrief) -> int`, `save_daf(engine, brief_id: int, brief: DealBrief, pdf_bytes: bytes, filename: str, recommendation: str) -> int`, `list_dafs(engine, limit: int = 20) -> list[dict]`, `load_daf(engine, daf_id: int) -> tuple[bytes, str]`. Used by Tasks 11–12.

- [ ] **Step 1: Write the failing test** (mock-based; real round-trip is the Task 13 smoke)

```python
# services/deal_committee/tests/test_library.py
from unittest.mock import MagicMock

from services.deal_committee.brief import DealBrief
from services.deal_committee.library import list_dafs, load_daf, save_brief, save_daf


def _engine_with(fetch_one=None, fetch_all=None):
    conn = MagicMock()
    if fetch_one is not None:
        conn.execute.return_value.fetchone.return_value = fetch_one
    if fetch_all is not None:
        conn.execute.return_value.fetchall.return_value = fetch_all
    engine = MagicMock()
    engine.begin.return_value.__enter__.return_value = conn
    engine.connect.return_value.__enter__.return_value = conn
    return engine, conn


def test_save_brief_inserts_jsonb_and_returns_id():
    engine, conn = _engine_with(fetch_one=(42,))
    brief = DealBrief(deal_name="蒙西储能一期", province="蒙西", confirmed=True)
    brief_id = save_brief(engine, brief)
    assert brief_id == 42
    sql, params = conn.execute.call_args[0][0], conn.execute.call_args[0][1]
    assert "marketdata.deal_briefs" in str(sql)
    assert "蒙西储能一期" in params["brief"]


def test_save_daf_stores_bytes_and_size():
    engine, conn = _engine_with(fetch_one=(7,))
    daf_id = save_daf(engine, 42, DealBrief(deal_name="x"), b"%PDF-fake", "DAF_x.pdf", "GO")
    assert daf_id == 7
    params = conn.execute.call_args[0][1]
    assert params["pdf"] == b"%PDF-fake"
    assert params["recommendation"] == "GO"


def test_list_dafs_returns_dicts():
    engine, conn = _engine_with(fetch_all=[(7, "蒙西储能一期", "DAF_a.pdf", 512, "GO", "2026-09-04")])
    rows = list_dafs(engine)
    assert rows[0]["deal_name"] == "蒙西储能一期"
    assert rows[0]["recommendation"] == "GO"


def test_load_daf_returns_bytes_and_filename():
    engine, conn = _engine_with(fetch_one=(b"%PDF-data", "DAF_a.pdf"))
    pdf, name = load_daf(engine, 7)
    assert pdf == b"%PDF-data" and name == "DAF_a.pdf"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `~/.venvs/bess-platform/bin/python -m pytest services/deal_committee/tests/test_library.py -v`
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: Write the implementation**

```sql
-- db/ddl/marketdata/deal_committee.sql
CREATE TABLE IF NOT EXISTS marketdata.deal_briefs (
    id           SERIAL PRIMARY KEY,
    deal_name    TEXT NOT NULL,
    brief        JSONB NOT NULL,
    confirmed    BOOLEAN NOT NULL DEFAULT FALSE,
    source_files TEXT[],
    created_at   TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS marketdata.deal_daf_library (
    id             SERIAL PRIMARY KEY,
    brief_id       INTEGER REFERENCES marketdata.deal_briefs(id),
    deal_name      TEXT NOT NULL,
    filename       TEXT NOT NULL,
    pdf_data       BYTEA NOT NULL,
    file_size_kb   INTEGER,
    recommendation TEXT,
    created_at     TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_deal_daf_brief ON marketdata.deal_daf_library(brief_id);
```

```python
# services/deal_committee/library.py
"""Persist deal briefs and generated DAF PDFs to marketdata.* (idempotent DDL)."""
from __future__ import annotations

from pathlib import Path

from sqlalchemy import text

from services.deal_committee.brief import DealBrief

_DDL_PATH = Path(__file__).resolve().parents[3] / "db" / "ddl" / "marketdata" / "deal_committee.sql"


def ensure_tables(engine) -> None:
    with engine.begin() as conn:
        for stmt in _DDL_PATH.read_text(encoding="utf-8").split(";"):
            if stmt.strip():
                conn.execute(text(stmt))


def save_brief(engine, brief: DealBrief) -> int:
    ensure_tables(engine)
    sql = text("""
        INSERT INTO marketdata.deal_briefs (deal_name, brief, confirmed, source_files)
        VALUES (:name, CAST(:brief AS jsonb), :confirmed, :files)
        RETURNING id
    """)
    with engine.begin() as conn:
        row = conn.execute(sql, {
            "name": brief.deal_name or "(未命名)",
            "brief": brief.model_dump_json(),
            "confirmed": brief.confirmed,
            "files": brief.source_files or None,
        }).fetchone()
    return int(row[0])


def save_daf(engine, brief_id: int, brief: DealBrief, pdf_bytes: bytes,
             filename: str, recommendation: str) -> int:
    ensure_tables(engine)
    sql = text("""
        INSERT INTO marketdata.deal_daf_library
            (brief_id, deal_name, filename, pdf_data, file_size_kb, recommendation)
        VALUES (:bid, :name, :filename, :pdf, :size_kb, :recommendation)
        RETURNING id
    """)
    with engine.begin() as conn:
        row = conn.execute(sql, {
            "bid": brief_id, "name": brief.deal_name or "(未命名)",
            "filename": filename, "pdf": pdf_bytes,
            "size_kb": max(1, len(pdf_bytes) // 1024),
            "recommendation": recommendation or None,
        }).fetchone()
    return int(row[0])


def list_dafs(engine, limit: int = 20) -> list[dict]:
    ensure_tables(engine)
    sql = text("""
        SELECT id, deal_name, filename, file_size_kb, recommendation, created_at
        FROM marketdata.deal_daf_library
        ORDER BY id DESC LIMIT :n
    """)
    with engine.connect() as conn:
        rows = conn.execute(sql, {"n": limit}).fetchall()
    return [{"id": r[0], "deal_name": r[1], "filename": r[2], "file_size_kb": r[3],
             "recommendation": r[4], "created_at": str(r[5])} for r in rows]


def load_daf(engine, daf_id: int) -> tuple[bytes, str]:
    sql = text("SELECT pdf_data, filename FROM marketdata.deal_daf_library WHERE id = :i")
    with engine.connect() as conn:
        row = conn.execute(sql, {"i": daf_id}).fetchone()
    if row is None:
        raise KeyError(f"DAF id={daf_id} 不存在")
    return bytes(row[0]), row[1]
```

- [ ] **Step 4: Run tests**

Run: `~/.venvs/bess-platform/bin/python -m pytest services/deal_committee/tests/ -v`
Expected: all PASS (25 tests)

- [ ] **Step 5: Commit**

```bash
git add services/deal_committee/library.py services/deal_committee/tests/test_library.py db/ddl/marketdata/deal_committee.sql
git commit -m "Add deal brief/DAF persistence to marketdata with idempotent DDL"
```

---

### Task 11: `intake_tab.py` (Tab 0 · Deal Intake)

**Files:**
- Create: `apps/deal_structurer/intake_tab.py`
- Modify: `apps/deal_structurer/app.py` (sidebar radio + routing)

**Interfaces:**
- Consumes: `extract_text` (T2), `extract_brief`, `DealBrief`, `low_confidence_fields` (T1/T3), `save_brief` (T10), `services.common.db_utils.get_engine`, `shared.anthropic_client.is_llm_available`.
- Produces: `render() -> None`; writes confirmed brief to `st.session_state["deal_brief"]` and DB id to `st.session_state["deal_brief_id"]` (consumed by Task 12).

- [ ] **Step 1: Write the implementation** (Streamlit tab — no unit tests per project convention; logic lives in tested modules. Smoke-verified in Task 13.)

```python
"""Tab 0 · Deal Intake — upload deal docs or manual entry → confirmed DealBrief."""
from __future__ import annotations

import os

import streamlit as st

from services.deal_committee.brief import DealBrief, extract_brief, low_confidence_fields
from services.deal_committee.intake_parser import SUPPORTED_EXTS, extract_text


def _brief_form(draft: DealBrief) -> DealBrief | None:
    low = set(low_confidence_fields(draft))

    def _warn(field: str):
        if field in low:
            st.caption(f"⚠️ 提取置信度较低,请核对 {field}")

    with st.form("deal_brief_form"):
        c1, c2 = st.columns(2)
        with c1:
            deal_name = st.text_input("项目名称", draft.deal_name); _warn("deal_name")
            asset_type = st.selectbox("资产类型",
                                      ["bess", "wind", "solar", "wind_bess", "solar_bess"],
                                      index=["bess", "wind", "solar", "wind_bess",
                                             "solar_bess"].index(draft.asset_type))
            province = st.text_input("省份", draft.province); _warn("province")
            node = st.text_input("节点(可选)", draft.node or "")
            capacity_mw = st.number_input("储能功率 (MW)", 0.0, 2000.0,
                                          float(draft.capacity_mw)); _warn("capacity_mw")
            capacity_mwh = st.number_input("储能容量 (MWh)", 0.0, 8000.0,
                                           float(draft.capacity_mwh)); _warn("capacity_mwh")
            efficiency = st.number_input("综合效率", 0.5, 1.0, float(draft.efficiency), 0.01)
            cycles = st.number_input("日均循环次数", 0.1, 4.0, float(draft.cycles_per_day), 0.1)
        with c2:
            installed_mw = st.number_input("新能源装机 (MW)", 0.0, 5000.0,
                                           float(draft.installed_mw)); _warn("installed_mw")
            capex_yi = st.number_input("总投资 (亿元)", 0.0, 200.0,
                                       (draft.capex_total_yuan or 0.0) / 1e8, 0.1)
            _warn("capex_total_yuan")
            commissioning = st.number_input("投运年份", 2024, 2035,
                                            int(draft.commissioning_year))
            tenor = st.number_input("项目期限 (年)", 1, 40, int(draft.tenor_years))
            counterparty = st.text_input("对手方", draft.counterparty)
            debt = st.number_input("负债率", 0.0, 0.95, float(draft.debt_ratio), 0.05)
            rate = st.number_input("贷款利率", 0.0, 0.30, float(draft.loan_rate), 0.005,
                                   format="%.3f")
            term = st.number_input("贷款期限 (年)", 1, 30, int(draft.loan_term_years))
        notes = st.text_area("交易结构要点", draft.structure_notes, height=80)
        submitted = st.form_submit_button("✅ 确认交易要素", type="primary",
                                          use_container_width=True)
    if not submitted:
        return None
    return DealBrief(
        deal_name=deal_name, asset_type=asset_type, province=province,
        node=node or None, capacity_mw=capacity_mw, capacity_mwh=capacity_mwh,
        efficiency=efficiency, cycles_per_day=cycles, installed_mw=installed_mw,
        capex_total_yuan=capex_yi * 1e8 or None, commissioning_year=int(commissioning),
        tenor_years=int(tenor), counterparty=counterparty, structure_notes=notes,
        debt_ratio=debt, loan_rate=rate, loan_term_years=int(term),
        field_confidence=draft.field_confidence, confirmed=True,
        source_files=draft.source_files,
    )


def _persist(brief: DealBrief) -> None:
    try:
        from services.common.db_utils import get_engine
        from services.deal_committee.library import save_brief
        st.session_state["deal_brief_id"] = save_brief(get_engine(), brief)
    except Exception as e:
        st.session_state["deal_brief_id"] = None
        st.warning(f"要素已保存在会话中,但写入数据库失败:{e}")


def render() -> None:
    st.header("0 · Deal Intake — 交易要素录入")
    st.caption("上传交易背景材料(docx / pptx / pdf / xlsx / txt),自动提取交易要素;"
               "确认后进入 6 · 投委会 生成投资建议书(DAF)。")

    from shared.anthropic_client import is_llm_available
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    llm_ok = is_llm_available(api_key)

    uploaded = st.file_uploader("交易背景材料(可多选)", type=list(SUPPORTED_EXTS),
                                accept_multiple_files=True)
    c1, c2 = st.columns(2)
    with c1:
        extract_btn = st.button("📄 解析文档并提取要素", disabled=not uploaded or not llm_ok,
                                use_container_width=True)
    with c2:
        manual_btn = st.button("✏️ 手工录入", use_container_width=True)
    if not llm_ok:
        st.warning("未检测到 LLM 配置(ANTHROPIC_API_KEY 或 BEDROCK_REGION)——文档提取不可用,可手工录入。")

    if extract_btn:
        texts, names = [], []
        for f in uploaded:
            try:
                texts.append(extract_text(f.getvalue(), f.name, api_key=api_key))
                names.append(f.name)
            except Exception as e:
                st.error(f"{f.name}:{e}")
        if texts:
            with st.spinner("正在提取交易要素…"):
                try:
                    draft = extract_brief("\n\n---\n\n".join(texts), names, api_key)
                    st.session_state["_draft_brief"] = draft
                except Exception as e:
                    st.error(f"要素提取失败:{e}")
    if manual_btn:
        st.session_state["_draft_brief"] = DealBrief()

    draft = st.session_state.get("_draft_brief")
    if draft is not None:
        st.divider()
        st.subheader("交易要素确认")
        brief = _brief_form(draft)
        if brief is not None:
            st.session_state["deal_brief"] = brief
            st.session_state["_draft_brief"] = None
            _persist(brief)
            st.success(f"交易要素已确认:{brief.deal_name or '(未命名)'} —— 请切换到 6 · 投委会")

    existing = st.session_state.get("deal_brief")
    if existing is not None and draft is None:
        st.info(f"当前已确认要素:**{existing.deal_name or '(未命名)'}** "
                f"({existing.province} · {existing.asset_type})。重新上传或手工录入可覆盖。")
```

Modify `apps/deal_structurer/app.py`:
- In the sidebar radio list, change `["1 · Price Simulation", ..., "💬 Strategist"]` to `["0 · Deal Intake", "1 · Price Simulation", ..., "5 · Deal Pricing", "6 · 投委会", "💬 Strategist"]`.
- Add routing branches before the existing ones:

```python
if tab_choice == "0 · Deal Intake":
    from apps.deal_structurer import intake_tab; intake_tab.render()
```
(and Task 12 adds the `"6 · 投委会"` branch.)

- [ ] **Step 2: Verify module imports and app boots**

Run: `~/.venvs/bess-platform/bin/python -c "import ast; ast.parse(open('apps/deal_structurer/intake_tab.py').read()); ast.parse(open('apps/deal_structurer/app.py').read()); print('syntax ok')"`
Expected: `syntax ok`

- [ ] **Step 3: Commit**

```bash
git add apps/deal_structurer/intake_tab.py apps/deal_structurer/app.py
git commit -m "Add deal intake tab with document extraction and brief confirmation"
```

---

### Task 12: `committee_tab.py` (Tab 6 · 投委会) + app routing

**Files:**
- Create: `apps/deal_structurer/committee_tab.py`
- Modify: `apps/deal_structurer/app.py` (routing branch)

**Interfaces:**
- Consumes: `run_committee`, `run_single_section`, `CommitteeResult`, `default_query_fn` (T6); `run_synthesis` (T7); `build_daf` (T9); `list_dafs`, `load_daf`, `save_daf` (T10); session keys `deal_brief`, `deal_brief_id` (T11).
- Produces: `render() -> None`.

- [ ] **Step 1: Write the implementation**

```python
"""Tab 6 · 投委会 — run committee analysis, synthesize, generate DAF PDF."""
from __future__ import annotations

import os

import streamlit as st

from services.deal_committee.orchestrator import (
    CommitteeResult, default_query_fn, run_committee, run_single_section,
)


def _api_key() -> str:
    return os.environ.get("ANTHROPIC_API_KEY", "")


def _rerun_section(key: str) -> None:
    result: CommitteeResult = st.session_state["committee_result"]
    with st.spinner("重新生成中…"):
        sec, econ = run_single_section(key, result.brief, default_query_fn, _api_key())
    for i, s in enumerate(result.sections):
        if s.key == key:
            result.sections[i] = sec
    if econ is not None:
        result.economics = econ
    result.synthesis = ""
    result.recommendation = ""


def render() -> None:
    st.header("6 · 投委会 — 投资决策建议书 (DAF)")
    brief = st.session_state.get("deal_brief")
    if brief is None or not brief.confirmed:
        st.warning("请先在 **0 · Deal Intake** 确认交易要素。")
        return

    st.caption(f"项目:**{brief.deal_name or '(未命名)'}** · {brief.province} · "
               f"{brief.asset_type} · {brief.capacity_mw:g}MW/{brief.capacity_mwh:g}MWh")

    if st.button("▶ 运行投委会分析", type="primary"):
        result = CommitteeResult(brief=brief, sections=[])
        with st.status("投委会分析运行中…", expanded=True) as status:
            def _done(sec):
                icon = "✅" if sec.status == "ok" else "❌"
                st.write(f"{icon} {sec.title}")
            result = run_committee(brief, api_key=_api_key(), on_section_done=_done)
            status.update(label="分析完成", state="complete")
        st.session_state["committee_result"] = result

    result: CommitteeResult | None = st.session_state.get("committee_result")
    if result is None:
        st.info("点击 **▶ 运行投委会分析** 开始。各章节将依次调用市场/量化/运营代理。")
        return

    for sec in result.sections:
        icon = "✅" if sec.status == "ok" else "❌"
        with st.expander(f"{icon} {sec.title}", expanded=False):
            if sec.status == "ok":
                st.markdown(sec.markdown)
            else:
                st.error(sec.error)
            if st.button("↻ 重新生成", key=f"rerun_{sec.key}"):
                _rerun_section(sec.key)
                st.rerun()

    st.divider()
    c1, c2 = st.columns(2)
    with c1:
        synth_btn = st.button("🧠 生成综合意见", use_container_width=True,
                              disabled=not any(s.status == "ok" for s in result.sections))
    with c2:
        pdf_btn = st.button("📄 生成并保存 DAF PDF", use_container_width=True,
                            disabled=not result.synthesis)

    if synth_btn:
        from services.deal_committee.synthesis import run_synthesis
        with st.spinner("综合意见生成中…"):
            try:
                result.synthesis, result.recommendation = run_synthesis(
                    result.brief, result.sections, result.economics, _api_key())
            except Exception as e:
                st.error(f"综合意见生成失败:{e}")
    if result.synthesis:
        if result.recommendation:
            st.metric("投资结论", result.recommendation)
        st.markdown(result.synthesis)

    if pdf_btn and result.synthesis:
        from services.deal_committee.daf_builder import build_daf
        from services.deal_committee.library import save_daf
        try:
            pdf = build_daf(result)
            st.session_state["_daf_pdf"] = pdf
            fname = f"DAF_{result.brief.deal_name or 'deal'}_{result.brief.province}.pdf"
            try:
                from services.common.db_utils import get_engine
                save_daf(get_engine(), st.session_state.get("deal_brief_id"),
                         result.brief, pdf, fname, result.recommendation)
                st.success("DAF 已保存到报告库")
            except Exception as e:
                st.warning(f"PDF 已生成,但保存到数据库失败:{e}")
        except Exception as e:
            st.error(f"PDF 生成失败:{e}")

    pdf = st.session_state.get("_daf_pdf")
    if pdf:
        st.download_button("⬇ 下载 DAF PDF", pdf,
                           file_name=f"DAF_{result.brief.deal_name or 'deal'}.pdf",
                           mime="application/pdf", use_container_width=True)

    st.divider()
    with st.expander("📚 历史 DAF"):
        try:
            from services.common.db_utils import get_engine
            from services.deal_committee.library import list_dafs, load_daf
            rows = list_dafs(get_engine())
            if not rows:
                st.caption("暂无历史 DAF。")
            for r in rows:
                c1, c2 = st.columns([4, 1])
                c1.write(f"**{r['deal_name']}** · {r['created_at'][:10]} · "
                         f"{r['recommendation'] or '—'} · {r['file_size_kb']} KB")
                if c2.button("下载", key=f"daf_{r['id']}"):
                    data, fname = load_daf(get_engine(), r["id"])
                    st.download_button("⬇", data, file_name=fname,
                                       mime="application/pdf", key=f"dl_{r['id']}")
        except Exception as e:
            st.caption(f"报告库不可用:{e}")
```

Modify `apps/deal_structurer/app.py`: add routing branch after `"5 · Deal Pricing"`:

```python
elif tab_choice == "6 · 投委会":
    from apps.deal_structurer import committee_tab; committee_tab.render()
```

- [ ] **Step 2: Verify syntax + full test suite**

Run: `~/.venvs/bess-platform/bin/python -c "import ast; ast.parse(open('apps/deal_structurer/committee_tab.py').read()); print('ok')"` and `~/.venvs/bess-platform/bin/python -m pytest services/deal_committee/tests/ -v`
Expected: `ok`; 25 PASS

- [ ] **Step 3: Commit**

```bash
git add apps/deal_structurer/committee_tab.py apps/deal_structurer/app.py
git commit -m "Add investment committee tab with analysis, synthesis, DAF PDF"
```

---

### Task 13: Dockerfile + requirements + local verification

**Files:**
- Modify: `apps/deal_structurer/Dockerfile`
- Modify: `apps/deal_structurer/requirements.txt`

**Interfaces:**
- Consumes: everything above.

- [ ] **Step 1: Update `apps/deal_structurer/requirements.txt`**

Append (versions unpinned unless already pinned in the file):

```
reportlab
matplotlib
python-docx
python-pptx
openpyxl
xlrd
```

Then check what `services/knowledge_pool/knowledge_docs.py` imports for PDF parsing (`grep -n "import" services/knowledge_pool/knowledge_docs.py | head -30`) and add that PDF library (e.g. `pypdf` / `pdfplumber`) to requirements too. Also check module-level imports of `services/hermes/market_agent_bridge.py` (should be stdlib-only) and of the five headless agents used (`services/bess_map`, `services/mengxi_trading`, `services/asset_risk`, `services/retail_risk`, plus `services/spot_mcp/tools.py` for the bridge's spot branch) — add any third-party deps they import that are not already in requirements (likely candidates: `pulp`, `requests`, `lxml`). Record what you added and why in the commit message.

- [ ] **Step 2: Update `apps/deal_structurer/Dockerfile`**

After the existing `COPY shared/ ./shared/` line, add:

```dockerfile
COPY libs/decision_models/     ./libs/decision_models/
COPY services/deal_committee/  ./services/deal_committee/
COPY services/knowledge_pool/  ./services/knowledge_pool/
COPY services/spot_mcp/        ./services/spot_mcp/
COPY services/bess_map/        ./services/bess_map/
COPY services/mengxi_trading/  ./services/mengxi_trading/
COPY services/asset_risk/      ./services/asset_risk/
COPY services/retail_risk/     ./services/retail_risk/
COPY services/hermes/__init__.py            ./services/hermes/__init__.py
COPY services/hermes/market_agent_bridge.py ./services/hermes/market_agent_bridge.py
COPY services/hermes/export_utils.py        ./services/hermes/export_utils.py
```

Notes for the implementer:
- Only copy additional `services/*` dirs that Step 1's import audit shows are actually imported (some headless agents may pull sibling modules — follow the imports; e.g. `services/bess_map/headless_agent.py` may import `services/bess_mcp` or `libs/decision_models`).
- Add CJK fonts for better PDF rendering before the `pip install` layer finishes:

```dockerfile
RUN apt-get update && apt-get install -y --no-install-recommends fonts-wqy-microhei \
    && rm -rf /var/lib/apt/lists/*
```

(reportlab falls back to built-in STSong-Light if fonts are absent — the build must not fail if apt is unavailable.)

- [ ] **Step 3: Local Docker build**

```bash
docker build --platform linux/amd64 -f apps/deal_structurer/Dockerfile -t bess-platform-deal-structurer:dev .
```

Expected: build succeeds. If a COPY fails (missing path), revisit Step 1's audit.

- [ ] **Step 4: Run tests inside the image**

```bash
docker run --rm --platform linux/amd64 bess-platform-deal-structurer:dev \
  python -m pytest services/deal_committee/tests/ -v
```

Expected: 25 PASS inside the container.

- [ ] **Step 5: App smoke (manual, with the user)**

```bash
docker run --rm -p 8522:8522 --platform linux/amd64 --env-file config/.env \
  bess-platform-deal-structurer:dev
```

Verify with the user in the browser at `http://localhost:8522/deal-structurer/`:
1. Tab 0 renders; manual entry → confirm brief succeeds
2. Tab 6 runs analysis (agent sections need network to RDS + Bedrock/API key — expect real sections to take 1–4 min); failed sections show ❌ with error and the pipeline completes
3. 生成综合意见 → 生成 DAF PDF → download opens a valid Chinese PDF
4. 历史 DAF lists the saved report

- [ ] **Step 6: Commit**

```bash
git add apps/deal_structurer/Dockerfile apps/deal_structurer/requirements.txt
git commit -m "Add deal committee deps and services to deal-structurer image"
```

---

## Self-Review Notes

**Spec coverage:**
- Intake (doc upload + extraction + confirmation hard gate) → Tasks 1–3, 11 ✓
- 7-section pipeline via `run_market_query` + in-process economics + rm_* risk table → Tasks 4–6 ✓ (spec's `ops_evidence` split into its 3 constituent agent sections, as designed in brainstorming)
- Grounded synthesis with GO/有条件 GO/NO-GO → Task 7 ✓
- DAF PDF 8-section layout in Chinese with tables/charts → Tasks 8–9 ✓ (DAF sections 2/6/7 from synthesis; appendix = section 8 of the layout, numbered 十 in the PDF)
- Library persistence (`marketdata.deal_briefs` + `marketdata.deal_daf_library` + DDL file) → Task 10 ✓
- Error handling (per-section isolation, timeouts, LLM guard, PDF failure retention, low-confidence gate) → Tasks 6, 11, 12 ✓
- Docker/deploy changes + verification → Task 13 ✓; ECS redeploy explicitly out of this plan (requires user confirmation)
- Testing (unit + integration with stubs + smoke) → per-task tests + Task 13 smoke ✓

**Type consistency:** `SectionResult(key, title, markdown, status, error)`, `CommitteeResult(brief, sections, economics, synthesis, recommendation)`, `EconomicsResult(mc, monthly_price, n_price_hours, n_simulations, model)`, `run_single_section(...) -> (SectionResult, EconomicsResult|None)` — same names used in Tasks 6/7/9/12. Test fixtures (`_fake_econ`, `BRIEF`) imported from `test_orchestrator` in `test_daf_builder` — consistent.
