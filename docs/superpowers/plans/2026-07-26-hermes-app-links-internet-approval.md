# Hermes App Links + Internet Approval — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Link Hermes to deal-structurer, asset-risk, and retail-risk apps via headless agents with auto-domain routing, and add an internet answer approval flow before committing findings to the knowledge base.

**Architecture:** Three new headless agents (`services/deal_structurer/`, `services/asset_risk/`, `services/retail_risk/`) follow the existing `mengxi_trading` pattern — tool definitions + agentic loop. Routing updated in `market_agent_bridge.py` and `agent.py` system prompt. Internet approval uses a `_pending_internet` dict in `app.py` + Feishu interactive card, matching the existing `_pending_survey` pattern.

**Tech Stack:** Python, Anthropic SDK (`claude-sonnet-4-6`), SQLAlchemy, FastAPI (Feishu card callbacks), existing Feishu card JSON format.

---

## File Map

| Action | File | Purpose |
|--------|------|---------|
| Create | `services/deal_structurer/headless_agent.py` | Deal structuring headless agent (no DB, pure models) |
| Create | `services/asset_risk/headless_agent.py` | Asset risk headless agent (DB queries) |
| Create | `services/retail_risk/headless_agent.py` | Retail risk headless agent (DB queries) |
| Modify | `services/hermes/market_agent_bridge.py` | Add 3 new route blocks |
| Modify | `services/hermes/agent.py:28-256` | Extend SYSTEM_PROMPT with new market keys + routing rules |
| Modify | `services/hermes/app.py:23-36` | Add `_pending_internet` dict |
| Modify | `services/hermes/app.py:~435` | Add `_build_internet_approval_card()` function |
| Modify | `services/hermes/app.py:~1528` | Add `internet_approve`/`internet_reject` card callbacks |
| Modify | `services/hermes/app.py:~4311` | Trigger internet approval card after sending internet reply |

---

## Task 1: Deal Structurer headless agent

**Files:**
- Create: `services/deal_structurer/headless_agent.py`
- Create: `tests/services/deal_structurer/test_headless_agent.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/services/deal_structurer/test_headless_agent.py
from unittest.mock import patch, MagicMock
from services.deal_structurer.headless_agent import run_deal_query

def test_run_deal_query_returns_string():
    mock_resp = MagicMock()
    mock_resp.stop_reason = "end_turn"
    mock_resp.content = [MagicMock(text="IRR is 12%")]
    mock_resp.content[0].type = "text"

    mock_client = MagicMock()
    mock_client.messages.create.return_value = mock_resp

    with patch("services.deal_structurer.headless_agent._make_client", return_value=mock_client):
        result = run_deal_query("What is the IRR for a 100MWh BESS in 蒙西?", api_key="test-key")

    assert isinstance(result, str)
    assert len(result) > 0
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cd C:/Users/dipeng.chen/OneDrive/ETRM/bess-platform
py -m pytest tests/services/deal_structurer/test_headless_agent.py -v
```

Expected: FAIL with `ModuleNotFoundError` or `ImportError`

- [ ] **Step 3: Create the headless agent**

```python
# services/deal_structurer/headless_agent.py
"""Headless Deal Structurer agent.

Routes deal structuring questions to the deal model tools:
run_price_simulation, run_dispatch_valuation, run_project_cashflow,
run_monte_carlo, price_deal_structure.

Usage:
    from services.deal_structurer.headless_agent import run_deal_query
    answer = run_deal_query("蒙西100MWh BESS的IRR?", api_key, pg_url)
"""
from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

_SYSTEM = """\
You are a deal structuring analyst for power assets. \
You build financial models to evaluate BESS and wind projects: \
PPA structures, project IRR, equity IRR, DSCR, NPV, capacity/energy revenue splits, \
dispatch valuations, and deal structure pricing.

## Workflow
1. Use run_price_simulation to simulate price paths for the target province.
2. Use run_dispatch_valuation to estimate annual revenue from the price paths.
3. Use run_project_cashflow to compute IRR/NPV for given capex/opex assumptions.
4. Use run_monte_carlo for a full probabilistic analysis in one step.
5. Use price_deal_structure to price a revenue floor/cap/collar/swap/tolling/PPA.

## Rules
- Always call a tool before stating any financial figure (IRR, NPV, revenue).
- Quote all monetary values in CNY (元); MWh for energy.
- State all model assumptions explicitly (kappa, mu, sigma, capex, debt ratio, etc.).
- Respond concisely with actionable insights.
- Respond in the same language as the question (Chinese or English).
"""


def _make_client(api_key: str):
    from shared.anthropic_client import make_client
    return make_client(api_key)


def run_deal_query(question: str, api_key: str, pg_url: str = "") -> str:
    """Run the deal structurer headless agent and return its answer."""
    from libs.deal_models.adapters.agent_tools import AGENT_TOOLS, dispatch_tool

    client = _make_client(api_key)

    system = _SYSTEM
    try:
        from services.knowledge_pool.expert_memory import get_relevant_insights, inject_expert_memory
        insights = get_relevant_insights(question, limit=4)
        mem_block = inject_expert_memory(insights)
        if mem_block:
            system += f"\n\n{mem_block}"
    except Exception:
        pass

    messages = [{"role": "user", "content": question}]
    while True:
        resp = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=2048,
            system=system,
            tools=AGENT_TOOLS,
            messages=messages,
        )
        messages = messages + [{"role": "assistant", "content": resp.content}]
        if resp.stop_reason == "end_turn":
            answer = next((b.text for b in resp.content if hasattr(b, "text")), "")
            try:
                from services.knowledge_pool.expert_memory import extract_spot_insights
                extract_spot_insights(user_msg=question, agent_reply=answer, api_key=api_key)
            except Exception:
                pass
            return answer
        tool_results = []
        for block in resp.content:
            if block.type == "tool_use":
                result_str = dispatch_tool(block.name, block.input)
                tool_results.append({
                    "type": "tool_result",
                    "tool_use_id": block.id,
                    "content": result_str,
                })
        if not tool_results:
            return next((b.text for b in resp.content if hasattr(b, "text")), "")
        messages = messages + [{"role": "user", "content": tool_results}]
```

- [ ] **Step 4: Create `tests/services/deal_structurer/__init__.py`** (empty)

```bash
mkdir -p C:/Users/dipeng.chen/OneDrive/ETRM/bess-platform/tests/services/deal_structurer
touch C:/Users/dipeng.chen/OneDrive/ETRM/bess-platform/tests/services/deal_structurer/__init__.py
```

- [ ] **Step 5: Run test to verify it passes**

```bash
cd C:/Users/dipeng.chen/OneDrive/ETRM/bess-platform
py -m pytest tests/services/deal_structurer/test_headless_agent.py -v
```

Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add services/deal_structurer/headless_agent.py tests/services/deal_structurer/
git commit -m "feat: add deal structurer headless agent"
```

---

## Task 2: Asset Risk headless agent

**Files:**
- Create: `services/asset_risk/headless_agent.py`
- Create: `tests/services/asset_risk/test_headless_agent.py`

Note: The tool schemas and `_execute_tool()` are imported directly from `apps/asset_risk/tab_agent.py` — no copy-paste needed.

- [ ] **Step 1: Write the failing test**

```python
# tests/services/asset_risk/test_headless_agent.py
from unittest.mock import patch, MagicMock
from services.asset_risk.headless_agent import run_asset_risk_query

def test_run_asset_risk_query_returns_string():
    mock_resp = MagicMock()
    mock_resp.stop_reason = "end_turn"
    mock_resp.content = [MagicMock(text="Book 1 P&L is +500,000 CNY")]
    mock_resp.content[0].type = "text"

    mock_client = MagicMock()
    mock_client.messages.create.return_value = mock_resp

    with patch("services.asset_risk.headless_agent._make_client", return_value=mock_client):
        with patch("services.asset_risk.headless_agent._make_engine", return_value=MagicMock()):
            result = run_asset_risk_query("What is the P&L for book 1?", api_key="test-key", pg_url="postgresql://test")

    assert isinstance(result, str)
    assert len(result) > 0
```

- [ ] **Step 2: Run test to verify it fails**

```bash
py -m pytest tests/services/asset_risk/test_headless_agent.py -v
```

Expected: FAIL with `ModuleNotFoundError`

- [ ] **Step 3: Extract `tools` list to module level in `apps/asset_risk/tab_agent.py`**

Read `apps/asset_risk/tab_agent.py`. The `tools` list at lines 46-81 is inside `_call_agent()`. Move it to module level (before `def render_agent`):

```python
# Add at module level (after imports, before def render_agent):
tools = [
    {
        "name": "get_book_pnl",
        "description": "Get P&L breakdown by category for a book.",
        "input_schema": {
            "type": "object",
            "properties": {
                "book_id": {"type": "integer"},
            },
            "required": ["book_id"],
        },
    },
    {
        "name": "get_position_mtm",
        "description": "Get current MtM summary with unrealised P&L for a book.",
        "input_schema": {
            "type": "object",
            "properties": {"book_id": {"type": "integer"}},
            "required": ["book_id"],
        },
    },
    {
        "name": "get_var",
        "description": "Get current VaR figures for a book.",
        "input_schema": {
            "type": "object",
            "properties": {"book_id": {"type": "integer"}},
            "required": ["book_id"],
        },
    },
    {
        "name": "get_asset_list",
        "description": "Get list of registered assets and their books.",
        "input_schema": {"type": "object", "properties": {}},
    },
]
```

Then in `_call_agent()`, delete the inline `tools = [...]` block (lines 46-81) — the function will now use the module-level `tools`.

- [ ] **Step 4: Create the headless agent**

```python
# services/asset_risk/headless_agent.py
"""Headless Asset Risk agent.

Routes asset risk questions to DB query tools:
get_book_pnl, get_position_mtm, get_var, get_asset_list.

Usage:
    from services.asset_risk.headless_agent import run_asset_risk_query
    answer = run_asset_risk_query("Book 1 P&L?", api_key, pg_url)
"""
from __future__ import annotations

import json
import logging
import os

logger = logging.getLogger(__name__)

_SYSTEM = """\
You are an asset risk management analyst for a Chinese electricity trading company. \
You track book P&L, position mark-to-market, value at risk (VaR), \
and portfolio exposure across the asset book.

## Rules
1. Always call get_asset_list first if you don't know the book_id.
2. Always call a tool before stating any P&L, VaR, or MTM figure.
3. Quote all monetary values in CNY (元); MWh for energy volumes.
4. Respond concisely with actionable risk insights.
5. Respond in the same language as the question (Chinese or English).
"""


def _make_client(api_key: str):
    from shared.anthropic_client import make_client
    return make_client(api_key)


def _make_engine(pg_url: str):
    url = pg_url or os.environ.get("PGURL") or os.environ.get("DATABASE_URL", "")
    if url.startswith("postgres://"):
        url = "postgresql://" + url[len("postgres://"):]
    if url.startswith("postgresql://") and not url.startswith("postgresql+psycopg2://"):
        url = "postgresql+psycopg2://" + url[len("postgresql://"):]
    from sqlalchemy import create_engine
    return create_engine(url, pool_pre_ping=True, connect_args={"connect_timeout": 10})


def run_asset_risk_query(question: str, api_key: str, pg_url: str = "") -> str:
    """Run the asset risk headless agent and return its answer."""
    from apps.asset_risk.tab_agent import tools as _tools, _execute_tool

    client = _make_client(api_key)
    engine = _make_engine(pg_url)

    system = _SYSTEM
    try:
        from services.knowledge_pool.expert_memory import get_relevant_insights, inject_expert_memory
        insights = get_relevant_insights(question, limit=4)
        mem_block = inject_expert_memory(insights)
        if mem_block:
            system += f"\n\n{mem_block}"
    except Exception:
        pass

    messages = [{"role": "user", "content": question}]
    while True:
        resp = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=2048,
            system=system,
            tools=_tools,
            messages=messages,
        )
        messages = messages + [{"role": "assistant", "content": resp.content}]
        if resp.stop_reason == "end_turn":
            answer = next((b.text for b in resp.content if hasattr(b, "text")), "")
            try:
                from services.knowledge_pool.expert_memory import extract_spot_insights
                extract_spot_insights(user_msg=question, agent_reply=answer, api_key=api_key)
            except Exception:
                pass
            return answer
        tool_results = []
        for block in resp.content:
            if block.type == "tool_use":
                result = _execute_tool(block.name, block.input, engine)
                tool_results.append({
                    "type": "tool_result",
                    "tool_use_id": block.id,
                    "content": json.dumps(result, default=str),
                })
        if not tool_results:
            return next((b.text for b in resp.content if hasattr(b, "text")), "")
        messages = messages + [{"role": "user", "content": tool_results}]
```

- [ ] **Step 5: Create `tests/services/asset_risk/__init__.py`**

```bash
mkdir -p C:/Users/dipeng.chen/OneDrive/ETRM/bess-platform/tests/services/asset_risk
touch C:/Users/dipeng.chen/OneDrive/ETRM/bess-platform/tests/services/asset_risk/__init__.py
```

- [ ] **Step 6: Run test to verify it passes**

```bash
py -m pytest tests/services/asset_risk/test_headless_agent.py -v
```

Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add apps/asset_risk/tab_agent.py services/asset_risk/headless_agent.py tests/services/asset_risk/
git commit -m "feat: add asset risk headless agent"
```

---

## Task 3: Retail Risk headless agent

**Files:**
- Create: `services/retail_risk/headless_agent.py`
- Create: `tests/services/retail_risk/test_headless_agent.py`

Same pattern as Task 2.

- [ ] **Step 1: Write the failing test**

```python
# tests/services/retail_risk/test_headless_agent.py
from unittest.mock import patch, MagicMock
from services.retail_risk.headless_agent import run_retail_risk_query

def test_run_retail_risk_query_returns_string():
    mock_resp = MagicMock()
    mock_resp.stop_reason = "end_turn"
    mock_resp.content = [MagicMock(text="Top customer margin is 200,000 CNY")]
    mock_resp.content[0].type = "text"

    mock_client = MagicMock()
    mock_client.messages.create.return_value = mock_resp

    with patch("services.retail_risk.headless_agent._make_client", return_value=mock_client):
        with patch("services.retail_risk.headless_agent._make_engine", return_value=MagicMock()):
            result = run_retail_risk_query("Top 5 customers by margin?", api_key="test-key", pg_url="postgresql://test")

    assert isinstance(result, str)
    assert len(result) > 0
```

- [ ] **Step 2: Run test to verify it fails**

```bash
py -m pytest tests/services/retail_risk/test_headless_agent.py -v
```

Expected: FAIL with `ModuleNotFoundError`

- [ ] **Step 3: Extract `tools` list to module level in `apps/retail_risk/tab_agent.py`**

Read `apps/retail_risk/tab_agent.py`. The `tools` list at lines 48-93 is inside `_call_agent()`. Move it to module level (before `def render_agent`):

```python
# Add at module level (after imports, before def render_agent):
tools = [
    {
        "name": "get_retail_margin",
        "description": "Get retail margin breakdown (revenue, procurement, T&D, penalties, net) for a customer or all customers.",
        "input_schema": {
            "type": "object",
            "properties": {
                "customer_id": {"type": "integer", "description": "Customer ID. Omit for all customers."},
            },
            "required": [],
        },
    },
    {
        "name": "get_procurement_coverage",
        "description": "Get procurement coverage ratio (forward-bought / contracted load) for a book.",
        "input_schema": {
            "type": "object",
            "properties": {
                "book_id": {"type": "integer", "description": "Book ID to check coverage for."},
            },
            "required": ["book_id"],
        },
    },
    {
        "name": "get_customer_pnl_ranking",
        "description": "Get customers ranked by net P&L contribution, showing top N.",
        "input_schema": {
            "type": "object",
            "properties": {
                "top_n": {"type": "integer", "description": "Number of top customers to return. Default 10."},
            },
            "required": [],
        },
    },
    {
        "name": "get_contract_expiry_pipeline",
        "description": "Get contracts expiring soon, grouped by province and contract type.",
        "input_schema": {
            "type": "object",
            "properties": {
                "days_ahead": {"type": "integer", "description": "Look ahead days. Default 90."},
            },
            "required": [],
        },
    },
]
```

Then in `_call_agent()`, replace the inline `tools = [...]` block with `# tools defined at module level`.

- [ ] **Step 4: Create the headless agent**

```python
# services/retail_risk/headless_agent.py
"""Headless Retail Risk agent.

Routes retail risk questions to DB query tools:
get_retail_margin, get_procurement_coverage, get_customer_pnl_ranking,
get_contract_expiry_pipeline.

Usage:
    from services.retail_risk.headless_agent import run_retail_risk_query
    answer = run_retail_risk_query("Top 5 customers by margin?", api_key, pg_url)
"""
from __future__ import annotations

import json
import logging
import os

logger = logging.getLogger(__name__)

_SYSTEM = """\
You are a retail electricity risk management analyst for a Chinese energy trading company. \
You monitor customer margins, procurement coverage, P&L rankings, and contract expiry pipelines.

## Rules
1. Always call a tool before stating any margin, coverage, or ranking figure.
2. Quote all monetary values in CNY (元); MWh for energy volumes.
3. Highlight any customers with negative margins or low procurement coverage.
4. Respond concisely with actionable insights.
5. Respond in the same language as the question (Chinese or English).
"""


def _make_client(api_key: str):
    from shared.anthropic_client import make_client
    return make_client(api_key)


def _make_engine(pg_url: str):
    url = pg_url or os.environ.get("PGURL") or os.environ.get("DATABASE_URL", "")
    if url.startswith("postgres://"):
        url = "postgresql://" + url[len("postgres://"):]
    if url.startswith("postgresql://") and not url.startswith("postgresql+psycopg2://"):
        url = "postgresql+psycopg2://" + url[len("postgresql://"):]
    from sqlalchemy import create_engine
    return create_engine(url, pool_pre_ping=True, connect_args={"connect_timeout": 10})


def run_retail_risk_query(question: str, api_key: str, pg_url: str = "") -> str:
    """Run the retail risk headless agent and return its answer."""
    from apps.retail_risk.tab_agent import tools as _tools, _execute_tool

    client = _make_client(api_key)
    engine = _make_engine(pg_url)

    system = _SYSTEM
    try:
        from services.knowledge_pool.expert_memory import get_relevant_insights, inject_expert_memory
        insights = get_relevant_insights(question, limit=4)
        mem_block = inject_expert_memory(insights)
        if mem_block:
            system += f"\n\n{mem_block}"
    except Exception:
        pass

    messages = [{"role": "user", "content": question}]
    while True:
        resp = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=2048,
            system=system,
            tools=_tools,
            messages=messages,
        )
        messages = messages + [{"role": "assistant", "content": resp.content}]
        if resp.stop_reason == "end_turn":
            answer = next((b.text for b in resp.content if hasattr(b, "text")), "")
            try:
                from services.knowledge_pool.expert_memory import extract_spot_insights
                extract_spot_insights(user_msg=question, agent_reply=answer, api_key=api_key)
            except Exception:
                pass
            return answer
        tool_results = []
        for block in resp.content:
            if block.type == "tool_use":
                result = _execute_tool(block.name, block.input, engine)
                tool_results.append({
                    "type": "tool_result",
                    "tool_use_id": block.id,
                    "content": json.dumps(result, default=str),
                })
        if not tool_results:
            return next((b.text for b in resp.content if hasattr(b, "text")), "")
        messages = messages + [{"role": "user", "content": tool_results}]
```

- [ ] **Step 5: Create `tests/services/retail_risk/__init__.py`**

```bash
mkdir -p C:/Users/dipeng.chen/OneDrive/ETRM/bess-platform/tests/services/retail_risk
touch C:/Users/dipeng.chen/OneDrive/ETRM/bess-platform/tests/services/retail_risk/__init__.py
```

- [ ] **Step 6: Run test to verify it passes**

```bash
py -m pytest tests/services/retail_risk/test_headless_agent.py -v
```

Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add apps/retail_risk/tab_agent.py services/retail_risk/headless_agent.py tests/services/retail_risk/
git commit -m "feat: add retail risk headless agent"
```

---

## Task 4: Update market_agent_bridge.py

**Files:**
- Modify: `services/hermes/market_agent_bridge.py`

Read the file first, then make the edits below.

- [ ] **Step 1: Add 3 new route blocks after the mengxi block**

In `services/hermes/market_agent_bridge.py`, find this block (around line 54):
```python
    if market in ("mengxi", "im", "inner-mongolia"):
        from services.mengxi_trading.headless_agent import run_mengxi_query
        return run_mengxi_query(question=question, api_key=api_key, pg_url=pg_url)
```

Add immediately after it:
```python
    if market in ("deal", "deal-structurer", "structurer"):
        from services.deal_structurer.headless_agent import run_deal_query
        return run_deal_query(question=question, api_key=api_key, pg_url=pg_url)

    if market in ("asset-risk", "risk", "book"):
        from services.asset_risk.headless_agent import run_asset_risk_query
        return run_asset_risk_query(question=question, api_key=api_key, pg_url=pg_url)

    if market in ("retail-risk", "retail"):
        from services.retail_risk.headless_agent import run_retail_risk_query
        return run_retail_risk_query(question=question, api_key=api_key, pg_url=pg_url)
```

- [ ] **Step 2: Update the error message at the end of `run_market_query()`**

Find the final return:
```python
    return f"Unknown market '{market}'. Available: gb, au, ercot, caiso, pjm, ph, po, bess-map, spot, mengxi, internet"
```

Replace with:
```python
    return f"Unknown market '{market}'. Available: gb, au, ercot, caiso, pjm, ph, po, bess-map, spot, mengxi, deal, asset-risk, retail-risk, internet"
```

- [ ] **Step 3: Verify syntax**

```bash
cd C:/Users/dipeng.chen/OneDrive/ETRM/bess-platform
py -m py_compile services/hermes/market_agent_bridge.py && echo "OK"
```

Expected: `OK`

- [ ] **Step 4: Commit**

```bash
git add services/hermes/market_agent_bridge.py
git commit -m "feat: add deal/asset-risk/retail-risk routes to market_agent_bridge"
```

---

## Task 5: Update Hermes agent.py system prompt

**Files:**
- Modify: `services/hermes/agent.py`

Read the file first. The SYSTEM_PROMPT is at lines 28-256.

- [ ] **Step 1: Add new domain descriptions to CAPABILITY AREAS**

Find the `📊 Trading Management` section (around line 44):
```
📊 Trading Management
  Inner Mongolia BESS assets, Mengxi trading P&L, dispatch schedules.
  Answer from KB context or use MARKET_AGENT(bess-map) for financial metrics and installed capacity.
```

Add after it:
```
💰 Deal Structuring
  Deal economics, IRR/NPV modelling, PPA pricing, revenue floors/caps, dispatch valuations.
  Use MARKET_AGENT(deal) for any deal structuring or project finance calculation.

📉 Asset Risk
  Book P&L, position mark-to-market, VaR, portfolio exposure.
  Use MARKET_AGENT(asset-risk) for any asset book risk question.

🏪 Retail Risk
  Customer margins, procurement coverage, contract expiry pipeline, customer P&L rankings.
  Use MARKET_AGENT(retail-risk) for any retail portfolio risk question.
```

- [ ] **Step 2: Add new market keys to the MARKET_AGENT params line**

Find:
```
MARKET_AGENT — ask a specialist market agent a data question
  params: {"market": "gb|au|ercot|caiso|pjm|ph|po|bess-map|spot|mengxi|internet", "question": "the full question to ask"}
```

Replace with:
```
MARKET_AGENT — ask a specialist market agent a data question
  params: {"market": "gb|au|ercot|caiso|pjm|ph|po|bess-map|spot|mengxi|deal|asset-risk|retail-risk|internet", "question": "the full question to ask"}
```

- [ ] **Step 3: Add routing rules for the new domains**

Find the block starting with:
```
- For MARKET_AGENT market keys: gb=GB/Great Britain...
```
(the long line starting at ~line 236)

Add these three bullet points after the existing MARKET_AGENT market keys line:
```
- Use MARKET_AGENT(deal) for any question about deal structuring, PPA pricing, revenue floors/caps/collars, project IRR/NPV/DSCR, dispatch valuation, Monte Carlo simulation, or project finance terms. These require financial model tools — do NOT use REPLY for these.
- Use MARKET_AGENT(asset-risk) for any question about book P&L, position mark-to-market (MtM), value at risk (VaR), unrealised P&L, portfolio exposure, or asking what assets/books exist. Key terms: 账面盈亏/头寸/风险敞口/VaR/book/position/MTM.
- Use MARKET_AGENT(retail-risk) for any question about retail customer margins, procurement coverage ratio, customer P&L ranking, contract expiry pipeline, or unhedged load. Key terms: 零售/代理/客户利润/合同到期/采购覆盖率/retail/coverage/margin.
```

- [ ] **Step 4: Remove the old structuring REPLY rule** (to avoid conflict)

Find:
```
- When user says "structuring", "term sheet", "market entry", "project financing", "条款", use REPLY drawing from KB context with: Market Context | Key Economics | Risk Factors | Recommendation.
```

Replace with:
```
- When user says "structuring", "term sheet", "project financing", "条款", "IRR", "NPV", "deal economics", use MARKET_AGENT(deal) — the deal agent has financial model tools. Only use REPLY if the user is asking a conceptual/policy question with no calculation needed.
```

- [ ] **Step 5: Verify syntax**

```bash
py -m py_compile services/hermes/agent.py && echo "OK"
```

Expected: `OK`

- [ ] **Step 6: Commit**

```bash
git add services/hermes/agent.py
git commit -m "feat: extend Hermes routing for deal/asset-risk/retail-risk domains"
```

---

## Task 6: Internet answer approval flow

**Files:**
- Modify: `services/hermes/app.py`

This task has 4 sub-parts. Read `services/hermes/app.py` at the relevant offsets before each edit.

### Part A: Add `_pending_internet` module-level dict

- [ ] **Step 1: Add the dict**

Find the existing pending dict declarations (around lines 23-36):
```python
_pending_folders: dict[str, tuple[str, int]] = {}
...
_pending_survey: dict[str, tuple[str, str]] = {}
```

Add after `_pending_survey`:
```python
# key=open_id, value={"question": str, "answer": str, "api_key": str, "ts": float}
_pending_internet: dict[str, dict] = {}
```

### Part B: Add the card builder function

- [ ] **Step 2: Add `_build_internet_approval_card()` function**

Find `_build_route_confirmed_card` (around line 435). Add this new function immediately after it:

```python
def _build_internet_approval_card(question: str, answer_preview: str) -> dict:
    """Feishu card asking user to approve saving an internet answer to the KB."""
    q_short = question[:120] + "…" if len(question) > 120 else question
    return {
        "config": {"wide_screen_mode": True},
        "header": {
            "template": "orange",
            "title": {"content": "🌐 Save to Knowledge Base?", "tag": "plain_text"},
        },
        "elements": [
            {"tag": "div", "text": {"tag": "lark_md", "content": f"**Q:** {q_short}"}},
            {"tag": "div", "text": {"tag": "lark_md", "content": f"**A (preview):** {answer_preview}"}},
            {"tag": "hr"},
            {"tag": "action", "actions": [
                {
                    "tag": "button",
                    "text": {"tag": "plain_text", "content": "✓ Approve & Save"},
                    "type": "primary",
                    "value": {"act": "internet_approve"},
                },
                {
                    "tag": "button",
                    "text": {"tag": "plain_text", "content": "✗ Skip"},
                    "type": "default",
                    "value": {"act": "internet_reject"},
                },
            ]},
        ],
    }
```

### Part C: Add card action callbacks

- [ ] **Step 3: Add `internet_approve` and `internet_reject` handlers in `feishu_card_inbound`**

In `feishu_card_inbound` (around line 1528), find the last `if act == ...` block before the final `return {}`. Add these two handlers:

```python
        if act == "internet_approve":
            import time as _time_mod
            entry = _pending_internet.pop(open_id, None)
            if entry is None:
                return {"toast": {"type": "fail", "content": "⏱ Approval expired — please ask again."}}
            if _time_mod.time() - entry.get("ts", 0) > 86400:
                return {"toast": {"type": "fail", "content": "⏱ Approval expired — please ask again."}}
            try:
                from services.knowledge_pool.expert_memory import extract_spot_insights
                extract_spot_insights(
                    user_msg=entry["question"],
                    agent_reply=entry["answer"],
                    api_key=entry["api_key"],
                )
                if feishu and open_id:
                    feishu.send_text(open_id=open_id, text=f"✓ Saved to knowledge base: {entry['question'][:100]}")
            except Exception as exc:
                logger.error("internet_approve: extract_spot_insights failed: %s", exc)
                if feishu and open_id:
                    feishu.send_text(open_id=open_id, text=f"⚠ Save failed: {exc}")
            return {}

        if act == "internet_reject":
            _pending_internet.pop(open_id, None)
            if feishu and open_id:
                feishu.send_text(open_id=open_id, text="✗ Discarded — answer not saved.")
            return {}
```

### Part D: Trigger approval card after sending internet answer

- [ ] **Step 4: Add card send after internet reply is sent to Feishu**

Find the block that sends the reply (around line 4311-4317):
```python
        if reply:
            ...
            if msg.source == "feishu" and feishu:
                feishu.send_text(open_id=msg.sender_id, text=reply)
```

After the `feishu.send_text(open_id=msg.sender_id, text=reply)` line (inside the `if msg.source == "feishu" and feishu:` block), add:

```python
                # Internet answer approval card
                if action.action == "MARKET_AGENT" and action.params.get("market") == "internet":
                    import time as _time_mod
                    _raw_answer = reply  # full reply including timestamp suffix
                    _q = action.params.get("question", msg.text)
                    _pending_internet[msg.sender_id] = {
                        "question": _q,
                        "answer": _raw_answer,
                        "api_key": os.environ.get("ANTHROPIC_API_KEY", ""),
                        "ts": _time_mod.time(),
                    }
                    _preview = _raw_answer[:300] + "…" if len(_raw_answer) > 300 else _raw_answer
                    try:
                        feishu.send_card(
                            open_id=msg.sender_id,
                            card=_build_internet_approval_card(_q, _preview),
                        )
                    except Exception as _card_err:
                        logger.warning("internet approval card send failed: %s", _card_err)
```

- [ ] **Step 5: Verify syntax**

```bash
py -m py_compile services/hermes/app.py && echo "OK"
```

Expected: `OK`

- [ ] **Step 6: Commit**

```bash
git add services/hermes/app.py
git commit -m "feat: add internet answer approval flow with Feishu card"
```

---

## Task 7: End-to-end smoke test

No automated test possible without live Feishu + DB. Verify manually:

- [ ] **Step 1: Run syntax check on all modified files**

```bash
cd C:/Users/dipeng.chen/OneDrive/ETRM/bess-platform
py -m py_compile services/deal_structurer/headless_agent.py && echo "deal OK"
py -m py_compile services/asset_risk/headless_agent.py && echo "asset OK"
py -m py_compile services/retail_risk/headless_agent.py && echo "retail OK"
py -m py_compile services/hermes/market_agent_bridge.py && echo "bridge OK"
py -m py_compile services/hermes/agent.py && echo "agent OK"
py -m py_compile services/hermes/app.py && echo "app OK"
```

Expected: all `OK`

- [ ] **Step 2: Run all unit tests**

```bash
py -m pytest tests/services/deal_structurer/ tests/services/asset_risk/ tests/services/retail_risk/ -v
```

Expected: 3 tests pass

- [ ] **Step 3: Deploy hermes to ECS**

```bash
# Build and push hermes image
docker build -t hermes:latest services/hermes/
# Tag and push to ECR (substitute your AWS account and region)
# aws ecr get-login-password --region ap-southeast-1 | docker login --username AWS --password-stdin <account>.dkr.ecr.ap-southeast-1.amazonaws.com
# docker tag hermes:latest <account>.dkr.ecr.ap-southeast-1.amazonaws.com/hermes:latest
# docker push <account>.dkr.ecr.ap-southeast-1.amazonaws.com/hermes:latest
# aws ecs update-service --cluster bess-platform --service hermes --force-new-deployment --region ap-southeast-1
```

- [ ] **Step 4: Manual Feishu test — deal routing**

Send in Feishu: `"帮我算一下蒙西100MWh BESS的IRR，capex 4亿，运维2000万每年"`

Expected: Hermes routes to MARKET_AGENT(deal), deal agent calls `run_price_simulation` + `run_dispatch_valuation` + `run_project_cashflow`, returns an IRR figure.

- [ ] **Step 5: Manual Feishu test — internet approval**

Send in Feishu: `"搜索一下最新储能政策"`

Expected: Hermes sends the internet answer, then immediately sends an orange approval card with "✓ Approve & Save" and "✗ Skip" buttons. Tap "✓ Approve & Save" → confirmation message appears.
