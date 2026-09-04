# Hermes App Links + Internet Approval — Design Spec

**Date:** 2026-07-26  
**Branch:** feat/deal-structurer-bedrock-migration  
**Scope:** Two features:
1. Link Hermes to deal-structurer, asset-risk, and retail-risk apps via new headless agents
2. Internet answer approval flow before committing findings to the knowledge base

---

## Feature 1: Three New Headless Agents

### Goal

Allow Hermes to answer questions about deal structuring, asset risk, and retail risk by routing to dedicated headless agents that call the same tool functions used by the Streamlit apps.

### Architecture

Three new files following the existing `services/mengxi_trading/headless_agent.py` pattern:

| File | Entry function | Tools |
|------|---------------|-------|
| `services/deal_structurer/headless_agent.py` | `run_deal_query()` | `run_price_simulation`, `run_dispatch_valuation`, `run_project_cashflow`, `run_monte_carlo`, `price_deal_structure` |
| `services/asset_risk/headless_agent.py` | `run_asset_risk_query()` | `get_book_pnl`, `get_position_mtm`, `get_var`, `get_asset_list` |
| `services/retail_risk/headless_agent.py` | `run_retail_risk_query()` | `get_retail_margin`, `get_procurement_coverage`, `get_customer_pnl_ranking`, `get_contract_expiry_pipeline` |

Each agent:
- Imports tool functions from the existing app/library layer (no new DB queries written)
- Wraps each tool with a Claude tool-use schema (JSON `input_schema`)
- Runs the standard agentic loop: build messages → call Claude → dispatch tools → loop until `end_turn`
- Uses `claude-sonnet-4-6` model, `max_tokens=2048`
- Reads expert insights at start via `get_relevant_insights()` + `inject_expert_memory()`
- Writes new insights at `end_turn` via `extract_spot_insights()`
- Accepts `api_key` and `pg_url` parameters matching all other headless agents

### Tool imports

- **Deal structurer**: `from libs.deal_models.adapters.agent_tools import run_price_simulation, run_dispatch_valuation, run_project_cashflow, run_monte_carlo, price_deal_structure`
- **Asset risk**: DB query functions extracted from `apps/asset_risk/tab_agent.py` — the internal `_get_book_pnl()`, `_get_position_mtm()`, `_get_var()`, `_get_asset_list()` helper functions (or equivalent SQL calls)
- **Retail risk**: DB query functions extracted from `apps/retail_risk/tab_agent.py` — same pattern

If the internal helpers aren't importable (they live inside a Streamlit callback), they will be refactored into standalone functions in the same file so they can be imported by both the Streamlit tab and the headless agent.

### System prompts

Each agent gets a focused system prompt:

**Deal structurer**: "You are a deal structuring analyst for power assets. You use financial models to evaluate PPA structures, project IRR, capacity/energy revenue splits, and dispatch valuations. Always call a tool before stating any financial figure."

**Asset risk**: "You are an asset risk analyst. You track book P&L, position mark-to-market, value at risk (VaR), and portfolio exposure across the asset book. Always call a tool before stating any metric."

**Retail risk**: "You are a retail risk analyst. You monitor retail margins, procurement coverage ratios, customer P&L rankings, and contract expiry pipelines. Always call a tool before stating any metric."

All prompts include: respond in the same language as the question; quote currency units explicitly; state the date range used.

### Routing updates

**`services/hermes/market_agent_bridge.py`** — add three new `if` blocks:

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

Also update the `return` error message at the bottom to include the new keys.

**`services/hermes/agent.py`** system prompt — extend the MARKET_AGENT routing rules:

```
- Deal structuring, PPA pricing, project IRR, capacity/energy revenue, dispatch valuation,
  offtake contracts, deal structure → market: "deal"
- Book P&L, position mark-to-market, VaR, Greeks, portfolio exposure, asset risk → market: "asset-risk"
- Retail margins, customer P&L, procurement coverage, contract expiry pipeline, retail portfolio → market: "retail-risk"
```

---

## Feature 2: Internet Answer Approval Flow

### Goal

After Hermes retrieves an internet answer, show the user an interactive Feishu card asking whether to commit the finding to the knowledge base. On approval, store the insight and confirm what was learned.

### Flow

```
User question
     │
     ▼
run_internet_query() → full answer text
     │
     ├──► Send answer to user (immediately, as today)
     │
     └──► Store {question, answer} in _pending_internet[open_id]
          Send Feishu interactive card (separate message):
          ┌─────────────────────────────────────────┐
          │ 🌐 Internet Answer — Save to KB?        │
          │                                         │
          │ Q: <user question>                      │
          │ A: <first 300 chars>...                 │
          │                                         │
          │  [✓ Approve & Save]  [✗ Skip]           │
          └─────────────────────────────────────────┘
                    │                │
             Approve button    Skip button
                    │                │
                    ▼                ▼
        extract_spot_insights()  Discard pending entry
        (background thread)
                    │
                    ▼
        Send confirmation: "✓ Saved to knowledge base: [question text]"
```

### Implementation

**`services/hermes/app.py`**:

1. Add module-level dict: `_pending_internet: dict[str, dict] = {}`  
   (key = `open_id`, value = `{"question": str, "answer": str, "api_key": str, "ts": float}`)

2. In the internet answer handler (after `run_internet_query()`):
   - Send the full answer to the user as normal
   - Store entry in `_pending_internet[open_id]`
   - Call `send_interactive_card_internet_approval(open_id, question, answer[:300])`

3. Add `send_interactive_card_internet_approval()` — builds a Feishu interactive card with:
   - Header: "🌐 Save to Knowledge Base?"
   - Body: question + truncated answer preview
   - Two buttons: `action_id = "internet_approve"` / `"internet_reject"`
   - Same card-building pattern as existing `send_interactive_card()` calls

4. In the existing `/feishu/action` (or equivalent card callback) handler:
   - If `action_id == "internet_approve"`: pop from `_pending_internet[open_id]`, call `extract_spot_insights()` synchronously (fast single LLM call), send "✓ Saved to knowledge base: [question]"
   - If `action_id == "internet_reject"`: pop and discard, send "✗ Discarded."

5. Stale entry cleanup: on any new message from `open_id`, discard `_pending_internet` entries older than 24h.

**`services/hermes/internet_agent.py`**: No changes needed. `run_internet_query()` continues to return the answer string as-is.

### Error handling

- If `extract_spot_insights()` fails (network, API), log the error and send "⚠ Save failed — answer not stored."
- If the user approves but the pending entry has expired (> 24h), send "⏱ Approval expired — please ask again to re-search."

---

## Files Changed

| File | Change |
|------|--------|
| `services/deal_structurer/headless_agent.py` | **New** |
| `services/asset_risk/headless_agent.py` | **New** |
| `services/retail_risk/headless_agent.py` | **New** |
| `apps/asset_risk/tab_agent.py` | Extract DB helpers to importable functions (if needed) |
| `apps/retail_risk/tab_agent.py` | Extract DB helpers to importable functions (if needed) |
| `services/hermes/market_agent_bridge.py` | Add 3 new route blocks + update error message |
| `services/hermes/agent.py` | Extend routing rules in system prompt |
| `services/hermes/app.py` | Add `_pending_internet` dict + approval card send + callback handler |

---

## Out of Scope

- New database tables or migrations (expert insights reuse existing `staging.kp_expert_insights`)
- Changes to `libs/deal_models/` model logic
- Changes to existing Streamlit app UI
- Authentication / per-user permissions for the new agents
