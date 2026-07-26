# Hermes App Links + Internet Approval — Session Handover
**Date:** 2026-07-26  
**Branch:** `feat/deal-structurer-bedrock-migration`  
**Last commit:** `1e05f96` — fix(hermes): add msg_type=interactive to update_card PATCH

---

## What Was Completed This Session

### Feature 1: Hermes linked to deal-structurer, asset-risk, and retail-risk apps

Three new headless agents were created and wired into Hermes routing:

| File | Entry function | Status |
|------|---------------|--------|
| `services/deal_structurer/headless_agent.py` | `run_deal_query(question, api_key, pg_url="")` | ✅ Done |
| `services/asset_risk/headless_agent.py` | `run_asset_risk_query(question, api_key, pg_url)` | ✅ Done |
| `services/retail_risk/headless_agent.py` | `run_retail_risk_query(question, api_key, pg_url)` | ✅ Done |

Routing files updated:
- **`services/hermes/market_agent_bridge.py`** — 3 new `if market in (...)` blocks added for `deal`, `asset-risk`, `retail-risk`
- **`services/hermes/agent.py`** — SYSTEM_PROMPT extended with topic descriptions and Chinese keyword auto-detection for deal/asset-risk/retail-risk; users don't need to type market keys

Key implementation notes:
- `services/deal_structurer/__init__.py` created (empty, required for package import)
- `apps/asset_risk/tab_agent.py` and `apps/retail_risk/tab_agent.py`: `tools` list moved from inside `_call_agent()` to module level so headless agents can import it
- Deal structurer agent accepts `pg_url` for interface consistency but doesn't use it (documented in docstring — deal models are pure Python)
- Asset/retail risk agents call `_make_engine(pg_url)` which raises `ValueError` with clear message if no DB URL configured

### Feature 2: Internet answer approval flow

After Hermes retrieves an internet answer, a Feishu interactive card is sent asking the user to approve before storing the finding in the knowledge base.

**Files changed:** `services/hermes/app.py`
- `_pending_internet: dict[str, dict]` added at module level (key = `open_id`)
- `_build_internet_approval_card(question, answer_preview)` — orange header card with Approve/Skip buttons
- Trigger block: after `run_internet_query()` returns, stores Q&A in `_pending_internet` and sends approval card
- `internet_approve` handler: pops entry, calls `extract_spot_insights()`, sends confirmation
- `internet_reject` handler: pops and discards
- Stale entries (>24h) return `"⏱ Approval expired"` toast
- If `extract_spot_insights()` fails: logs error, sends `"⚠ Save failed"`

### Bug fix: 每日提醒 "完成" button didn't remove tasks

**File changed:** `services/hermes/feishu_client.py:107`

```python
# Before (broken)
json={"content": json.dumps(card)}

# After (fixed)
json={"msg_type": "interactive", "content": json.dumps(card)}
```

Root cause: Feishu's `PATCH /im/v1/messages/{message_id}` requires `msg_type` in the body. Without it the API silently failed, leaving completed tasks still visible in the briefing card. The DB row was correctly marked done, but the card never refreshed.

### Deployment

All changes deployed to ECS task definition `bess-platform-hermes:160` on cluster `bess-platform-cluster`, service `bess-platform-hermes-svc` (ap-southeast-1).

---

## Architecture Reference

### Headless agent pattern (all agents follow this)
```python
_SYSTEM = "..."  # focused system prompt

def _make_client(api_key): ...
def _make_engine(pg_url): ...   # only DB-backed agents

def run_*_query(question: str, api_key: str, pg_url: str = "") -> str:
    client = _make_client(api_key)
    # Inject expert memory (READ path)
    system = _SYSTEM
    try:
        insights = get_relevant_insights(question, limit=4)
        mem_block = inject_expert_memory(insights)
        if mem_block: system += f"\n\n{mem_block}"
    except Exception: pass

    messages = [{"role": "user", "content": question}]
    while True:
        resp = client.messages.create(model="claude-sonnet-4-6", max_tokens=2048,
                                      system=system, tools=..., messages=messages)
        messages += [{"role": "assistant", "content": resp.content}]
        if resp.stop_reason == "end_turn":
            answer = next((b.text for b in resp.content if hasattr(b, "text")), "")
            try: extract_spot_insights(user_msg=question, agent_reply=answer, api_key=api_key)
            except Exception: pass
            return answer
        tool_results = [...]
        if not tool_results:
            return next((b.text for b in resp.content if hasattr(b, "text")), "")
        messages += [{"role": "user", "content": tool_results}]
```

### Feishu `_pending_*` dict pattern
Module-level dicts in `services/hermes/app.py` keyed by `open_id` for stateful Feishu card interactions:
- `_pending_internet[open_id]` — internet approval (Q&A awaiting user approval)
- `_pending_survey`, `_pending_folders`, `_pending_reroute`, etc. — same pattern

Card actions arrive at `POST /hermes/inbound/feishu-card`. The handler reads `payload.get("action", {}).get("value", {}).get("act", "")` and dispatches.

### Daily briefing card flow
- Built by `build_task_card()` in `services/hermes/scheduler.py`
- Sent by `send_morning_briefing()` via `feishu.send_card()`
- "完成" button value: `{"act": "done_task", "task_id": id, "title": title}`
- Callback in `app.py`: extracts `message_id = payload.get("open_message_id", "")`, runs `_bg_done_task` async background task
- `_bg_done_task`: marks done in DB → `list_open_cards()` → `build_task_card()` → `feishu.update_card(message_id, card)`

---

## Pending Tasks (not done this session)

| # | Task | Notes |
|---|------|-------|
| 1 | Fix DB: `thermal_mw=2812` for 冀南 in `province_installed_monthly` table | Old data issue |
| 2 | Submit Anthropic use-case form in AWS Bedrock Console | Required for Bedrock model access |
| 3 | Review and commit unstaged changes in `apps/gb-market/` | Various modified files not yet committed |

---

## Key Files Quick Reference

| File | Purpose |
|------|---------|
| `services/hermes/app.py` | Main FastAPI app; card callbacks, internet approval flow |
| `services/hermes/agent.py` | `HermesAgent` class; SYSTEM_PROMPT with routing rules |
| `services/hermes/market_agent_bridge.py` | Routes `market` param to correct headless agent |
| `services/hermes/scheduler.py` | Morning briefing card builder (`build_task_card`) |
| `services/hermes/feishu_client.py` | Feishu API client (`send_card`, `update_card`, etc.) |
| `services/hermes/tasks_client.py` | DB helpers for `hermes_tasks` (`complete_card`, `list_open_cards`) |
| `services/mengxi_trading/headless_agent.py` | Reference implementation for headless agent pattern |
| `services/deal_structurer/headless_agent.py` | NEW: deal structuring agent |
| `services/asset_risk/headless_agent.py` | NEW: asset risk agent |
| `services/retail_risk/headless_agent.py` | NEW: retail risk agent |
| `scripts/deploy_hermes.sh` | ECR build + push + ECS force-deploy |

---

## How to Continue

1. **Read this file** for context
2. **Check out the branch:** `git checkout feat/deal-structurer-bedrock-migration`
3. **Latest commit:** `git log --oneline -5`
4. Address pending tasks above, or pick up any new issues the user raises

The design spec is at `docs/superpowers/specs/2026-07-26-hermes-app-links-internet-approval-design.md` and the implementation plan at `docs/superpowers/plans/2026-07-26-hermes-app-links-internet-approval.md`.
