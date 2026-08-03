# Hermes 完成 Button Fix — Session Handover
**Date:** 2026-08-04  
**Branch:** `feat/deal-structurer-bedrock-migration`  
**Last commit:** `37e7976` — fix(hermes): handle new Feishu 事件与回调 card callback format  
**Deployed:** `bess-platform-hermes:161`

---

## Problem Diagnosed This Session

### Symptom
Clicking 完成 on the daily morning briefing task card showed no response (no toast, no card refresh). Same overdue tasks appeared every morning.

### Root Cause (Two-part)

**Part 1 — Card Action Request URL not configured (fixed first)**  
Feishu's card button callbacks were never reaching the server. CloudWatch logs showed zero `POST /hermes/inbound/feishu-card` requests ever received. The Card Action Request URL was simply not set in the Feishu developer console.

**Fix:** In the Feishu developer console (open.feishu.cn), under **事件与回调 → 回调配置**, added:
- `消息卡片回传交互（旧）` (`card.action.trigger_v1`) subscription
- Request URL: `https://www.pjh-etrm.ai/hermes/inbound/feishu-card`

Note: the new `卡片回传交互` (card.action.trigger) was grayed out because our cards use the old format (no `"schema": "2.0"`). The `消息卡片回传交互（旧）` is the correct one.

**Part 2 — Payload format mismatch (the card update bug)**  
After configuring the URL, the toast showed (✅ 已完成：...) and DB tasks were being marked done. But the card wasn't refreshing. 

Root cause: the new `事件与回调` system sends `card.action.trigger_v1` callbacks in a **different payload structure** from the old bot 卡片请求网址:

| Field | Old format (bot settings) | New format (事件与回调) |
|-------|--------------------------|----------------------|
| `open_id` | `payload.open_id` | `payload.event.operator.open_id` |
| `action` | `payload.action` | `payload.event.action` |
| `open_message_id` | `payload.open_message_id` | `payload.event.context.open_message_id` |

The code was extracting `open_message_id` from `payload.open_message_id` which returned `""` in the new format → `if not _message_id: return` exited early → card never updated.

**Fix:** `services/hermes/app.py` — the card callback parser now tries both formats:
```python
_event     = payload.get("event") or payload
open_id    = (_event.get("operator") or {}).get("open_id", "") or payload.get("open_id", "")
action     = _event.get("action") or payload.get("action", {})
_ctx       = _event.get("context") or {}
_open_msg_id = _ctx.get("open_message_id", "") or payload.get("open_message_id", "")
```

---

## Current State

| Item | Status |
|------|--------|
| Feishu Card Action URL configured | ✅ Done |
| `消息卡片回传交互（旧）` subscribed | ✅ Done |
| Payload format fix in code | ✅ Done, commit `37e7976` |
| Deployed | ✅ `bess-platform-hermes:161` |
| 完成 button — toast shows | ✅ Confirmed working |
| 完成 button — card refreshes | ⚠️ Deployed in :161, **not yet confirmed by user** |

---

## What To Do Next

1. **Verify the card refresh works** — user clicks 完成 on today's morning briefing; task should disappear from the card immediately.

2. **Check CloudWatch logs** after a click to confirm `mid=om_...` is now being captured:
   ```bash
   export PATH="/c/Program Files/Amazon/AWSCLIV2:$PATH"
   aws logs filter-log-events \
     --region ap-southeast-1 \
     --log-group-identifier "arn:aws:logs:ap-southeast-1:319383842493:log-group:/ecs/bess-platform" \
     --log-stream-name-prefix "hermes" \
     --filter-pattern "feishu-card act=" \
     --start-time $(( $(date +%s) * 1000 - 1800000 )) --limit 20
   ```
   Look for `mid=om_...` (non-empty). If `mid=` is still empty, the payload format is different from both known variants — add `logger.info("full payload: %s", payload)` to capture the raw JSON.

3. **Optionally remove the `logger.info` debug line** (app.py:1613) once confirmed working — it logs every card click including open_id.

---

## Infrastructure Reference

| Item | Value |
|------|-------|
| ECS cluster | `bess-platform-cluster` |
| ECS service | `bess-platform-hermes-svc` |
| Current task def | `bess-platform-hermes:161` |
| App URL | `https://www.pjh-etrm.ai` |
| Card callback URL | `https://www.pjh-etrm.ai/hermes/inbound/feishu-card` |
| CloudWatch log group | `/ecs/bess-platform` (stream prefix: `hermes`) |
| Deploy script | `bash scripts/deploy_hermes.sh` (requires aws in PATH — run from Claude Code bash or add `export PATH="/c/Program Files/Amazon/AWSCLIV2:$PATH"` first) |

## Deploy Command (from Claude Code bash)
```bash
export PATH="/c/Program Files/Amazon/AWSCLIV2:$PATH"
cd "C:\Users\dipeng.chen\OneDrive\ETRM\bess-platform"
bash scripts/deploy_hermes.sh
```

---

## Key Files

| File | Purpose |
|------|---------|
| `services/hermes/app.py:1600` | Card callback handler — payload parsing fix is here |
| `services/hermes/feishu_client.py:103` | `update_card()` — PATCH im/v1/messages with msg_type=interactive |
| `services/hermes/tasks_client.py:77` | `complete_card()` — marks task done in DB |
| `services/hermes/scheduler.py:161` | `build_task_card()` — builds the refreshed card |
| `scripts/deploy_hermes.sh` | ECR build + push + ECS force-deploy |
