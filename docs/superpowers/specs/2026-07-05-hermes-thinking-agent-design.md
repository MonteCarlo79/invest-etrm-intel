# Hermes Thinking Agent — Design Spec
**Date:** 2026-07-05  
**Status:** Approved

---

## Overview

Add proactive "thinking" capability to Hermes. Instead of only executing scheduled tasks or responding to user messages, Hermes periodically reviews the health of the platform's data and the design of its apps, then sends focused observations and questions to the user via Feishu.

---

## Architecture

A new `ThinkingAgent` class in `services/hermes/thinking_agent.py`. Separate from `HermesAgent` — same Anthropic client, but a different system prompt and a multi-step tool loop instead of a single JSON-action dispatch.

Two modes, same class:

| Mode | Trigger | Default model |
|------|---------|---------------|
| `health` | Daily, 00:10 UTC (08:10 Beijing), after morning briefing | `claude-haiku-4-5-20251001` |
| `design` | Weekly, Monday 00:30 UTC (08:30 Beijing) | `claude-sonnet-4-6` |

Both models overridable via env vars:
- `HERMES_THINK_HEALTH_MODEL`
- `HERMES_THINK_DESIGN_MODEL`

Same model alias map as `HermesAgent` (`gpt`, `deepseek`, `claude`, `auto`).

---

## Tool Loop

```
ThinkingAgent.run(mode)
  → build seed prompt
  → call Claude with tools
  → Claude calls tools iteratively until satisfied
  → Claude calls send_feishu_message(text)
  → log run to hermes_thinking_log
```

Max iterations: 8 tool calls per run (cost guard).  
Max file reads: 5 source files per design review run.

### Available Tools

| Tool | Signature | Purpose |
|------|-----------|---------|
| `query_db` | `(sql: str) → str` | Read-only SELECT; results as markdown table (max 50 rows) |
| `check_etl_freshness` | `() → str` | Returns last-updated timestamp for each monitored table |
| `read_source_file` | `(path: str) → str` | Read a repo `.py` file (max 300 lines, repo-relative path) |
| `list_app_files` | `(app: str) → str` | List `.py` files under `apps/<app>/` or `services/<app>/` |
| `send_feishu_message` | `(text: str) → str` | Send message to owner via Feishu; terminates the loop |

`query_db` enforces read-only: rejects any SQL containing `INSERT`, `UPDATE`, `DELETE`, `DROP`, `ALTER`, `TRUNCATE`.

---

## Health Check Mode

**Seed prompt:**
> You are Hermes. Check the health of the BESS platform data.
> Use `check_etl_freshness` to see what is stale, `query_db` to spot gaps or anomalies.
> When you find something worth the user's attention, use `send_feishu_message` to ask one focused question.
> For each issue, include the upstream data source (exchange website, government portal, data provider) so the user knows where to go get the missing data.
> If everything looks healthy, call `send_feishu_message` with an empty string to signal silence.
> Be concise. Do not send noise.

**What she checks:**

| Signal | Query / Method | Threshold |
|--------|---------------|-----------|
| Table freshness | `check_etl_freshness()` | >2 days stale → flag |
| Mengxi P&L gaps | `SELECT date FROM ops_pnl WHERE date > NOW()-30d` | Missing dates for any of 4 IM assets |
| Nodal backfill | `SELECT MAX(date) FROM nodal_pf_annual WHERE year = EXTRACT(year FROM NOW())` | NULL → flag |
| News screener yield | `SELECT COUNT(*) FROM news_articles WHERE created_at > NOW()-1d` | 0 → flag |
| Zero-result screeners | `SELECT * FROM hermes_thinking_log WHERE mode='health' ORDER BY ts DESC LIMIT 1` | Compare table counts |

**Silence rule:** If `send_feishu_message` is called with empty string, nothing is sent. No noise on healthy days.

**Example output:**
> 注意到山东现货价格数据最后更新是2天前（7月3日）。
> 📍 原始数据来源：山东电力交易中心 → 每日结算公告 PDF（spot-watcher 服务自动抓取）。
> ETL 是否遇到问题，还是数据源暂停发布？

---

## Design Review Mode

**Seed prompt:**
> You are Hermes. It is Monday. Review the design and operations of the platform apps.
> Use `list_app_files` and `read_source_file` to read app code. Use `query_db` to understand what data exists.
> Form 2–3 specific, actionable observations or questions. Focus on:
> - Missing features that would make apps more useful
> - Data that exists in the DB but is not surfaced in any app
> - Inconsistencies between apps (e.g. one app has IRR, another doesn't)
> - UX gaps or confusing flows
> Avoid generic suggestions. Be direct and specific.
> Check `hermes_thinking_log` first — do not repeat observations from the last 14 days.
> Send via `send_feishu_message`.

**Apps reviewed (rotating weekly to stay within 5-file limit):**

| Week | Apps |
|------|------|
| Week A (odd) | `spot-market`, `bess-map`, `hermes` agent |
| Week B (even) | `mengxi-dashboard`, `gb-market`, `hermes` scheduler |

**Example output:**
> 本周平台观察：
> 1. `bess-map` 有 IRR 估算，但 `mengxi-dashboard` 没有显示每个资产的实际 IRR 对比理论值。这个对比有没有价值加进去？
> 2. `ops_pnl` 表里有2026年全年数据，但日报只展示最近30天——是否希望加一个季度视图？
> 3. `spot-market` 的 Strategist KB 搜索只索引了山东/广东/蒙西——其他省份的文件入库了吗？

---

## Data Lineage Map

Baked into the `ThinkingAgent` system prompt as a static reference. Maps monitored tables to their upstream external source:

| Table | Upstream Source |
|-------|----------------|
| `spot_prices` | 各省电力交易中心网站 → 日结算公告 PDF（spot-watcher 抓取）|
| `bess_economics` | 各省交易中心 + 内部 MILP 调度模型计算 |
| `ops_pnl` | 内蒙古电力交易中心 → 15分钟结算数据（ops_ingestion 服务）|
| `province_installed_monthly` | 国家能源局 / 各省能源局官网 → 月度装机容量报告 Excel |
| `exchange_reports` | 北京/上海/广州电力交易中心 → 月度市场报告 PDF |
| `news_articles` | RSS 订阅 + 各交易中心公告页（news_screener 服务）|
| `nodal_pf_annual` | 内部计算：station_master × MILP × 全年现货价格 |
| `province_cap_comp` | 各省调频/容量补偿政策文件（capcomp_screener 网络搜索）|
| `gb_prices` | Elexon API (api.bmreports.com) |
| `au_prices` | AEMO Data Archive (aemo.com.au) |

---

## Database Migration

New table `hermes_thinking_log`:

```sql
-- db/ddl/hermes/005_thinking_log.sql
CREATE TABLE IF NOT EXISTS hermes_thinking_log (
    id             SERIAL PRIMARY KEY,
    mode           TEXT NOT NULL,          -- 'health' | 'design'
    ts             TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    files_read     TEXT[],                 -- source files read this run
    tables_checked TEXT[],                 -- DB tables queried
    message_sent   TEXT,                   -- full text sent to Feishu (for dedup)
    model_used     TEXT
);
CREATE INDEX ON hermes_thinking_log (mode, ts DESC);
```

**Dedup rule:** Before sending, agent checks if the same observation (fuzzy match on first 80 chars) was sent within 7 days (health) or 14 days (design). If yes, suppress.

---

## Scheduler Changes (`services/hermes/app.py`)

Two new `scheduler.add_job` calls added after the morning briefing job:

```python
# Health check: daily 00:10 UTC (08:10 Beijing) — after morning briefing
scheduler.add_job(
    thinking_agent.run,
    "cron", hour=0, minute=10,
    kwargs={"mode": "health"},
)

# Design review: every Monday 00:30 UTC (08:30 Beijing)
scheduler.add_job(
    thinking_agent.run,
    "cron", day_of_week="mon", hour=0, minute=30,
    kwargs={"mode": "design"},
)
```

---

## Development Request Pipeline

### Motivation

Hermes runs on ECS with a personal Anthropic token (cost-sensitive). Heavy code development should use the company Claude token on the user's laptop. The pipeline splits responsibilities:

| Stage | Where | Token used |
|-------|-------|-----------|
| Observe + reason + spec | ECS (Hermes) | Personal (Haiku — cheap) |
| Develop + test + deploy | Laptop (Claude Code) | Company |

### Trigger

Two ways Hermes creates a dev request:

1. **Design review auto-generates** — when she identifies an improvement during weekly review, she writes a request file in addition to the Feishu message
2. **User asks explicitly** — e.g. "Hermes，帮我记录一个需求：在 mengxi-dashboard 加 IRR 对比" → she reasons about it and writes the file

### Output File Format

Files written to OneDrive: `etrm/bess-platform/dev-requests/YYYY-MM-DD-<slug>.md`

```markdown
# Dev Request: <title>
**Created:** YYYY-MM-DD HH:MM by Hermes (ThinkingAgent)
**Priority:** high | medium | low
**Triggered by:** design review | health check | user request
**Status:** pending

## Context
<Why this matters — what Hermes observed or what the user asked>

## Requested Change
<What needs to be built or fixed, described precisely>

## Files to Touch
- `path/to/file.py` — what to change
- `path/to/other.py` — read-only reference

## Data Sources / APIs
<Which DB tables, agent tools, or external APIs are relevant>

## Acceptance Criteria
- [ ] Criterion 1
- [ ] Criterion 2
- [ ] No new DB tables unless specified

## Notes
<Hermes's observations about edge cases, similar existing code, etc.>
```

### Laptop Pickup

OneDrive syncs the file to `C:\Users\dipeng.chen\OneDrive\etrm\bess-platform\dev-requests\` automatically. User opens the file in Claude Code and executes with the company token. No automated watcher needed — the Feishu notification is the trigger.

### Feishu Notification

After writing the file, Hermes sends:
> 📝 已记录开发需求：`dev-requests/2026-07-05-fix-irr-display.md`
> 同步到 OneDrive 后可用公司 Claude token 拾取开发。

### Completion Signal

When development is done, user can:
- Reply to Hermes in Feishu: "IRR 需求已完成" → Hermes marks it done in the thinking log
- Or manually update the file's `Status: completed` field

### New Tool for ThinkingAgent

| Tool | Signature | Purpose |
|------|-----------|---------|
| `write_dev_request` | `(slug: str, content: str) → str` | Write `.md` to OneDrive dev-requests folder via OneDriveClient |

---

## Files Changed / Created

| File | Change |
|------|--------|
| `services/hermes/thinking_agent.py` | **New** — `ThinkingAgent` class with `write_dev_request` tool |
| `db/ddl/hermes/005_thinking_log.sql` | **New** — `hermes_thinking_log` table |
| `services/hermes/app.py` | **Edit** — import `ThinkingAgent`, 2 new scheduler jobs |

No changes to `HermesAgent`, `scheduler.py`, or any app code.

---

## Cost Estimate

| Mode | Model | Est. tokens/run | Est. cost/month |
|------|-------|----------------|----------------|
| Health check (daily) | Haiku | ~3k in + 1k out | ~$0.30 |
| Design review (weekly) | Sonnet | ~15k in + 2k out | ~$0.80 |
| Dev request write (on-demand) | Haiku | ~2k in + 1k out | ~$0.10 |
| **Total** | | | **~$1.20/month** |
