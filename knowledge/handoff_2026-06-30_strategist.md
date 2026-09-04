# Handoff — 2026-06-30 — Strategist + Hermes News/Reports

## How to pick up

Read this file, then start working. No need to re-read any other file unless you need specific implementation details.

---

## What was done this session

### Hermes — Market PDF Reports (`services/hermes/market_report.py`) ✅ LIVE td:104

New file. Generates daily + monthly China power market PDF reports from the Strategist knowledge base and sends via Feishu.

**Architecture:**
- `_query_articles(pg_url, from_dt)` — queries `staging.spot_knowledge_docs` by **`created_at`** (NOT `published_at`). Backfilled articles have old pub dates; using created_at ensures recently ingested articles always appear.
- Daily window: `now - 30h`, with 72h fallback if fewer than 5 articles found
- Articles capped at 50 (daily) / 100 (monthly) before prompt construction
- `_generate_report_content()` — calls Claude Sonnet 4.6 with **assistant prefill `{`** to force valid JSON output (avoids `Expecting ',' delimiter` parse errors from prose preamble). `_clean()` strips newlines from article text before prompt.
- `_build_pdf()` — ReportLab Platypus, A4, NotoSans CJK fonts, navy/steel colour scheme. Cover page + executive summary + sections.
- `send_daily_report()` / `send_monthly_report()` — sends Feishu card (summary) + PDF file

**Triggers:**
| Method | Daily | Monthly |
|--------|-------|---------|
| Scheduler | 07:00 UTC (15:00 BJ) | 1st of month, 09:00 UTC |
| HTTP | POST /hermes/reports/daily | POST /hermes/reports/monthly |
| Feishu chat | `电力日报`, `市场日报`, `/dailyreport` | `电力月报`, `市场月报`, `/monthlyreport` |
| Spot Market UI | 📄 Daily Report button | 📊 Monthly Report button |

**Known issue:** PDF is only 2.6 KB → likely blank/minimal sections. Verify by opening `debug/电力市场日报_20260630.pdf`. If sections are empty, check Hermes CloudWatch logs for JSON parse warnings.

---

### Hermes — News Screener (`services/hermes/news_screener.py`) ✅ LIVE td:104

- Improved AI scoring prompt: explicit 0-10 guide, title-only instruction, official source bonus
- Digest filter changed from `is_new=True` to `published_at >= now-24h` (backfill pre-ingests made `is_new` always False)
- AI executive summary added for 🔥 tier articles (relevance ≥ 8)

---

### Hermes — Mengxi Ranking Date Fix (`services/hermes/mengxi_ranking_report.py`) ✅ LIVE td:104

- `_latest_data_date()` now uses `GREATEST(MAX from md_id_cleared_energy, MAX from md_da_cleared_energy)`
- Root cause: Mengxi intraday market (`md_id_cleared_energy`) has no data after 2025-07-10 — likely suspended. Day-ahead (`md_da_cleared_energy`) should have current data.
- **TODO**: Verify by running: `SELECT MAX(data_date) FROM marketdata.md_da_cleared_energy;`
- If DA has recent data but ID doesn't, ranking report will now show the correct date.
- But the ranking VOLUMES still query `md_id_cleared_energy` — if ID volumes are also stale, the rankings themselves will be stale. May need to switch volume query to DA data.

---

### Spot Market App (`apps/spot-market/app.py`) ✅ LIVE v40/td:55

- News Sources tab: added 📄 Daily Report + 📊 Monthly Report buttons (POST to Hermes)
- Recent Ingested Articles panel (last 40 articles from `staging.spot_knowledge_docs`)
- Suggested Sources panel (8 pre-configured WeChat accounts)

---

## What to do next — Strategist

The user said "continue with strategist". The Strategist is the AI agent tab in the Spot Market app. Current state (v33, deployed):

### Strategist current state
- File: `apps/spot-market/app.py` — `_render_strategist_tab()` section
- Streaming: `_run_agent_turn` uses `client.messages.stream` (token-by-token into `st.empty()`)
- Tools available: `get_spot_prices`, `get_bess_pnl`, knowledge base search, `get_province_data`
- File upload: `📎 Upload file to knowledge base` expander above chat
- Model: Claude Sonnet 4.6

### What the user likely wants (based on context)
The user was about to work on Strategist improvements. Possible areas:
1. **More tools** — e.g. news search from `staging.spot_knowledge_docs`, market report query
2. **Better context** — inject recent high-score news articles into Strategist system prompt automatically
3. **Knowledge base improvements** — surfacing recent market reports in Strategist answers

**Suggested first step:** Ask the user "Which Strategist improvement do you want to work on?" and check `apps/spot-market/app.py` around `_render_strategist_tab()` to understand current state before proposing changes.

---

## Key file locations

| File | Purpose |
|------|---------|
| `services/hermes/market_report.py` | NEW — daily/monthly PDF report generator |
| `services/hermes/news_screener.py` | News screener + Feishu digest |
| `services/hermes/mengxi_ranking_report.py` | Mengxi BESS ranking report |
| `services/hermes/app.py` | Hermes FastAPI app — routing + schedulers |
| `apps/spot-market/app.py` | Spot Market Streamlit app (incl. Strategist tab) |
| `scripts/deploy_hermes.sh` | Hermes ECR build + ECS deploy |
| `scripts/update_spot_markets_taskdef.py` | Spot Market ECS deploy (`IMAGE_TAG=vNN py ...`) |

---

## Deployment state

| Service | Version | Task Def | Commit | Branch |
|---------|---------|----------|--------|--------|
| Hermes | td:104 | bess-platform-hermes:104 | edaf342 | cost-optimisation |
| Spot Market | v40 | rev:55 | — | cost-optimisation |

**Deploy Hermes:** `bash scripts/deploy_hermes.sh`
**Deploy Spot Market:** `IMAGE_TAG=vNN py scripts/update_spot_markets_taskdef.py`

---

## DB tables relevant to next work

| Table | Schema | Purpose |
|-------|--------|---------|
| `spot_knowledge_docs` | staging | News articles + relevance scores + ai_summary |
| `spot_knowledge_chunks` | staging | Full text chunks (NOT in docs table — no `content` column on docs) |
| `news_sources` | hermes | WeChat/web source registry |
| `md_id_cleared_energy` | marketdata | Mengxi intraday cleared energy (stale after 2025-07-10) |
| `md_da_cleared_energy` | marketdata | Mengxi day-ahead cleared energy (should be current) |

---

## Env vars (Hermes ECS task)

- `ANTHROPIC_API_KEY` — Claude API key
- `PGURL` / `HERMES_DB_URL` — Postgres connection string
- `FEISHU_OWNER_OPEN_ID` — Feishu user ID for report delivery
- `OUTLOOK_REFRESH_TOKEN` — expires ~every 90 days; renew with `py scripts/auth_microsoft_mail.py`

---

## Outstanding issues

1. **Mengxi ranking volume data stale** — rankings show 2025-07-10 data even after date fix. Root cause: `md_id_cleared_energy` has no rows with `data_date > 2025-07-10`. May need to switch ranking query to `md_da_cleared_energy`.
2. **PDF report quality** — verify td:104 generates sections correctly (assistant prefill fix deployed, not yet tested with live trigger). Test with `电力日报` in Feishu after td:104 is stable (~1 min).
3. **News screener relevance** — many articles scoring <6 due to CAPTCHA causing title-only scoring. Run `电力日报` after news screener has had a chance to run (daily at 14:00 Beijing).
