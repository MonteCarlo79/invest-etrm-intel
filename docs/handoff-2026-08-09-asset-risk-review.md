# Asset Risk Management — Status Review & Gap Analysis

**Date:** 2026-08-09
**Branch:** `feat/deal-structurer-bedrock-migration`
**Reviewed against:** `docs/superpowers/specs/2026-07-16-asset-risk-design.md` (Approved) + `docs/superpowers/plans/2026-07-16-asset-risk-management.md`
**Trigger:** A stale 2026-07-20 handoff prompt ("write the plan, then execute greenfield") was pasted into a new session — the plan and implementation already existed. This review assessed actual state.

---

## Health check results

| Check | Result |
|---|---|
| Test suites (`tests/risk`, `tests/settlement`, `tests/operating_assets`) | ✅ 40 passed (0.73s) |
| Local boot (`streamlit run apps/asset_risk/app.py --server.port=8512`) | ✅ health 200, page 200 |
| Module imports (app + 6 tabs) | ✅ all clean |
| Deployment | ✅ live at **v28** (user-confirmed; local untracked tfvars says v2 — stale) |
| Spec coverage | ⚠️ functional core done; Tabs 3–6 still at plan-skeleton depth |

**Key context:** App 1 is NOT greenfield. Fully built (all 6 tabs, libs/risk, libs/settlement, services/forward_curve, services/operating_assets, all rm_ DDL), deployed, and iterated through 2026-08-06 (12+ post-plan commits). App 2 (`apps/retail_risk/` + `services/retail_risk/` + `db/ddl/marketdata/rm_retail.sql` + headless agent) is also built.

---

## Beyond-spec additions (good drift to keep)

- **Tab 2 (Settlement)** far richer than spec: multi-file upload, auto month-detection from filename + PDF content (`services/settlement_ingest/scanner.py: extract_month_from_filename`, `parser_charge.py: extract_billing_period`), overwrite semantics, scanned-PDF Claude Vision path (`parser_discharge.py`), invoice-folder Scan & Ingest with dry-run, Chinese monthly breakdown （价差收入 / 容量补偿价差 / 套利价差 / 日均充放次数 / 转化率， YTD row, color-coded headers), reconciliation view
- **`services/settlement_ingest/`** — post-spec production ingestion path (scanner, folder_mapper, parser_charge, parser_discharge, watcher). The spec's WeCom receiver (Path A) was never built — folder scan won. Treat WeCom path as dead unless re-requested.
- **Tab 1**: invoice-folder mapping UI, inline capacity/duration edit form

---

## Gaps vs approved spec, ranked by impact

### 1. Agent tab diverges from platform pattern (CLAUDE.md agent requirements)
- Only 4 of 6 spec'd tools — missing `get_settlement_summary(book_id, month)` and `get_deviation_analysis(book_id, start, end)`
- No domain-grounding rule in system prompt, no `agent_memory` read/write (app key should be `asset_risk`), no Haiku auto-extract — all three mandatory pattern elements absent
- Single-turn only: `session_state.agent_messages` keeps display history but it is never sent to the model
- `services/asset_risk/headless_agent.py` exists — check whether it covers any of this before rebuilding; `tab_agent.py` does not use it

### 2. VaR snapshots never persisted
- Nothing writes `rm_var_snapshots` → agent's `get_var` tool always returns "No VaR data" (cross-component break)
- Missing: backtesting chart (actual daily P&L vs VaR band with exceedance markers)
- Note: VaR uses book's own `rm_position_volumes.market_price_cny_mwh` history (spec said `spot_prices_hourly` 252-day) — works but thinner history

### 3. Tab 3 (Realised P&L) is thin
- 🐛 **Dead control:** "Date Range" input rendered (`tab_pnl.py:30`) but never applied to any query
- Missing: volume deviation table (nominated→cleared→settled + deviation split MWh/¥)
- Missing: comparison views (asset-vs-asset same province, MoM, YoY)
- Missing: spec's structured waterfall (TOU-period split, curtailment red bar, reconciliation-gap bar) — current waterfall is generic category totals
- Missing: BESS dispatch-window Gantt; wind forecast-vs-actual scatter; curtailment hour-of-day heatmap
- Present: BESS ops KPIs from `rm_dispatch_daily`; wind curtailment KPIs + 10% threshold line ✓

### 4. Tab 4 (Positions & MtM) missing write paths
- No hourly-position upload UI (spec: exchange files → `libs/settlement/parser.py` → `rm_position_volumes`)
- `actual_price_cny_mwh` / `pnl_cny` computed-on-write logic (spec formula: Σ(channel_price × channel_volume)/Σ volume) exists nowhere — only wind parser fills `pnl_cny` from source 收益 column
- Contract register read-only (spec: inline add/edit for bilateral contracts)
- LingFeng curve pull (`services/forward_curve/lingfeng_pull.py`) not wired into UI
- No book-level MtM 30-day time series

### 5. Tab 1 minor gaps
- No `fuel_type` field for thermal assets
- No retire/deactivate toggle (spec: edit/deactivate inline)
- No virtual/aggregated book creation (books without linked asset)

### 6. Dangling UX
- Tab 2 unknown-format error says "Please use manual column mapping" — that UI does not exist (`tab_settlement.py:248`)

---

## Hygiene issues

- **Schema drift:** `rm_assets.invoice_folder` used by app (`tab_asset_config.py`, `tab_settlement.py`) but absent from `db/ddl/marketdata/rm_assets_books.sql`. Likely other post-spec columns too — reconcile DDL with live schema before next fresh-environment deploy.
- **Streamlit deprecation:** `use_container_width` removal warning (post-2025-12-31) — will break on next streamlit upgrade. Repo-wide, not just this app.
- **Repo (OneDrive sync damage, from 2026-08-08 incident):**
  - Broken git ref `refs/heads/feat/deal-structurer-bedrock-migration-Chen's MacBook Air` (bad object) — breaks `git log --all` and `--all` variants
  - ~9 untracked conflict-duplicate files `apps/asset_risk/*-Chen's MacBook Air.*` (plus more in other apps)
  - `.git-impaired-20260808/` backup still on disk pending deletion
  - `infra/terraform/services-new-Chen's MacBook Air.tf` is a committed suffixed file that terraform READS (all `*.tf`) — asset_risk ECS resources live only there
- **Test gaps:** no tests for `services/settlement_ingest/*` (production ingestion path), `bess_daily`/`bess_dispatch` parsers, or ingest orchestrator

---

## Suggested next moves (agreed order)

1. Fix the dead date-range control in `tab_pnl.py` (small, real bug)
2. Bring Tab 6 agent to platform pattern (grounding rule + `agent_memory` + 2 missing tools + multi-turn) — CLAUDE.md compliance
3. VaR snapshot writer + backtesting chart (un-breaks `get_var`)
4. Reconcile DDL files with live schema (`invoice_folder` et al.)
5. Tab 4 write paths (position upload, contract add/edit, LingFeng pull wiring)

---

## Key file paths

| What | Path |
|---|---|
| App | `apps/asset_risk/` (app.py + tab_*.py, Dockerfile, requirements.txt) |
| Headless agent | `services/asset_risk/headless_agent.py` |
| Risk libs | `libs/risk/` (mtm, pnl, var, greeks) |
| Settlement libs | `libs/settlement/` (parser, categorizer) |
| Ingestion (production) | `services/settlement_ingest/` |
| Ingestion (ops files) | `services/operating_assets/` |
| Forward curves | `services/forward_curve/` |
| DDL | `db/ddl/marketdata/rm_*.sql` (7 files incl. rm_retail.sql) |
| Tests | `tests/risk/`, `tests/settlement/`, `tests/operating_assets/` |
| Spec | `docs/superpowers/specs/2026-07-16-asset-risk-design.md` |
| Plan | `docs/superpowers/plans/2026-07-16-asset-risk-management.md` |
| Reference agent pattern | `apps/spot-market/app.py` (`_SPOT_AGENT_BASE_SYSTEM`, memory injection) |
