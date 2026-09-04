# Deal Committee — Investment Decisions Engine Design Spec
*Created: 2026-09-04*

---

## Context

Deal Structurer today is the quant foundation (price sim → dispatch → cashflow → MC → deal pricing, tabs 1–5 + Strategist). This spec adds the **investment committee layer** on top: the user uploads a deal background document (doc/ppt/pdf/excel) or fills a short form, and the app orchestrates the platform's existing hermes headless-agent machinery to produce a professional **Deal Advice Form (DAF)** PDF — go/no-go recommendation with market background, economics, risk analysis, and risk mitigation, in Chinese, with data tables and charts.

Reference output format: `reports/DAF/*.docx` (Shell Energy Europe Deal Approval Forms — deal terms header, deal summary, portfolio fit, economics table, numbered risks with mitigants + likelihood, approval matrix).

**Decisions locked in brainstorming (2026-09-04):**
- Workflow: **staged with checkpoints** (intake → confirm brief → analysis with progress → review → PDF)
- Report language: **Chinese** (English technical terms where natural: IRR, VaR, capture rate)
- Architecture: **Approach A — in-process committee**. Deal-structurer imports the existing headless agents + knowledge pool directly. No new HTTP endpoints, no Cognito bypass, no async job plumbing.

---

## Architecture

```
apps/deal_structurer/
├── intake_tab.py        ← NEW Tab 0 · Deal Intake
├── committee_tab.py     ← NEW Tab 6 · Investment Committee
└── app.py               ← register the two tabs

services/deal_committee/          ← NEW: pure orchestration, no Streamlit imports
├── __init__.py
├── brief.py             # DealBrief pydantic schema + LLM extraction
├── intake_parser.py     # docx/pptx/pdf/xlsx/txt → plain text
├── orchestrator.py      # section pipeline → list[SectionResult]
├── sections.py          # per-section question builders
├── synthesis.py         # committee synthesis call → go/no-go + mitigations
├── charts.py            # matplotlib PNG charts for the PDF
└── daf_builder.py       # reportlab DAF PDF (CJK)

Reused as-is (no changes):
├── services/hermes/market_agent_bridge.py   # run_market_query dispatcher
├── services/{bess_map,mengxi_trading,asset_risk,retail_risk}/headless_agent.py
├── services/spot_mcp/tools.py               # spot data (bridge's spot branch)
├── services/knowledge_pool/knowledge_docs.py # KB FTS + file loaders
├── services/hermes/export_utils.py          # reportlab CJK font pattern
└── libs/deal_models/                        # economics engine (unchanged)
```

**Dependency rule:** `services/deal_committee` contains no Streamlit imports (same rule as `libs/deal_models` having no I/O). The two tabs are thin adapters. All LLM calls go through `shared/anthropic_client.make_client` (Bedrock on ECS, API key locally).

**No changes to hermes service.** "Mobilising hermes" = importing `services/hermes/market_agent_bridge.py`, the same dispatcher hermes itself uses. The bridge lazy-imports each headless agent, so importing it does not pull in hermes's chat-client dependencies.

---

## Data Flow

1. **Intake** (`intake_tab.py`)
   - User uploads one or more docs (docx/pptx/pdf/xlsx/txt) and/or fills a manual form.
   - `intake_parser` extracts plain text (reuse `services/knowledge_pool` file loaders).
   - Claude (sonnet) extracts a `DealBrief` JSON with per-field confidence flags.
   - User reviews/edits in a form — low-confidence fields highlighted. **Confirmation is a hard gate**: the committee cannot run on an unconfirmed brief.
   - Confirmed brief persisted to `marketdata.deal_briefs` (new table).

2. **Committee run** (`orchestrator.py`, driven from `committee_tab.py`)
   - Sequential sections (v1), each via `run_market_query(agent, question, ...)` except economics:
     1. `market_background` → **spot** headless agent (via bridge): price levels, spreads, volatility, inter-provincial flows, market stage
     2. `policy` → **KB** (`knowledge_pool` FTS + spot agent): provincial spot rules, BESS participation, capacity compensation, policy risks
     3. `economics` → **in-process** `libs/deal_models` engine (not an agent): price history → sim → dispatch → cashflow → MC → P10/P50/P90 revenue, equity IRR, capture rate. Runs from brief params, independent of tabs 1–5 session state. If `brief.node` is set, pull nodal data (`reports.nodal_pf_node_daily`, `knowledge/mengxi/bess_node_registry.md`).
     4. `ops_evidence` → **mengxi + asset_risk + retail_risk** headless agents: realized capture rates, cycles, availability, book benchmarks
     5. `risk` → synthesis over rm_* aggregates (direct RDS) + prior section outputs
   - Each section: `SectionResult{key, title, markdown, tables, charts, status, error}`. Per-section timeout 180s. A failed section does not stop the pipeline.
   - Checkpoint UI: sections appear as expanders as they complete; per-section re-run button.

3. **Synthesis** (`synthesis.py`)
   - One sonnet call: all section outputs + DAF skeleton → 结论 (**GO / 有条件 GO / NO-GO**) + 风险缓释建议 + 核心假设. Grounded: every number must trace to a section output; forbidden from inventing data.

4. **DAF PDF** (`daf_builder.py` + `charts.py`)
   - reportlab platypus; CJK font registration follows `services/hermes/export_utils.py` pattern (TTFont with UnicodeCIDFont fallback).
   - Layout per §DAF Layout below.
   - PDF bytes + brief JSON persisted to `marketdata.deal_daf_library` (new table, mirrors `intl_market.report_library` shape). Committee tab lists past DAFs with download.

---

## DealBrief Schema

| Group | Fields |
|---|---|
| Identity | `deal_name`, `source_files: list[str]`, `asset_type` ∈ {bess, wind, solar, wind_bess, solar_bess} |
| Site | `province`, `node: str | None` |
| Technical | `capacity_mw`, `capacity_mwh` (BESS), `efficiency`, `cycles_per_day`, `installed_mw` (wind/solar) |
| Commercial | `capex_total` or `capex_per_kw`, `commissioning_date`, `tenor_years`, `counterparty`, `structure_notes` |
| Financing | `debt_ratio`, `loan_rate`, `loan_term` (pre-filled from cashflow tab defaults) |
| Meta | `field_confidence: dict[str, float]`, `confirmed: bool`, `created_at` |

Pydantic model in `brief.py`; DB table `marketdata.deal_briefs` stores the JSONB + confirmation state.

---

## DAF Layout (A4, Chinese)

1. **交易概要表** — 项目名称 / 资产类型 / 省份 / 节点 / 容量 / 投资额 / 期限 / 对手方 / 关键日期
2. **交易摘要** — narrative summary
3. **市场背景** — market stage + policy summary; 12-month price level / spread / volatility charts
4. **经济性分析** — KPI table (equity IRR P10/P50/P90, capture rate, DSCR, payback) + revenue distribution + IRR histogram
5. **运营实证** — ops benchmark table (realized capture, cycles, availability; asset-risk / retail-risk evidence where relevant)
6. **风险分析** — numbered risks, each with 可能性 / 影响 / 缓释措施
7. **投资建议** — GO / 有条件 GO / NO-GO + conditions + mitigations + key assumptions
8. **附录** — data sources, model versions, generation timestamp

---

## Error Handling

- Section agent failure/timeout (180s) → section marked 失败 with error, pipeline continues; DAF notes 数据缺失; per-section re-run in UI.
- No data for province/node → section reports it explicitly; synthesis must state the gap (grounding rule, no invented numbers).
- LLM unavailable → `is_llm_available` guard at tab load with a clear message.
- PDF generation failure → section outputs retained in session; retry PDF without re-running analysis.
- Unconfirmed or low-confidence brief → committee run blocked until user confirms.

---

## Docker / Deploy

- `apps/deal_structurer/Dockerfile`: add `COPY` for `services/hermes/` (bridge + export_utils only — prune to needed files), `services/spot_mcp/`, `services/bess_map/`, `services/mengxi_trading/`, `services/asset_risk/`, `services/retail_risk/`, `services/knowledge_pool/`, `services/deal_committee/`; add any missing pip deps (reportlab, matplotlib, python-docx, python-pptx, openpyxl, pdf parsers as required by the knowledge_pool loaders).
- New tables (`marketdata.deal_briefs`, `marketdata.deal_daf_library`): `CREATE TABLE IF NOT EXISTS` at first use (idempotent migration pattern used across the platform), DDL committed under `db/ddl/marketdata/`.
- ECS redeploy: new image tag + task-def revision per `docs/BEDROCK_MIGRATION_GUIDE.md` deployment workflow. Requires explicit confirmation before deploy.

---

## Build Order

1. `services/deal_committee/brief.py` + `intake_parser.py` — schema + text extraction + LLM extraction
2. `services/deal_committee/sections.py` + `orchestrator.py` — pipeline with stubbed sections
3. Wire real sections: spot/KB, economics engine, mengxi/asset_risk/retail_risk, risk
4. `services/deal_committee/synthesis.py`
5. `services/deal_committee/charts.py` + `daf_builder.py` + library persistence
6. `apps/deal_structurer/intake_tab.py` + `committee_tab.py` + app.py routing
7. Dockerfile + requirements + local run verification
8. Deploy (explicit confirmation required)

---

## Verification

1. **Unit tests** (`services/deal_committee/tests/`): brief extraction from fixture docs, section question builders, DAF builder renders valid PDF containing Chinese text, chart functions return PNG bytes.
2. **Integration**: orchestrator with stubbed `run_market_query` (no LLM cost) assembles all SectionResults; economics section with real RDS 蒙西 data returns plausible P10/P50/P90.
3. **Smoke**: both new tabs render locally; end-to-end run on a sample brief produces a downloadable PDF; library save/load round-trip.
4. **Grounding spot-check**: read the generated DAF; every number traces to a section output or brief field.
5. **Format check**: generated DAF section headers match the 8-section layout above; risk entries each carry 可能性/影响/缓释.

---

## Out of Scope (this phase)

- Parallel section execution (sequential v1; revisit if run time hurts)
- Multi-deal comparison / province screening mode (bess-map already ranks provinces)
- Interactive agent-led DAF drafting (the Strategist tab stays as-is)
- Approval workflow / signature matrix (the DAF records the recommendation; human approval stays offline)
- hermes HTTP API (deliberately rejected — Approach B)
