# Structurer Agent — Design Spec

**Date:** 2026-09-09 · **Status:** approved by user (all 4 sections) · **App:** `apps/deal_structurer` (Pillar 5)

## Goal

Turn the 💬 Strategist tab into a **Structurer agent** that lets the user upload
投委会 PDFs and other documents, ask questions, **challenge the results of tabs
1-6**, and get revised numbers and DAFs — with every revision versioned in the
DB-backed DAF library as an audit trail.

## Decisions (user-approved)

| Question | Decision |
|---|---|
| 修订语义 (revise semantics) | **改参数重算** — challenges patch parameters and re-run models; DAF numbers are always recomputed, never text-edited |
| Agent 触达方式 | **DB 结果存储驱动** — agent reads/writes `deal_briefs` / `deal_daf_results` / `deal_daf_library`; revisions are NEW versioned rows, never overwrites |
| Upload persistence | Conversation-scoped context (no KB ingestion in v1) |
| Full committee re-run from chat | NOT by default — agent must ask explicit confirmation first |

## Section 1 — Agent architecture & tools

- Rename tab **💬 Strategist → 💬 Structurer** (`apps/deal_structurer/app.py` nav list, `strategist.py` header).
- System prompt becomes a deal-structuring + committee-challenge persona with the
  grounding rule: numbers only from DB reads / tool re-runs, never from training memory.
- New tools alongside the existing 5 financial-model tools
  (`libs/deal_models/adapters/agent_tools.py`):

| Tool | Purpose |
|---|---|
| `list_deals` | Recent briefs + result ids/recommendations (`list_briefs`, `list_results`) |
| `get_deal_result(name_or_id)` | Brief + economics + sections + synthesis for a deal (`load_result`, `list_briefs`) |
| `update_deal_parameters(brief_id, field=value, …)` | Validated whitelist patch + `structure_notes` audit line + rev-N naming |
| `rerun_analysis(brief_id, scope)` | `economics` (local, default) / `section:<key>` / `synthesis` / `daf`; full committee only after explicit user confirmation |
| `read_uploaded_doc(filename, page?, query?)` | Paged / keyword-windowed read of an uploaded document |

- The tab also renders a **本次修订记录** panel listing result ids the agent created this session.

## Section 2 — Upload flow

- `st.file_uploader` atop the Structurer tab: multi-file; types as Tab 0
  (`PDF, PPTX, DOCX, XLSX, XLS, TXT, PNG, JPG, WEBP`).
- Extraction via existing `services/deal_committee/intake_parser.extract_text`
  (images via existing vision path).
- Per-file `{filename, text, ts}` kept in session (last 5); agent system context
  lists uploaded docs; `read_uploaded_doc` pages/queries them.
- Key flow: upload old DAF → challenge its assumptions → agent compares against
  live register/model → re-runs with updated parameters.

## Section 3 — Challenge → revise → regenerate loop

```
user: 容量补偿费率354太高,按280重算
agent: get_deal_result → update_deal_parameters(comp_rate=280)
       → rerun_analysis("economics")   (local, seconds)
       → rerun_analysis("synthesis")   (one LLM call, optional)
       → rerun_analysis("daf")         (rebuild PDF, new library row)
reply: 原口径 IRR 31.3% → 新口径 24.1%; 结论维持; 已存为 历史 DAF #42
```

**Scope grading for `rerun_analysis`:**
- `economics` — default for parameter challenges (local, fast)
- `section:<key>` — one committee section with patched brief (~1-4 min for agent sections)
- `synthesis` — re-derive from existing sections + new economics
- `daf` — rebuild PDF from current result row
- **Full committee: confirmation-gated** ("重跑全部 7 个章节,约5-10分钟,确认?")

**Guardrails:**
- `update_deal_parameters` whitelist only: comp_rate, cycles_per_day, efficiency,
  capex_total_yuan, debt_ratio, loan_rate, loan_term_years, tenor_years,
  province, node, capacity_mw, capacity_mwh, commissioning_year, deal_name.
  No free-text/structure fields in v1.
- Revision rows: `deal_name + " (rev N)"` + appended `structure_notes` line
  recording the challenge text.
- Agent must quote old → new numbers in its reply (no silent revisions).

## Section 4 — Testing & rollout

- Unit tests: parameter whitelist, rerun scope dispatch, read_uploaded_doc paging.
- AppTest: upload → Q&A; challenge → patch → re-run → new history row; DAF rebuild.
- Prod run-task probe: full loop on 谷山梁二期 with comp_rate=280 — verify revision row + DAF in library.
- Rollout: single image `bess-platform-deal-structurer:v26` (td from service's current tdArn, force-new-deployment), explicit deploy confirmation.

## File-level changes

| File | Change |
|---|---|
| `apps/deal_structurer/strategist.py` | Rework: rename, upload widget, new tools wiring, revision panel |
| `apps/deal_structurer/app.py` | Nav label 💬 Strategist → 💬 Structurer |
| `libs/deal_models/adapters/agent_tools.py` | Add tool schemas + dispatch for the 5 new tools |
| `services/deal_structurer/structurer_agent.py` (new) | Tool implementations (DB reads, patch, rerun, doc read) |
| `services/deal_committee/library.py` | `next_rev_name(engine, brief_id)` helper if needed |
| `tests/services/deal_structurer/test_structurer_agent.py` (new) | Unit tests |

## Non-goals (v1)

- Text-editing the DAF/synthesis without recomputation
- KB persistence of uploads ("存入知识库" button — follow-up)
- Full-committee re-run without confirmation
- Free-text parameter patches (whitelist only)
- hermes chat changes (the loop lives in the app tab only)
