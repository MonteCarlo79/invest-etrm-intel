# Hermes Knowledge Read/Write Hooks — Design Spec

**Date:** 2026-08-06
**Status:** Approved (user review pending)
**Branch:** feat/deal-structurer-bedrock-migration
**Related:** CLAUDE.md Stage 4 (Markdown Second Brain); `knowledge/` Obsidian vault; `project_lingfeng_pipeline` memory

---

## 1. Purpose

The `knowledge/` vault (913 markdown files: 860 spot daily reports, 27 province notes, 7 concept notes) is currently **write-only** — no agent reads it, and Hermes's scheduled outputs (morning briefing, daily market report) vanish after Feishu delivery. This change connects Hermes to the vault in both directions, implementing the first slice of the CLAUDE.md Stage 4 vision:

- **Read:** Hermes and its headless sub-agents inject relevant vault notes into their prompts at query time.
- **Write:** Hermes persists scheduled outputs as briefing notes and mirrors auto-extracted insights as inbox notes for human review.

## 2. Decisions (user-approved 2026-08-06)

| # | Decision | Choice |
|---|----------|--------|
| 1 | Scope | All four: read (Hermes main + headless sub-agents), write (scheduled outputs + insight notes) |
| 2 | Read mechanism | OneDrive-direct via existing `services/hermes/onedrive_client.py` (no DB sync table, no image baking) |
| 3 | Write landing zones | Briefings → `knowledge/hermes/briefings/` direct; insights → `knowledge/hermes/inbox/` for review/promotion in Obsidian |
| 4 | Retrieval integration | Context injection (matches `_retrieve_kb_context` / `retrieve_for_agent` patterns), not a new tool/action |
| 5 | Read scope | `knowledge/spot_market/` + `knowledge/hermes/briefings/`. `inbox/` excluded (unreviewed). `knowledge/policy/` PDFs stay on the existing KB-ingest path |
| 6 | Sub-agent coverage | `bess_map` + `mengxi_trading` headless agents only. `intl_market_common` / `gb_knowledge` excluded (vault content is China-spot; would add noise to GB/AU/PH/PO queries) |

## 3. Architecture

### 3.1 New module: `services/knowledge_pool/vault_reader.py` (~200 lines)

OneDrive-backed read access to the vault. All functions return empty/`""` on any failure — knowledge I/O must never break a chat loop.

- `VaultReader` — wraps a lazily-created shared `OneDriveClient`, rooted at `knowledge/`
- `list_notes(area)` — folder listing with 15-min in-memory TTL cache
- `search_notes(query=None, province=None, date=None, limit=3)` — matching order:
  1. Date mention (normalized `YYYY-MM-DD`) → `01_daily_reports/<date>.md` and/or `hermes/briefings/<date>-*.md`
  2. CN province name → `02_provinces/<province>.md`
  3. Theme keyword → `03_concepts/*.md` filename match
  4. Free-text fallback → `OneDriveClient.search(query)`, filtered to in-scope areas
- `read_note(path, max_chars=2000)` — `read_file_by_path`, UTF-8, truncate with a `[…truncated]` marker
- `retrieve_vault_context(query, max_notes=3) -> str` — single entry point for all consumers; returns a formatted `## Vault knowledge (from notes)` block, or `""` on failure

### 3.2 New module: `services/knowledge_pool/vault_writer.py` (~150 lines)

- `write_briefing_note(kind, content, note_date=None)` — kind ∈ `{morning, daily_report}` → uploads `knowledge/hermes/briefings/YYYY-MM-DD-<kind>.md` with YAML frontmatter (`note_type: briefing`, `kind`, `date`, `source: hermes`, `created` timestamp)
- `write_insight_note(category, subject, content, source_app)` — → `knowledge/hermes/inbox/YYYY-MM-DD-<slug>.md`; frontmatter adds `note_type: insight`, `category`, `source_app`, `review_status: pending`
- Same-path uploads overwrite (idempotent briefing re-runs)
- Failures log a warning and return `None`; never raise into callers

### 3.3 Integration edits (surgical)

| File | Change |
|------|--------|
| `services/hermes/agent.py` | In `HermesAgent.process()`, after the existing `_retrieve_kb_context` call: also call `vault_reader.retrieve_vault_context(msg)` and append the block; wrapped in try/except |
| `services/bess_map/headless_agent.py` | After the `retrieve_for_agent(...)` append (lines ~434-441): append `retrieve_vault_context(query)`; try/except guarded |
| `services/mengxi_trading/headless_agent.py` | Same append next to its expert-memory injection (lines ~262-271); try/except guarded |
| `services/hermes/scheduler.py` | In `send_morning_briefing()`: after successful Feishu send, `write_briefing_note("morning", briefing_text)` |
| `services/hermes/market_report.py` | In `send_daily_report()`: after Feishu send, `write_briefing_note("daily_report", text)` where `text` is the report's markdown body assembled from the same data sections that feed the PDF rendering (PDF delivery unchanged) |
| `services/knowledge_pool/expert_memory.py` | Inside `extract_spot_insights()`, after each DB insert: `write_insight_note(...)` — single hook covering all 3 existing call sites (bridge, bess_map, mengxi) |

### 3.4 Shared client

`vault_reader` / `vault_writer` each hold a module-level lazy `OneDriveClient` singleton (same construction as `thinking_agent.py`). No changes to `onedrive_client.py`.

## 4. Data flow

**Read (Hermes main):** user message → `_retrieve_kb_context` (DB, unchanged) + `retrieve_vault_context` (OneDrive) → both appended to prompt → LLM chain (Azure GPT → DeepSeek → Claude, unchanged).

**Read (headless):** query → `retrieve_for_agent` (DB, unchanged) + `retrieve_vault_context` (OneDrive) → system-prompt appends → tool loop.

**Write (briefing):** scheduler/market_report builds text → Feishu send (unchanged) → `write_briefing_note` → OneDrive upload → OneDrive syncs to laptop → user reads/corrects in Obsidian.

**Write (insight):** chat turn completes → `extract_spot_insights` (Haiku → `staging.kp_expert_insights` insert, unchanged) → `write_insight_note` → `inbox/` → user promotes worthy notes in Obsidian into curated vault areas.

## 5. Error handling

- OneDrive unreachable / token expired → read path returns `""`, write path logs + skips; chat loop unaffected
- Token-refresh failure log includes remediation hint: `python scripts/auth_microsoft_mail.py`
- `list_notes` cache failure → fall back to `OneDriveClient.search()`
- Injection caps: max 3 notes × 2,000 chars per query
- OneDrive rate limiting (429) → treat as transient failure, skip silently (no retry storm inside a chat turn)

## 6. Testing

**Unit tests** (new `services/knowledge_pool/tests/test_vault_reader.py` + `test_vault_writer.py`, fake OneDrive client, no network):
- YAML frontmatter formatting (required keys, date handling)
- Filename slugging (CN province names, date prefixes, collision-free insight slugs)
- `search_notes` matching: date mention → daily note; province name → province note; theme → concept note; fallback ordering
- `retrieve_vault_context` cap enforcement (3 notes, 2,000 chars) and empty-on-failure behavior

**Live smoke (manual, one-shot):**
1. Read `knowledge/spot_market/04_indices/index.md` via `vault_reader`
2. Write a scratch note to `knowledge/hermes/inbox/_smoke_test.md`, read it back, verify in OneDrive web
3. One end-to-end Hermes query locally (`services/hermes/app.py`) confirming the vault block appears in the prompt

## 7. Deployment

- Requires Hermes image rebuild + ECS redeploy (`bess-platform-hermes`) — **explicit in-session confirmation at that time**
- If the Hermes base image is MCR-hosted (throttled from this network), reuse the ECR-delta build workaround documented in `project_mac_transfer` memory
- No DB migrations. No Terraform changes.

## 8. Non-goals

- No DB sync table for vault content (revisit if retrieval quality demands FTS — that is the Stage 3 RAG path)
- No changes to `agent.py`'s action enum / tool dispatch (deferred refactor per `feedback_hermes_tool_dispatch_refactor`)
- No ingestion of `knowledge/policy/` PDFs (existing KB-ingest path covers it)
- No Obsidian plugin work; the vault remains plain markdown
