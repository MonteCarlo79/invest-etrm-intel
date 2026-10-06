# Market Intel Intake — Design Spec

**Date:** 2026-10-06
**Status:** Approved design, pre-implementation
**Apps touched:** `apps/spot-market` (UI), `services/knowledge_pool` (backend)
**Apps NOT touched:** hermes / Feishu path, bess-map, portal

---

## 1. Problem

The user's market-intelligence capture flow (PPT/PDF/DOC/images/URLs collected during investigation) currently runs through Feishu → Hermes → OneDrive/KB. Three structural failure modes:

1. **Breaks** — each Feishu image is a separate message spawning its own thread + OneDrive upload; one failure kills that image with only an error text, no batch semantics.
2. **Unknown image count** — `_pending_folders` in `services/hermes/app.py` requires pre-declaring batch size (or sticky "unlimited" mode that misroutes the next unrelated file).
3. **Misclassifies** — Feishu image filenames are random keys (`{image_key}.jpg`), so filename-based classification has nothing to work on; and the KB taxonomy (`services/knowledge_pool/knowledge_docs.py::CATEGORIES`) has no category for 装机/容量 or 调频/辅助服务 content.

## 2. Decisions (from brainstorming, 2026-10-06)

| Decision | Choice |
|---|---|
| Intake point | In-app upload UI (Feishu path kept as-is for quick mobile capture) |
| UI location | `apps/spot-market` Knowledge tab, new "Intel Intake" section |
| Batch semantics | One upload batch = **one document** (N images = N pages) |
| Classification | Doc-level, from content (vision), user-editable before commit |
| Route menu | Open-typed: extractor proposes whichever route types it finds (see §5) |
| 系统运行费 | Note-only for v1 (no draft queue — see §5 constraint) |
| Pipeline statistics | **Structured table** `marketdata.province_storage_pipeline` (recurring series; approved 2026-10-06) + Strategist read tool `get_storage_pipeline` |
| Review | Nothing auto-commits; review panel with per-route checkboxes |

## 3. Architecture

Three units, single-purpose each:

### 3.1 Extractor — `services/knowledge_pool/intake_extract.py` (new)

`extract_batch(pages: list[Page], api_key) -> IntakeProposal`

- `Page`: `{filename, bytes, media_type, kind}` where kind ∈ `image | text_doc`.
- Image pages: **one** Claude (sonnet-4-6) vision call containing all images, so the model sees the deck as a whole. Batches > 20 images split into page-groups for extraction but remain one document.
- Text-doc pages (pdf/pptx/docx/txt): reuse existing extractors in `knowledge_docs.py`; vision not applied to non-image pages.
- Prompt asks for strict JSON:
  ```
  {title, province, category, summary,
   pages: [{page_no, text}],
   routes: [{type, province, content, structured?}]}
  ```
- `province` ∈ 29 LingFeng market names + `national` + `null`.
- Route `type` ∈ menu in §5; `structured` carries typed fields for rate routes (e.g. `{metric, value, unit, month}`).

### 3.2 KB backend — `services/knowledge_pool/knowledge_docs.py` (extend)

- **Taxonomy additions** (keywords + EN/ZH labels):
  - `market_intel` 市场情报 — third-party analysis decks, market commentary
  - `capacity_pipeline` 装机与项目储备 — installed capacity, project registries, grid-connection pipeline, planning targets
  - `ancillary_market` 辅助服务 — 调频/调峰 demand, rules, compensation mechanisms
- **Schema change (approved):** `ALTER TABLE staging.spot_knowledge_docs ADD COLUMN IF NOT EXISTS province TEXT` — additive, idempotent, same migration pattern as the existing `app` column. No data rewrite.
- **`ingest_document_batch()`**: one doc row (`file_hash` = sha256 of sorted per-file sha256 concatenation → order-independent dedup) + one chunk per page (`page_no` set, `chunk_index` = page order). Returns `(doc_id, is_new, category)`.
- Dedup: existing `file_hash UNIQUE` constraint; UI warns on duplicate batch and requires explicit "ingest anyway".

### 3.2b Pipeline statistics table — `marketdata.province_storage_pipeline` (new)

Recurring per-province storage pipeline series, populated by the `pipeline_stat` route. Metric-keyed long format (same idiom as `province_ancillary_revenue`) so new metric types never need an ALTER:

```sql
CREATE TABLE IF NOT EXISTS marketdata.province_storage_pipeline (
    id            SERIAL PRIMARY KEY,
    province      TEXT   NOT NULL,          -- Chinese market name (宁夏), same convention as province_cap_comp
    as_of_date    DATE   NOT NULL,          -- date the statistic refers to; targets use horizon (e.g. 2030-12-31)
    metric        TEXT   NOT NULL,          -- vocabulary below
    value         NUMERIC,
    unit          TEXT,                     -- GW | GWh | 个 | 倍
    source        TEXT,                     -- e.g. 国网宁电[2026]70号, deck title
    source_doc_id INT REFERENCES staging.spot_knowledge_docs(id),  -- traceability to KB doc
    status        TEXT   NOT NULL DEFAULT 'confirmed',  -- intake is human-reviewed → confirmed; future screeners write 'draft'
    notes         TEXT,
    ingested_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_psp_nat
    ON marketdata.province_storage_pipeline (province, as_of_date, metric, COALESCE(source, ''));
CREATE INDEX IF NOT EXISTS idx_psp_prov_date
    ON marketdata.province_storage_pipeline (province, as_of_date DESC);
```

Initial metric vocabulary: `installed_new_storage_gw` (全口径新型储能并网), `installed_grid_side_storage_gw` (电网侧独立储能), `registry_projects` / `registry_gw` / `registry_gwh` (在库), `filed_not_registry_gw` / `filed_not_registry_gwh` (备案未入库), `total_filed_gw` / `total_filed_gwh` (全部备案合计), `planning_gap_gw` (规划缺口), `grid_access_gap_gw` (网架接入缺口), `grid_remaining_access_gw` (网架剩余可接入), `target_gw` (规划目标; 口径 differences — 自治区 vs 能源局 — become separate rows distinguished by `source`/`notes`). Extendable by string, no DDL.

**Consumption:** one new read-only Strategist tool `get_storage_pipeline(province=None, metric=None)` in `apps/spot-market/app.py`, following the existing tool pattern — returns latest value per metric + history, so the agent can answer "宁夏储能备案多少 / 在库缺口多大" from the table. No bess-map changes; Quant consumes pipeline intelligence via `quant_note` text.

### 3.3 UI — `apps/spot-market/app.py`, Knowledge tab, "Intel Intake" section

- `st.file_uploader(accept_multiple_files=True)` — png/jpg/jpeg/webp + pdf/pptx/docx/txt. Any count.
- **Extract** button → `extract_batch` → proposal held in `st.session_state` (nothing written).
- Review form: editable title, province dropdown, category dropdown (ZH labels), summary textarea; per-route rows: checkbox + type label + editable content.
- **Commit** → `ingest_document_batch` → selected route writers → toast with doc id + per-route outcome.
- Failed route writes after successful doc commit: reported per-route, **Retry routes** button re-fires only failed ones (idempotent writers; no double-ingest — see §5).

## 4. Data flow

```
files → uploader → session_state pages
      → Extract → IntakeProposal (session_state)
      → user edits title/province/category/summary/routes
      → Commit → doc row + page chunks (staging.spot_knowledge_*)
              → route writers (selected only)
              → toast: doc_id + per-route success/failure
```

## 5. Fact routes

| Route type | Trigger content | Destination | Status semantics |
|---|---|---|---|
| `ancillary_revenue` | realized 调频收入 monthly amounts (万元) | `marketdata.province_ancillary_revenue` | `status='draft'` → existing bess-map 调频收入 review queue |
| `capacity_comp_rate` | 容量电价/容量补偿标准, 元/kW·年, 峰段小时 | `marketdata.province_cap_comp` | `status='draft'` (table already has status column) → bess-map review |
| `fr_market_params` | 调频容量价格 元/kW·h, 全省调频资金池 亿元/年 | `marketdata.province_fr_market` | `status='draft'` (table already has status column) → bess-map review |
| `pipeline_stat` | structured pipeline figures (装机/在库/备案/规划/缺口, each with value + unit + as_of_date) | `marketdata.province_storage_pipeline` (§3.2b) | `status='confirmed'` — intake panel IS the human review; `source_doc_id` links back to the KB doc |
| `spot_note` | interpretive fundamentals context (oversupply ratios, load shape, targets narrative) | `agent_memory` app=`spot_market`, category=`province_note` (new category for this app — CLAUDE.md "extend as needed"), source=`intake` | active immediately |
| `quant_note` | BESS-revenue-relevant intelligence (AGC/调峰 demand sizing, mechanism/eligibility rules, structural constraints, 系统运行费 figures) | `agent_memory` app=`bess_map`, category=`province_note`, source=`intake` | active immediately |
| anything else | — | KB chunk text only | — |

Note routes are **persona-typed** (which agent consumes it), not content-typed — the extractor decides who cares, avoiding content-classification misfits (e.g. AGC demand sizing is neither a pipeline stat nor a policy change, but Quant clearly cares). Structured pipeline *numbers* go to the `pipeline_stat` table; `spot_note` carries only the *interpretation* (e.g. "备案31.6GW vs 20GW目标 → 1.45x oversupply, spread-compression risk"), so numbers and narrative have exactly one home each.

**Sysopfee constraint (why note-only):** `province_sysopfee_monthly` has no status/draft column — bare `(province, year_month, fee_yuan_kwh)` with upsert-overwrite semantics. Direct writes would silently overwrite confirmed values with unreviewed intel. The automated monthly screener (1st of month) already fills it systematically. Sysopfee figures therefore ride the `quant_note` route as text. A full draft queue (status column + bess-map review wiring + 系统运行费 tab filter) is deferred.

**Writer idempotency:** agent_memory writers check for an existing active row with same (app, category, subject) and update content in place (mirrors the register_url lesson — never soft-delete + re-ingest duplicates). Rate-draft writers check for existing non-superseded row on same natural key — (province, month, metric) for `province_ancillary_revenue`; (province, effective_date, source) for `province_cap_comp` / `province_fr_market` — and skip if present (mirrors hermes td:185 dedup pattern).

## 6. Error handling

- Single page extraction fails → page marked with error text, other pages proceed; commit allowed with partial pages (user's choice at review).
- Vision/API failure → no proposal, error shown, uploader retains files (bytes in session state).
- Doc commit OK, route write fails → doc stands; per-route failure surfaced; retry re-fires failed routes only.
- Batch hash already in KB → warning at review; commit needs explicit "ingest anyway" tick.

## 7. Testing

- **Unit:** new keyword categories classify 宁夏-deck-style text into `capacity_pipeline`/`ancillary_market`; batch hash order-independence; proposal-JSON parsing (mocked Claude response); partial-page failure path; route-writer idempotency (no dup rows on retry); `province_storage_pipeline` upsert respects (province, as_of_date, metric, source) natural key; `get_storage_pipeline` returns latest-per-metric correctly.
- **Local live fixture:** the 9-image 宁夏 deck (`IMG_3828`–`IMG_3836`): upload → verify proposal (expect category `capacity_pipeline` or `market_intel`, province 宁夏, routes: `pipeline_stat` rows — registry_gw=19.93, filed_not_registry_gw=11.7, total_filed_gw=31.63, planning_gap_gw=11.46, grid_access_gap_gw=6.36, installed_new_storage_gw series 7.63/8.0/10.13, target_gw 30/20 with two 口径 — + `spot_note` (oversupply interpretation) + `quant_note` (AGC-demand sizing and 4h-constraint intelligence), **no** rate drafts — deck has demand figures, no compensation rates) → edit one field deliberately → commit → verify doc+chunks, pipeline rows with `source_doc_id` backlink, both agent_memory rows, zero ancillary rows → ask Strategist "宁夏独立储能在库规模多少" and confirm it answers from `get_storage_pipeline`.
- Existing suites must stay green. Hermes/Feishu path untouched.

## 8. Cost

One sonnet vision call per batch (~9 images ≈ 15–20k tokens). Negligible vs per-image calls; cheaper than the current broken flow's repeated failures.

## 9. Out of scope (v1)

- Hermes/Feishu image path fixes (kept as-is for mobile capture).
- Sysopfee structured draft queue (needs status column + bess-map wiring).
- bess-map and portal code — untouched; they consume via existing queues/memory.
- Pipeline-stats chart/dashboard UI (table + Strategist tool only; visualisation when a second consumer appears).
- URL intake (existing `register_url` path already covers URLs).
- KB browser province facet (province column is written but not yet surfaced in search UI).
