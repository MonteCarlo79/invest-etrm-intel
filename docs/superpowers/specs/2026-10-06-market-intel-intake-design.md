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
| `spot_note` | pipeline/generation/load/targets/fundamentals (装机, 在库, 备案, 规划, 发电量, 负荷, 峰谷差, 缺口) | `agent_memory` app=`spot_market`, category=`province_note` (new category for this app — CLAUDE.md "extend as needed"), source=`intake` | active immediately |
| `quant_note` | BESS-revenue-relevant intelligence (AGC/调峰 demand sizing, mechanism/eligibility rules, structural constraints, 系统运行费 figures) | `agent_memory` app=`bess_map`, category=`province_note`, source=`intake` | active immediately |
| anything else | — | KB chunk text only | — |

Note routes are **persona-typed** (which agent consumes it), not content-typed — the extractor decides who cares, avoiding content-classification misfits (e.g. AGC demand sizing is neither a pipeline stat nor a policy change, but Quant clearly cares).

**Sysopfee constraint (why note-only):** `province_sysopfee_monthly` has no status/draft column — bare `(province, year_month, fee_yuan_kwh)` with upsert-overwrite semantics. Direct writes would silently overwrite confirmed values with unreviewed intel. The automated monthly screener (1st of month) already fills it systematically. Sysopfee figures therefore ride the `quant_note` route as text. A full draft queue (status column + bess-map review wiring + 系统运行费 tab filter) is deferred.

**Writer idempotency:** agent_memory writers check for an existing active row with same (app, category, subject) and update content in place (mirrors the register_url lesson — never soft-delete + re-ingest duplicates). Rate-draft writers check for existing non-superseded row on same natural key — (province, month, metric) for `province_ancillary_revenue`; (province, effective_date, source) for `province_cap_comp` / `province_fr_market` — and skip if present (mirrors hermes td:185 dedup pattern).

## 6. Error handling

- Single page extraction fails → page marked with error text, other pages proceed; commit allowed with partial pages (user's choice at review).
- Vision/API failure → no proposal, error shown, uploader retains files (bytes in session state).
- Doc commit OK, route write fails → doc stands; per-route failure surfaced; retry re-fires failed routes only.
- Batch hash already in KB → warning at review; commit needs explicit "ingest anyway" tick.

## 7. Testing

- **Unit:** new keyword categories classify 宁夏-deck-style text into `capacity_pipeline`/`ancillary_market`; batch hash order-independence; proposal-JSON parsing (mocked Claude response); partial-page failure path; route-writer idempotency (no dup rows on retry).
- **Local live fixture:** the 9-image 宁夏 deck (`IMG_3828`–`IMG_3836`): upload → verify proposal (expect category `capacity_pipeline` or `market_intel`, province 宁夏, routes: `spot_note` carrying pipeline/load/registry figures + `quant_note` carrying AGC-demand sizing and 4h-constraint intelligence, **no** rate drafts — deck has demand figures, no compensation rates) → edit one field deliberately → commit → verify doc+chunks, both agent_memory rows, zero ancillary rows.
- Existing suites must stay green. Hermes/Feishu path untouched.

## 8. Cost

One sonnet vision call per batch (~9 images ≈ 15–20k tokens). Negligible vs per-image calls; cheaper than the current broken flow's repeated failures.

## 9. Out of scope (v1)

- Hermes/Feishu image path fixes (kept as-is for mobile capture).
- Sysopfee structured draft queue (needs status column + bess-map wiring).
- bess-map and portal code — untouched; they consume via existing queues/memory.
- URL intake (existing `register_url` path already covers URLs).
- KB browser province facet (province column is written but not yet surfaced in search UI).
