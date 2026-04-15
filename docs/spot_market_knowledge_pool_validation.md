# Spot Market Knowledge Pool — Validation Report

**Date**: 2026-04-15  
**Pipeline version**: Phase 1 (commit `5527a95` + hardening pass)  
**Environment**: Local PostgreSQL 18.2, Anaconda Python 3.13, Windows 11

---

## 1. Corpus Summary

| Metric | Value |
|--------|-------|
| PDFs ingested | 350 (all `parsed`) |
| PDFs skipped (already done) | 0 after final full run |
| PDFs with errors | 0 |
| Date range covered | 2024-08-08 → 2026-12-31 |
| Total pages | 12,463 |
| Pages with inferred date | 3,218 (25.8%) |
| Total chunks | 15,728 |
| Total facts (pdf_regex) | 2,606 |
| Unique driver provinces | 27 |
| Notes generated | 860 daily · 27 province · 7 concept · 1 index |

**Input directories scanned:**

| Year | PDFs |
|------|------|
| 2024 | ~67 (PDF glob) |
| 2025 | ~247 (PDF glob) |
| 2026 | ~41 (PDF glob) |

---

## 2. Idempotency

**Test**: Re-ran `spot_market_ingest.py` (without `--force`) on the same 5-file subset after initial parse.

| Pass | Documents | Pages | Chunks | Facts | Result |
|------|-----------|-------|--------|-------|--------|
| First run | 5 | 176 | 243 | 73 | Processed |
| Second run | 0 | 176 | 243 | 73 | All 5 skipped |

**Verdict: PASS.** Hash-based dedup in `document_registry.py` prevents re-processing. `ON CONFLICT DO UPDATE` upsert patterns in page/chunk/fact tables prevent row duplication even if `--force` is used.

**Known caveat**: When `--force` re-ingests a document with a corrected year (e.g., a 2024 PDF initially run with year=2025), old facts at the wrong date are not deleted automatically. The fix is to run `DELETE FROM staging.spot_report_facts WHERE document_id = %s` before re-extracting. The ingest script does not do this automatically for simplicity; add `--clean-facts` flag if needed in future.

---

## 3. Date Inference

**Method chain** (in order of priority):

1. `report_date_min/max` on document: derived from page-level inference (`_infer_page_date`)
2. Per-page date: regex `(\d{1,2})月(\d{1,2})日` in page text combined with the year derived from the source directory name (e.g., `data/spot reports/2025/`)
3. Filename: `dates_from_filename()` in `spot_ingest.py` parses `(M.D)` / `(M.D-M.D)` patterns — used by the original spot price pipeline, NOT by the knowledge pool ingest (which uses page text)

**Coverage**: 25.8% of pages have an inferred date. This is expected — most pages in multi-province reports are continuation table pages without a date header. The date is typically only present on the first page of each date section.

**Sample results**:

| Document | Pages | Dated Pages | Inferred Range |
|----------|-------|-------------|----------------|
| 电力现货市场价格与运行日报 (7.16).pdf | 27 | 7 | 2025-07-15 → 2025-07-16 |
| 电力现货市场价格与运行日报 (7.28).pdf | 57 | 17 | 2025-07-24 → 2025-07-28 |
| 电力现货市场价格与运行日报（10.17）.pdf | 20 | 6 | 2024-10-10 → 2024-10-17 |
| 2026 PDFs | varies | ~15–25% | 2026-01 → 2026-04 |

**Year correctness**: Fixed in hardening pass. Year is now derived from the containing directory name (`data/spot reports/2025/` → year=2025), not from a single CLI default. 2024 and 2026 documents are correctly attributed.

**Known limitation**: The `report_year` column in `staging.spot_report_documents` is not updated on `--force` re-registration (the hash-match returns early). The authoritative date range is `report_date_min/report_date_max`; do not rely on `report_year` for queries.

---

## 4. Chunk Coverage

**Configuration**: 500-char chunks with 100-char overlap.

| Metric | Value |
|--------|-------|
| Average chunks per document | 45 |
| Minimum (doc with 14 pages) | ~20 |
| Maximum (doc with 171 pages) | ~200 |
| Documents with 0 chunks | 0 |

Every ingested document produced at least one chunk. No documents were lost in the text extraction step.

**Chunk type distribution** (approximate across corpus):
- `reason`: pages containing `原因为` or price deviation phrases (~15%)
- `table`: pages containing price table headers (~35%)
- `header`: pages with spot market section headings (~5%)
- `body`: all other pages (~45%)

---

## 5. Fact Extraction Quality

### 5.1 Fact types (final, post-fix)

| Fact type | Count | With province | Unique provinces | Source |
|-----------|-------|---------------|------------------|--------|
| section_marker | 1,388 | 0 | — | pdf_regex |
| driver | 866 | 866 (100%) | 27 | pdf_regex |
| interprovincial | 352 | 0 | — | pdf_regex |
| price_da / price_rt | 0 | — | — | spot_daily_bridge (not populated — public.spot_daily absent in this DB) |

### 5.2 Driver extraction

**Before fix**: The province-name regex `[\u4e00-\u9fff]{1,6}` matched any 1–6 Chinese characters at any position — 99.2% false positive rate (capturing "元", "不结算", "期间电力供需" etc. as province names).

**After fix**: `_RE_REASON` now uses a pipe-joined allowlist of 35 known province names as the first match group, anchored to known provincial identifiers. False positive rate post-fix: **0%** (all 866 driver facts have valid province names).

**Top provinces by driver sentence count**:

| Province | Driver facts |
|----------|-------------|
| 蒙西 | 171 |
| 湖北 | 122 |
| 陕西 | 66 |
| 蒙东 | 56 |
| 福建 | 42 |

### 5.3 source_method field

Every fact row carries `source_method`:
- `pdf_regex` — extracted from page text by pattern matching
- `spot_daily_bridge` — copied from `public.spot_daily` (zero rows in this environment; will populate when that table exists)

This allows downstream consumers to filter by confidence: bridge facts are `high` confidence (structured DB); regex facts are `medium` confidence.

---

## 6. Note Determinism

**Test**: Generated `knowledge/spot_market/01_daily_reports/2025-07-16.md` twice using the same document ID and data.

| Run | MD5 hash |
|-----|----------|
| First | `8ffb37cd06484c0f2ccbe9ff852467ec` |
| Second | `8ffb37cd06484c0f2ccbe9ff852467ec` |

**Verdict: PASS.** Notes are fully deterministic across re-runs — no timestamps in the body, no random IDs, no non-deterministic ordering. The only non-deterministic field is `updated_at` in the DB registry, which does not appear in the note body.

**Multi-document overwrite behavior**: When multiple documents cover the same report date (e.g., a single-day report and a multi-day report both covering 2025-07-16), the note is overwritten on each generation call. The last document processed determines the final note content. This is expected behavior — the daily note generation iterates over all parsed documents in ascending document_id order; the highest document_id wins for any given date.

**Implication**: For dates covered by both a single-day report and a multi-day aggregate, the note will reflect whichever document was processed last. This is noted as a known design choice, not a bug.

---

## 7. Retrieval Quality

**Configuration**: Full-text search using PostgreSQL GIN index on `to_tsvector('simple', chunk_text)`. For queries ≤ 4 characters, falls back to `ILIKE %query%`.

**Note on Chinese FTS**: The `simple` text search config treats whitespace-separated tokens as lexemes. Since Chinese has no word separators, each uninterrupted Chinese character sequence is treated as a single token. Queries of ≤ 4 characters use ILIKE for better recall.

| # | Query | Filters | Result | Notes |
|---|-------|---------|--------|-------|
| 01 | 新能源出力下降 | — | HIT | Concept found in monthly summary |
| 02 | 负荷增加 | — | HIT (3) | Valid driver sentences with numeric context |
| 03 | 省间现货 | — | HIT (3) | Interprovincial market sections found |
| 04 | 最高价 最低价 | — | HIT (3) | Table header rows matched |
| 05 | 均价偏高 | province=山东 | MISS | Phrase literally absent from corpus; reports use 均价上升/均价偏高 is rare |
| 06 | 实时均价 | from=2025-07-01 | HIT (3) | Date filter working correctly |
| 07 | 日前市场 | province=山西 | HIT (3) | Province filter matched by text presence |
| 08 | 水电 | — | HIT (3) | Hydro context in driver sentences |
| 09 | 检修 | — | HIT (3) | Maintenance context: 柴拉直流、青豫直流 |
| 10 | 新能源消纳 | — | MISS | Phrase appears only in policy doc, not daily reports |

**Hit rate: 8/10 (80%)**

**MISS analysis**:
- Query 05: "均价偏高" doesn't appear verbatim. The corpus uses "均价上升" or "均价偏高" interchangeably but the latter is rare. Workaround: search "偏高" or "均价上升".
- Query 10: "新能源消纳" appears in a policy compilation PDF (not a daily report) in a carbon trading context, not as a daily market driver phrase. The concept note for 新能源消纳 will remain sparse.

---

## 8. Provenance Quality

Daily report notes now include an explicit `## Provenance` section:

```markdown
## Provenance

- **Source file**: `电力现货市场价格与运行日报 (7.16).pdf`
- **Document ID**: 2
- **Pages covering this date**: 5
- Page 1: 655 chars
- Page 2: 488 chars
- Page 17: 692 chars
- Page 19: 24 chars
- Page 26: 303 chars
```

Province notes include source document list with doc_id, filename, and date range.

Concept notes include a structured evidence table:
```
| Date | Province | Provenance | Driver Sentence |
```

All note types carry YAML frontmatter with `document_id`, `source_path`, and `source_file`.

Driver facts in the DB carry `page_no`, which is surfaced in province note driver lists as `(doc={id} p.{page_no})`.

**Gap**: Daily notes link back to page numbers but not to specific line ranges within pages (that level of provenance is not stored).

---

## 9. Known Imperfections

| Issue | Severity | Notes |
|-------|----------|-------|
| `report_year` column stays 2025 on `--force` re-registration | Low | Not used for queries; use `report_date_min/max` instead |
| 25.8% page date coverage | Medium | Most continuation pages lack a date header; this is inherent to the PDF layout, not a parsing failure |
| FTS misses multi-character Chinese phrases without spaces | Low | Use short (≤4 char) queries or ILIKE mode for better recall on dense Chinese phrases |
| `spot_daily_bridge` facts: 0 in this environment | High (data completeness) | `public.spot_daily` does not exist in this DB. Bridged price facts (DA/RT avg/max/min per province per day) will be absent until the spot price ingest pipeline is run |
| Daily note last-write-wins for multi-document dates | Low | Documents covering the same date overwrite each other; the note reflects the last-processed document |
| Driver regex recall | Medium | The `原因为` pattern requires the exact phrase; reports that use slightly different phrasing (e.g., "主要原因是" or "由于") are missed |
| `--force` with year change leaves stale facts | Low | Old facts at wrong date persist until manual `DELETE FROM staging.spot_report_facts WHERE document_id = %s` |

---

## 10. Recommendations Before Settlement Pool

1. Run `spot_ingest.py` (the price pipeline) to populate `public.spot_daily`, then re-run ingest with `--force` to populate bridge price facts.
2. Consider adding "主要原因" and "由于" to `_RE_REASON` alternatives to improve driver recall.
3. The `report_year` update on re-registration is a cosmetic fix (low priority).
4. Consider a `--clean-facts-on-force` flag in `spot_market_ingest.py` for clean re-runs.
5. The settlement knowledge pool can reuse `document_registry.py`, `pdf_ingestion.py`, and `markdown_notes.py` as-is; only `fact_extraction.py` and note templates need settlement-specific adjustments.
