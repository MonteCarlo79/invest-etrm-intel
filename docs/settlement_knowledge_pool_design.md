# Settlement Knowledge Pool — Execution-Ready Design Spec

**Date**: 2026-04-15 (hardened from initial design, post corpus + DB scan)
**Status**: Design only. No implementation yet.
**Baseline**: Spot market knowledge pool frozen at commit `4ca18bc`.
**Pre-scan completed**: `db/ddl/` tree and `data/raw/settlement/invoices/` fully enumerated.

---

## 0. Pre-Scan Findings: Existing DDL and Code

A full scan of `db/ddl/` revealed the following relevant existing work. This materially changes the implementation plan — several tables that were proposed are either already built or must align with existing schema.

### Existing tables that are authoritative — use directly

| Table | File | How settlement pool uses it |
|-------|------|-----------------------------|
| `core.asset_alias_map` | `db/ddl/core/asset_alias_map.sql` + `_seed.sql` | **Canonical asset identity**. Already has 8 assets with dispatch_unit_name_cn, short_name_cn, display_name_cn, tt_asset_name_en, market_key alias types. Need only one new alias_type added (see §A below). |
| `core.document_registry` | `db/ddl/core/document_registry.sql` | Business-level document registry with `asset_codes[]`, `settlement_month`, `period_start/end`, `processing_status`. Settlement docs should link here via FK. |
| `raw_data.file_registry` | `db/ddl/raw_data/file_registry.sql` | Physical file registry (S3 path, hash, parse_status). Settlement `settlement_report_documents` links here optionally if files are in S3. Local-only runs leave `file_registry_id` NULL. |
| `core.asset_monthly_compensation` | `db/ddl/core/asset_monthly_compensation.sql` | Expected compensation rate per asset per month (`compensation_yuan_per_mwh`). Used as the expectation baseline in reconciliation flag thresholds. |

### Existing staging tables — superseded, not reused

| Table | File | Status |
|-------|------|--------|
| `staging.mengxi_settlement_extracted` | `db/ddl/staging/mengxi_settlement_extracted.sql` | Simple one-value-per-file extraction (discharge_mwh, settlement_yuan). Superseded by `staging.settlement_report_facts` which has richer schema. Migration path: existing rows can be backfilled as `fact_type='energy_mwh'` / `fact_type='total_amount'` with `source_method='prior_extract'` after implementation. |
| `staging.mengxi_compensation_extracted` | `db/ddl/staging/mengxi_compensation_extracted.sql` | One compensation_yuan per asset per month. Superseded. Same migration path. |

### §A: One required seed addition to `core.asset_alias_map`

The invoice file directories and bracket codes (B-6, B-7, etc.) are not yet in `core.asset_alias_map`. They must be added as a new `alias_type = 'invoice_dir_code'` to enable the ingest script to resolve filenames to asset slugs without hardcoding.

```sql
-- db/ddl/core/asset_alias_map_seed.sql — APPEND the following rows:
INSERT INTO core.asset_alias_map (asset_code, alias_type, alias_value, province, city_cn) VALUES
('wulanchabu', 'invoice_dir_code', 'B-1',    'Mengxi', '乌兰察布'),
('suyou',      'invoice_dir_code', 'B-6',    'Mengxi', '锡林郭勒'),
('wulate',     'invoice_dir_code', 'B-7',    'Mengxi', '巴彦淖尔'),
('hangjinqi',  'invoice_dir_code', 'B-8',    'Mengxi', '鄂尔多斯'),
('hetao',      'invoice_dir_code', 'B-9',    'Mengxi', '巴彦淖尔'),
('gushanliang','invoice_dir_code', 'B-10',   'Mengxi', '鄂尔多斯'),
('siziwangqi', 'invoice_dir_code', 'B-11',   'Mengxi', '乌兰察布'),
('wuhai',      'invoice_dir_code', 'B-外',   'Mengxi', '乌海')
ON CONFLICT (asset_code, alias_type, alias_value) DO NOTHING;
```

The ingest script resolves `asset_slug` at runtime:
```python
SELECT asset_code FROM core.asset_alias_map
WHERE alias_type = 'invoice_dir_code' AND lower(alias_value) = lower(%s)
```

---

## 1. Objective

Build a structured knowledge pool for BESS settlement documents. The pipeline ingests monthly per-asset settlement PDFs, extracts typed facts (energy quantities, charge components, totals), generates Obsidian-compatible notes, and maintains a reconciliation trail when multiple invoice versions exist for the same asset-period.

**Primary use cases:**
- Per-asset per-month charge component breakdown with full source provenance
- Cross-month energy and revenue time series per asset
- Cross-asset comparison of capacity compensation and penalty charges
- Reconciliation of revised invoices (same asset+month, different documents)

---

## 2. Source Corpus — Hardened Description

### 2.1 File locations

```
data/raw/settlement/invoices/{asset_dir}/{year}年结算单/   ← per-asset PDFs
data/raw/settlement/compensation/                          ← standalone all-asset compensation tables
data/extracted_settlement/{asset_dir}/{year}年结算单/      ← copies/alternates (treated identically)
```

### 2.2 Invoice types (complete enumeration from corpus scan)

| `invoice_type` value | Chinese name | Scope | Notes |
|---------------------|-------------|-------|-------|
| `grid_injection` | 上网结算单 | One asset, one period | Majority of files |
| `grid_withdrawal` | 下网电费清单 | One asset, one period | Paired with grid_injection |
| `rural_grid` | 农网结算单 | One asset, one period | **New in 2026**; observed B-1 and B-7 |
| `capacity_compensation` | 容量补偿费用统计表 | All assets, one month | Multi-asset table PDF |

**Note**: `rural_grid` was absent from the initial design but observed in `B-1/2026年结算单/B-1乌兰察布储能电站2026年01月农网结算单.pdf` and equivalent B-7 files. Treatment is identical to `grid_injection` for extraction purposes.

### 2.3 Filename naming conventions (two patterns)

The file naming convention changed between 2025 and 2026. The ingest script must handle both.

**Pattern A — 2024/2025** (bracket prefix format):
```
{M}月 【{B-code}-上/下】{asset_name}{description}.pdf
Examples:
  10月 【B-6-上】苏右10月上网结算单.pdf       → grid_injection, month=10
  10月 【B-6-下】苏右10月下网结算单-ok.pdf    → grid_withdrawal, month=10
  9月 【B-1-上】 乌兰察布储能2024年9月协同结算单.pdf
```

Regex to parse:
```python
# Group 1: month, Group 2: B-code, Group 3: direction (上/下)
_RE_FNAME_A = re.compile(r"^(\d{1,2})月.+【(B-[\w外]+)-([上下])", re.UNICODE)
```

**Pattern B — 2026** (inline format):
```
{B-code}{asset_name}{YYYY}年{MM}月上/下网电费结算单.pdf
Examples:
  B-1乌兰察布储能电站2026年01月上网电费结算单.pdf   → grid_injection, month=1, year=2026
  B-7-下】远景乌拉特储能电站2026年1月农网电费结算单.pdf  → rural_grid, month=1
```

Regex to parse:
```python
# Group 1: B-code, Group 2: year, Group 3: month, Group 4: direction keyword
_RE_FNAME_B = re.compile(
    r"(B-[\w外]+).{0,20}(\d{4})年0?(\d{1,2})月.{0,10}(上网|下网|农网)",
    re.UNICODE,
)
```

**Fallback**: if neither pattern matches, extract month from directory year + manual `settlement_month` from page text. Log as `parse_confidence='low'` in the document row.

**Non-PDF files** (`.png`, `.lnk`, `.xlsx`): excluded by `glob("*.pdf")`. No registration, no error.

### 2.4 Asset directory → asset_slug mapping

Resolved at ingest time via `core.asset_alias_map` query (§A). The directory prefix (first token matching `B-\d+` or `B-外`) is the lookup key.

| Directory name | B-code | `asset_slug` |
|---------------|--------|-------------|
| B-1 乌兰察布 | B-1 | wulanchabu |
| B-6 内蒙苏右 | B-6 | suyou |
| B-7 内蒙乌拉特 | B-7 | wulate |
| B-8 内蒙杭锦旗 | B-8 | hangjinqi |
| B-9 内蒙巴盟 | B-9 | hetao |
| B-10谷山梁 | B-10 | gushanliang |
| B-11 内蒙四子王旗 | B-11 | siziwangqi |
| B-【外】乌海储能 | B-外 | wuhai |

---

## 3. Resolved Design Rules (former open questions)

### Rule 1 — Canonical Asset Identity

**Resolution**: `core.asset_alias_map` is the single source of truth.

- Every fact, document, and note row uses `asset_slug` (e.g., `suyou`) as the stable key, not the B-code.
- `settlement_report_documents` stores **both** `asset_slug` (stable) and `invoice_dir_code` (e.g., `B-6`, for display and reverse-lookup).
- At ingest time, the B-code extracted from the filename is resolved to `asset_slug` by querying `core.asset_alias_map WHERE alias_type='invoice_dir_code'`. If no match is found, the document is registered with `ingest_status='unresolved_asset'` and skipped from fact extraction pending manual mapping.
- The station name in the PDF text (e.g., "景蓝乌尔图储能电站") is **not used** for resolution — too variable across documents. Only the B-code from the filename or directory is used.
- To add a new asset: INSERT a row into `core.asset_alias_map` with `alias_type='invoice_dir_code'`, then re-run with `--force` for unresolved documents.

### Rule 2 — Scanned and Non-Text PDFs

**Resolution**: register and stub, do not error, do not skip silently.

- The ingest script runs pdfplumber on every `.pdf` file. If `total_chars == 0` across all pages, the document is marked `ingest_status='empty'` with `parse_error='no_text_layer; likely scanned image'`.
- A **stub note** is generated for every `empty` document. The stub note contains: document metadata (asset, month, invoice_type, filename), a `## Parser Caveats` section with `⚠ Scanned PDF — no text extracted. Manual entry required.`, and empty sections for all fact types.
- The stub note appears in the monthly asset note's `## Provenance` section with a flag: `[SCAN] {filename} — no text layer, values absent`.
- Numeric fields in the monthly note that rely on this document are omitted (not zero) and marked `(no data — scanned PDF)`.
- `.png` files in the directory (e.g., `4月 【B-6-上】苏右截图4月.png`) are excluded by the `glob("*.pdf")` call and **not registered**. This is acceptable because the corresponding month will show `(no data — scanned PDF)` in the note due to the missing invoice, which correctly prompts manual entry.

### Rule 3 — Period-Half vs Monthly Roll-Up

**Resolution**: all aggregation is to full calendar month; `period_half` is a document-level annotation only.

From the corpus scan, there are no true 上半月/下半月 splits in the data. Instead, the multi-document patterns observed are:
- **Catch-up/commissioning supplements**: e.g., `3月 【B-6-上-含1月调试期】苏右3月及1月上网电量结算单-OK.pdf` (covers both March regular + January trial period). This carries `period_notes='含1月调试期'` at document level. Its facts are tagged with the fiscal month from the filename (March = month 3) but the fact_text records the actual date range.
- **Dual-issuer invoices**: e.g., B-7 February has `【B-7-上-交易中心-宣定】` and `【B-7-上-场站】` — two invoices for the same asset+month+type from different issuers. These are treated as **separate documents** both with `invoice_type='grid_injection'`, `settlement_month=2`. The second document triggers a reconciliation check (see Rule 4).

**Aggregation rule**: for the monthly asset note, facts from all documents sharing the same `(asset_slug, settlement_year, settlement_month, invoice_type)` are **summed** for energy quantities and **kept distinct** (one row per document) for amounts. If two `total_amount` facts exist for the same asset+month+type, they are flagged in the reconciliation table rather than summed — dual totals indicate a revision, not additive billing.

**`period_half` column**: retained in `settlement_report_documents` but populated from filename parsing only:
- Contains `'full'` for standard monthly invoices
- Contains `'commissioning_supplement'` for filenames matching `含.*调试期|含.*试运行期`
- Contains `'issuer_{label}'` for dual-issuer patterns where the bracket code contains a sub-tag (e.g., `交易中心`, `场站`)
- Allowed values: `full | commissioning_supplement | issuer_trading_center | issuer_plant | other`

### Rule 4 — Correction Invoice Lineage

**Resolution**: content-hash deduplication + same-(asset, month, type) collision → automatic reconciliation row.

**File identity**: SHA-256 of file content (inherited from `document_registry.py`'s `sha256_file()`). Each unique file content = one document row regardless of filename. Renamed files with same content = same hash = skip.

**Revision detection rule**: after registering a new document, the ingest script checks:
```sql
SELECT id FROM staging.settlement_report_documents
WHERE asset_slug = %s
  AND settlement_year = %s
  AND settlement_month = %s
  AND invoice_type = %s
  AND id != %s                  -- not the newly registered doc
  AND ingest_status = 'parsed'
  AND period_half = %s          -- same period_half to avoid flagging commissioning docs
```
If one or more prior versions exist:
- All combinations of (prior_doc_id, new_doc_id) for the same `(fact_type, component_name)` are compared and written to `staging.settlement_reconciliation`.
- A delta ≥ 1% of `value_a` or ≥ 500 元 (whichever is smaller) sets `flagged = TRUE`.
- The flag threshold is configurable as a constant at the top of `settlement_fact_extraction.py`:
  ```python
  RECON_FLAG_PCT_THRESHOLD = 0.01   # 1%
  RECON_FLAG_ABS_THRESHOLD = 500    # yuan
  ```
- Both versions remain in `settlement_report_documents` with `ingest_status='parsed'`. Neither is marked superseded — the analyst decides which is authoritative via the reconciliation note.

**Filename suffix noise** (`-ok`, `-OK`, `new`, spaces): irrelevant because identity is content-hash based. Two files with different suffixes but identical content are deduplicated correctly.

**Overwrite scenario** (same filename, replaced on disk): the hash changes → new document_id → reconciliation row. This is handled correctly.

### Rule 5 — Charge Component Normalization Taxonomy

**Resolution**: three-tier taxonomy with canonical names, aliases, and group.

**Rule**: extracted component names are normalized to canonical form before storage. Raw text names are stored in `fact_text`; `component_name` column always holds the canonical form.

```
Tier 1: GROUP — top-level billing category
Tier 2: CANONICAL_NAME — stored in settlement_report_facts.component_name
Tier 3: ALIASES — raw text variants observed in PDFs that map to this canonical name
```

| Group | Canonical `component_name` | Raw aliases (normalize to canonical) |
|-------|---------------------------|--------------------------------------|
| **energy** | `峰段电费` | 峰电量电费, 峰时电费, 峰段上网电费 |
| **energy** | `谷段电费` | 谷电量电费, 谷时电费, 谷段上网电费 |
| **energy** | `平段电费` | 平电量电费, 平时电费, 平段上网电费 |
| **energy** | `上网电量` | 上网电量, 发电量, 注入电量 (unit: kWh or MWh) |
| **energy** | `下网电量` | 下网电量, 用电量, 取电量 (unit: kWh or MWh) |
| **compensation** | `储能容量补偿费` | 容量补偿费, 储能容量补偿, 独立储能容量补偿费, 容量补偿 |
| **ancillary** | `调频辅助服务费` | 调频服务费, AGC辅助服务费, 频率调节服务费 |
| **ancillary** | `备用辅助服务费` | 备用服务费, 旋转备用费 |
| **power_quality** | `无功电量费` | 无功电量费, 无功补偿费 |
| **power_quality** | `力率调整费` | 力率调整费, 功率因数调整费 |
| **capacity** | `基本电费` | 基本电费, 容量电费 |
| **tax** | `增值税` | 增值税, VAT |
| **tax** | `附加税` | 城市维护建设税, 教育费附加, 附加税费, 地方教育附加 |
| **penalty** | `偏差考核费` | 偏差考核费, 考核费用, 电量偏差费 |
| **penalty** | `违约金` | 违约金, 罚款 |
| **total** | `合计` | 合计, 总计, 结算总金额 |
| **total** | `应付金额` | 应付金额, 应付电费, 甲方应付 |
| **total** | `应收金额` | 应收金额, 应收电费, 乙方应收 |

**Normalization implementation**:
```python
# In settlement_fact_extraction.py
_COMPONENT_ALIASES: dict[str, str] = {
    "峰电量电费": "峰段电费",
    "峰时电费": "峰段电费",
    "谷电量电费": "谷段电费",
    "容量补偿费": "储能容量补偿费",
    "储能容量补偿": "储能容量补偿费",
    "独立储能容量补偿费": "储能容量补偿费",
    # ... full alias map
}

def normalize_component(raw: str) -> str:
    """Return canonical component name, or raw if not in alias map."""
    return _COMPONENT_ALIASES.get(raw.strip(), raw.strip())
```

**Unknown components**: if a matched component name is not in `_KNOWN_COMPONENTS` (the allowlist), store it with `component_name = 'unknown:' + raw_text[:50]` and `confidence='low'`. This ensures unknown items appear in notes with a flag rather than being silently dropped.

---

## 4. End-to-End Example: B-6 苏右, October 2025

**Input files**:
- `10月 【B-6-上】苏右10月上网结算单.pdf` → grid_injection
- `10月 【B-6-下】苏右10月下网结算单-ok.pdf` → grid_withdrawal
- `2025年10月储能容量补偿费用统计表（发电厂）.pdf` → capacity_compensation (covers all assets)

**Step 1 — Filename parsing** (per file, before registration):
```python
# Pattern A match on "10月 【B-6-上】苏右10月上网结算单.pdf"
month=10, b_code="B-6", direction="上" → invoice_type="grid_injection"
# Resolve asset:
SELECT asset_code FROM core.asset_alias_map
  WHERE alias_type='invoice_dir_code' AND alias_value='B-6'
# → asset_slug = "suyou"
# year from directory: "2025年结算单/" → settlement_year=2025
```

**Step 2 — Registration** (`staging.settlement_report_documents`):
```
doc_id=42: suyou, 2025, 10, grid_injection, hash=abc..., status=pending
doc_id=43: suyou, 2025, 10, grid_withdrawal, hash=def..., status=pending
doc_id=51: NULL asset (capacity_compensation covers all), 2025, 10, capacity_compensation, hash=ghi...
```

**Step 3 — Page extraction**:
- pdfplumber extracts N pages → `staging.settlement_report_pages` (doc_id=42, page_no=1..N, extracted_text, char_count)
- Chunked → `staging.settlement_report_chunks` (chunk_type: header/table/amount_line/body)

**Step 4 — Fact extraction** (doc_id=42, invoice_type=grid_injection):
```
fact_type=energy_mwh,      component_name=上网电量,      metric_value=1234.5,  unit=MWh,  source_method=pdf_regex,       confidence=medium, page_no=1
fact_type=charge_component, component_name=峰段电费,       metric_value=45678.0, unit=yuan, source_method=table_extraction, confidence=high,   page_no=2
fact_type=charge_component, component_name=谷段电费,       metric_value=12345.0, unit=yuan, source_method=table_extraction, confidence=high,   page_no=2
fact_type=charge_component, component_name=储能容量补偿费, metric_value=8900.0,  unit=yuan, source_method=table_extraction, confidence=high,   page_no=2
fact_type=total_amount,     component_name=合计,           metric_value=102345.0,unit=yuan, source_method=pdf_regex,       confidence=medium, page_no=3
```
→ `staging.settlement_report_facts` (5 rows for doc_id=42)

For capacity_compensation (doc_id=51): multi-asset table → one fact per asset per row:
```
asset_slug=suyou,      settlement_month=10, fact_type=charge_component, component_name=储能容量补偿费, metric_value=8900.0
asset_slug=wulate,     settlement_month=10, fact_type=charge_component, component_name=储能容量补偿费, metric_value=9200.0
... (all 8 assets)
```

**Step 5 — Reconciliation check**:
```python
# After inserting facts for doc_id=42:
prior_docs = SELECT id FROM staging.settlement_report_documents
             WHERE asset_slug='suyou' AND settlement_year=2025
               AND settlement_month=10 AND invoice_type='grid_injection'
               AND id != 42 AND ingest_status='parsed' AND period_half='full'
# → [] (no prior docs for this period) → no reconciliation rows created
```

**Step 6 — Status update**:
```
doc_id=42: ingest_status='parsed', page_count=N, report_date_min=2025-10-01, report_date_max=2025-10-31
```

**Step 7 — Note generation** (`settlement_generate_notes.py --asset suyou --year 2025 --month 10`):
```
key = "suyou_2025-10"
path = knowledge/settlement/01_monthly/suyou/2025-10.md
```
Note content:
```markdown
---
note_type: monthly_asset
asset_slug: suyou
asset_name_cn: 苏右
invoice_dir_code: B-6
settlement_year: 2025
settlement_month: 10
document_ids: [42, 43, 51]
generated_at: 2026-04-15
---

# B-6 苏右 — 2025年10月结算

## Settlement Summary

| Item | Grid Injection (上网) | Grid Withdrawal (下网) | Net |
|------|-----------------------|-----------------------|-----|
| Energy (MWh) | 1,234.5 | 456.7 | +777.8 |
| Total (元) | 102,345 | 23,456 | +78,889 |

## Charge Components — Grid Injection (上网)

| Component | Group | Amount (元) | Source |
|-----------|-------|-------------|--------|
| 峰段电费 | energy | 45,678 | doc=42 p.2 |
| 谷段电费 | energy | 12,345 | doc=42 p.2 |
| 储能容量补偿费 | compensation | 8,900 | doc=42 p.2 |
| 合计 | total | 102,345 | doc=42 p.3 |

## Capacity Compensation (from all-asset table)

| Asset | Amount (元) | Source |
|-------|-------------|--------|
| 苏右 (suyou) | 8,900 | doc=51 p.1 |
| *(cross-check: matches grid_injection component — consistent)* |

## Provenance

- Grid injection: `10月 【B-6-上】苏右10月上网结算单.pdf` (doc_id=42, 3 pages)
- Grid withdrawal: `10月 【B-6-下】苏右10月下网结算单-ok.pdf` (doc_id=43, 2 pages)
- Capacity compensation: `2025年10月储能容量补偿费用统计表（发电厂）.pdf` (doc_id=51)

## Parser Caveats

- component `平段电费`: not found in grid_injection — invoice may not have flat-rate period; not flagged
- totals check: grid_injection 合计 (102,345) vs sum of components (66,923) — gap: 35,422 元 unattributed; review page 2

## Reconciliation

No prior versions found for this asset-period. No reconciliation.
```

---

## 5. Framework Reuse — Revised

### Reused as-is (no modification)

| Module | What is reused |
|--------|---------------|
| `services/knowledge_pool/db.py` | `get_conn()` — identical env fallback chain |
| `services/knowledge_pool/document_registry.py` | `sha256_file()` helper function only |

### Reused in spirit, re-implemented for settlement tables (Option B per original design)

| Function | Settlement equivalent | Reason for re-implementation |
|----------|-----------------------|------------------------------|
| `extract_and_store_pages()` | `extract_and_store_settlement_pages()` | Writes to `settlement_report_pages`; date inference targets `YYYY年M月` not `月日` |
| `build_and_store_chunks()` | `build_and_store_settlement_chunks()` | Writes to `settlement_report_chunks`; chunk_type adds `amount_line` |

### Not reused (settlement-specific implementations)

| Module | Why not reused |
|--------|----------------|
| `fact_extraction.py` | All regex targets spot market phrases (原因为, 均价, 省份); settlement needs energy/amount/component patterns |
| `markdown_notes.py` | All templates are spot-market–specific; settlement note structure differs fundamentally |

---

## 6. Revised Database Schema

File: `db/ddl/staging/settlement_report_knowledge.sql`

**Changes from initial design** based on scan findings:
- `asset_code` renamed to `asset_slug` throughout (aligns with `core.asset_alias_map.asset_code`)
- Added `invoice_dir_code` column for display/reverse-lookup
- Added `core_document_id` FK to `core.document_registry` (optional; NULL if not pre-registered)
- `period_half` constrained to allowed values
- `settlement_report_facts` unique key includes `period_half` to allow commissioning docs alongside regular monthly docs
- `settlement_reconciliation` adds `flag_threshold_pct` and `flag_threshold_abs` for traceability

### 6.1 `staging.settlement_report_documents`

```sql
CREATE TABLE IF NOT EXISTS staging.settlement_report_documents (
    id                  bigserial       PRIMARY KEY,
    source_path         text            NOT NULL,
    file_name           text            NOT NULL,
    asset_slug          text,                        -- NULL for capacity_compensation (multi-asset)
    invoice_dir_code    text,                        -- e.g. 'B-6'; display only
    settlement_year     smallint        NOT NULL,
    settlement_month    smallint        NOT NULL CHECK (settlement_month BETWEEN 1 AND 12),
    period_half         text            NOT NULL DEFAULT 'full',
                                                     -- full | commissioning_supplement |
                                                     -- issuer_trading_center | issuer_plant | other
    invoice_type        text            NOT NULL,
                                                     -- grid_injection | grid_withdrawal |
                                                     -- rural_grid | capacity_compensation
    period_notes        text,                        -- free-text suffix from filename (e.g. '含1月调试期')
    report_date_min     date,
    report_date_max     date,
    file_hash           text            NOT NULL,
    file_size_bytes     bigint,
    page_count          int,
    ingest_status       text            NOT NULL DEFAULT 'pending',
                                                     -- pending | parsed | empty | unresolved_asset | error
    parser_version      text            NOT NULL DEFAULT 'v1',
    parse_error         text,
    core_document_id    uuid            REFERENCES core.document_registry(document_id),
    created_at          timestamptz     NOT NULL DEFAULT now(),
    updated_at          timestamptz     NOT NULL DEFAULT now(),
    UNIQUE (file_hash)
);

CREATE INDEX IF NOT EXISTS idx_srd_settl_asset   ON staging.settlement_report_documents(asset_slug);
CREATE INDEX IF NOT EXISTS idx_srd_settl_period  ON staging.settlement_report_documents(settlement_year, settlement_month);
CREATE INDEX IF NOT EXISTS idx_srd_settl_type    ON staging.settlement_report_documents(invoice_type);
CREATE INDEX IF NOT EXISTS idx_srd_settl_status  ON staging.settlement_report_documents(ingest_status);
```

### 6.2 `staging.settlement_report_pages` and `staging.settlement_report_chunks`

Identical schema to spot equivalents (see §3 in original design). No schema changes. GIN FTS index on `settlement_report_chunks.chunk_text`.

### 6.3 `staging.settlement_report_facts`

```sql
CREATE TABLE IF NOT EXISTS staging.settlement_report_facts (
    id                  bigserial   PRIMARY KEY,
    document_id         bigint      NOT NULL REFERENCES staging.settlement_report_documents(id) ON DELETE CASCADE,
    asset_slug          text        NOT NULL,
    settlement_year     smallint    NOT NULL,
    settlement_month    smallint    NOT NULL,
    period_half         text        NOT NULL DEFAULT 'full',
    invoice_type        text        NOT NULL,
    fact_type           text        NOT NULL,
                                             -- energy_mwh | energy_kwh | charge_component
                                             -- total_amount | capacity_compensation | penalty
    component_name      text,                -- canonical normalized name (see §3 Rule 5 taxonomy)
    component_group     text,                -- energy | compensation | ancillary | power_quality
                                             -- capacity | tax | penalty | total
    metric_value        numeric,
    metric_unit         text,                -- yuan | kWh | MWh | yuan/MWh
    fact_text           text        NOT NULL, -- verbatim source phrase / table cell
    page_no             smallint    NOT NULL,
    confidence          text        NOT NULL DEFAULT 'medium',
    source_method       text        NOT NULL DEFAULT 'pdf_regex',
                                             -- pdf_regex | table_extraction | manual_entry | prior_extract
    created_at          timestamptz NOT NULL DEFAULT now(),
    UNIQUE (document_id, asset_slug, fact_type, component_name, period_half)
);
```

### 6.4 `staging.settlement_reconciliation`

```sql
CREATE TABLE IF NOT EXISTS staging.settlement_reconciliation (
    id                  bigserial   PRIMARY KEY,
    asset_slug          text        NOT NULL,
    settlement_year     smallint    NOT NULL,
    settlement_month    smallint    NOT NULL,
    invoice_type        text        NOT NULL,
    fact_type           text        NOT NULL,
    component_name      text,
    version_a_doc_id    bigint      NOT NULL REFERENCES staging.settlement_report_documents(id),
    version_b_doc_id    bigint      NOT NULL REFERENCES staging.settlement_report_documents(id),
    value_a             numeric,
    value_b             numeric,
    delta               numeric GENERATED ALWAYS AS (value_b - value_a) STORED,
    delta_pct           numeric GENERATED ALWAYS AS (
                            CASE WHEN value_a <> 0
                            THEN ROUND((value_b - value_a) / ABS(value_a) * 100, 4)
                            ELSE NULL END
                        ) STORED,
    flagged             boolean     NOT NULL DEFAULT FALSE,
    flag_reason         text,       -- '1% threshold exceeded' | '500 yuan threshold exceeded'
    flag_threshold_pct  numeric     NOT NULL DEFAULT 1.0,
    flag_threshold_abs  numeric     NOT NULL DEFAULT 500.0,
    created_at          timestamptz NOT NULL DEFAULT now(),
    UNIQUE (asset_slug, settlement_year, settlement_month, invoice_type,
            fact_type, component_name, version_a_doc_id, version_b_doc_id)
);
```

### 6.5 `staging.settlement_report_notes`

No schema changes from initial design. `note_key` uses `asset_slug` not B-code.

---

## 7. New Python Modules (unchanged from initial design, updated signatures)

```
services/knowledge_pool/
  settlement_ingestion.py          ~130 lines
  settlement_fact_extraction.py    ~250 lines  (larger: includes full taxonomy map)
  settlement_markdown_notes.py     ~280 lines
  settlement_retrieval.py          ~90 lines

scripts/
  settlement_ingest.py             ~180 lines
  settlement_generate_notes.py     ~100 lines
  settlement_query.py              ~120 lines
```

Key function signatures:

```python
# settlement_ingestion.py
def extract_and_store_settlement_pages(doc_id, pdf_path, settlement_year, settlement_month) -> tuple[int, date|None, date|None]
def build_and_store_settlement_chunks(doc_id) -> int

# settlement_fact_extraction.py
def normalize_component(raw: str) -> str
def extract_facts_for_settlement_document(doc_id, invoice_type, asset_slug, settlement_year, settlement_month) -> int
def run_reconciliation_check(doc_id, asset_slug, settlement_year, settlement_month, invoice_type, period_half) -> int

# settlement_markdown_notes.py
def generate_monthly_asset_note(asset_slug, year, month, out_dir) -> Path
def generate_asset_summary_note(asset_slug, out_dir) -> Path
def generate_component_note(component_name, year, month, out_dir) -> Path
def generate_reconciliation_note(asset_slug, year, month, out_dir) -> Path

# settlement_retrieval.py
def search_settlement_chunks(query, asset_slug, invoice_type, year, month, limit) -> list[dict]
def get_settlement_facts(fact_type, asset_slug, component_name, year, month, limit) -> list[dict]
def get_reconciliation_deltas(asset_slug, flagged_only, year, month) -> list[dict]
```

---

## 8. Provenance Requirements (unchanged + one addition)

Every fact row: `document_id` + `page_no` + `source_method` + `fact_text` all required (NOT NULL).

**Addition**: `fact_text` must include enough context to locate the source in the page. Minimum: the full line or table row from which the value was extracted. For `table_extraction` facts, `fact_text` = the raw cell text + its row label.

---

## 9. Complete File Inventory (revised)

### Files to create (new)

```
db/ddl/core/asset_alias_map_seed.sql         ← APPEND 8 invoice_dir_code rows (§A above)
db/ddl/staging/settlement_report_knowledge.sql ← 5 new staging tables

services/knowledge_pool/
  settlement_ingestion.py
  settlement_fact_extraction.py
  settlement_markdown_notes.py
  settlement_retrieval.py

scripts/
  settlement_ingest.py
  settlement_generate_notes.py
  settlement_query.py
```

### Files to modify (one only)

```
db/ddl/core/asset_alias_map_seed.sql   ← append 8 rows; no schema changes
```

### Existing files unchanged (spot pool frozen)

All `services/knowledge_pool/` existing modules, all `scripts/spot_market_*.py` scripts, all spot DDL. Zero modifications.

### Existing staging tables superseded (not deleted, migration deferred)

```
staging.mengxi_settlement_extracted    ← superseded; backfill migration deferred to post-implementation
staging.mengxi_compensation_extracted  ← superseded; same
```

---

## 10. Implementation Sequence

1. Append `invoice_dir_code` rows to `db/ddl/core/asset_alias_map_seed.sql` and apply
2. Create `db/ddl/staging/settlement_report_knowledge.sql` and apply
3. Implement `services/knowledge_pool/settlement_ingestion.py`
4. Implement `services/knowledge_pool/settlement_fact_extraction.py` (with taxonomy map and reconciliation check)
5. Implement `scripts/settlement_ingest.py` (filename parser + orchestration)
6. Smoke test: ingest B-6 2025 (all 12 months), verify fact counts, reconciliation rows
7. Implement `services/knowledge_pool/settlement_markdown_notes.py`
8. Implement `scripts/settlement_generate_notes.py`
9. Full corpus ingest: all assets, all years
10. Implement `services/knowledge_pool/settlement_retrieval.py` + `scripts/settlement_query.py`
