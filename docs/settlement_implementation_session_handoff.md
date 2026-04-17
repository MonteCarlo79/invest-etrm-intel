# Settlement Knowledge Pool — Implementation Session Handoff

**Date**: 2026-04-16
**Branch**: `cost-optimisation`
**Last commit**: `9b23bd5` (hardened design spec)
**Status**: Implementation NOT yet started. All tasks pending.

---

## 0. What To Do Next

Resume implementation from **Task 26** (DDL). Follow the task list in §2 in order.
Do NOT implement anything until DDL is applied and confirmed.
Validate on smoke-test subset before full corpus.

---

## 1. Critical Pre-Scan Findings (already incorporated into design spec)

### 1.1 Existing DB tables already in place — reuse, do not recreate

| Table | Location | How used |
|-------|----------|---------|
| `core.asset_alias_map` | `db/ddl/core/asset_alias_map.sql` | Canonical asset identity. **Needs one seed addition** (§3 below) |
| `core.asset_alias_map_seed.sql` | `db/ddl/core/asset_alias_map_seed.sql` | Seeded with 8 assets (suyou, wulate, etc.) but missing `invoice_dir_code` aliases |
| `core.document_registry` | `db/ddl/core/document_registry.sql` | Business registry with `asset_codes[]`, `settlement_month`. FK link optional |
| `raw_data.file_registry` | `db/ddl/raw_data/file_registry.sql` | Physical S3 file registry. FK link optional (NULL for local runs) |
| `core.asset_monthly_compensation` | `db/ddl/core/asset_monthly_compensation.sql` | Expected compensation rate per asset per month |
| `staging.mengxi_settlement_extracted` | `db/ddl/staging/mengxi_settlement_extracted.sql` | Simple older extraction. Superseded — do not modify |
| `staging.mengxi_compensation_extracted` | `db/ddl/staging/mengxi_compensation_extracted.sql` | Simple older extraction. Superseded — do not modify |

### 1.2 Asset directory → asset_slug mapping (confirmed)

| Invoice directory | B-code | `asset_slug` in core.asset_alias_map |
|------------------|--------|---------------------------------------|
| B-1 乌兰察布 | B-1 | wulanchabu |
| B-6 内蒙苏右 | B-6 | suyou |
| B-7 内蒙乌拉特 | B-7 | wulate |
| B-8 内蒙杭锦旗 | B-8 | hangjinqi |
| B-9 内蒙巴盟 | B-9 | hetao |
| B-10谷山梁 | B-10 | gushanliang |
| B-11 内蒙四子王旗 | B-11 | siziwangqi |
| B-【外】乌海储能 | B-外 | wuhai |

### 1.3 PDF text extraction findings (from pdfplumber sampling)

**Key discovery**: Most per-asset invoices (B-6 上网, B-1 上网) have **0 chars** from pdfplumber — they are scanned/image PDFs. Only the **下网** (grid withdrawal) PDFs and the **capacity compensation** tables have extractable text.

| File sampled | Pages | Chars | Verdict |
|-------------|-------|-------|---------|
| B-6 Oct 上网 | 1 | 0 | Scanned — register as `empty` |
| B-6 Oct 下网 | 3 | 955+1174+888 | **Text available** — extract |
| B-1 Sep 上网 | 1 | 0 | Scanned |
| Capacity compensation Oct | 17 | ~3000/page | **Text available** — multi-asset table |

### 1.4 Actual invoice text structure (B-6 Oct 下网, confirmed from pdfplumber)

The 下网 (grid withdrawal) invoice is issued by Inner Mongolia State Grid (内蒙古电网). Structure:

**Page 1 — Summary**:
```
总电费 7,107,568.13 元
1 市场上网电费 + 2 辅助服务费 + 3 目录电费 + 4 贴(补)贴... + 5 低碳辅助服务费 +
6 峰谷电费差 + 7 系统运行费 + 8 力率调整费 + 9 政策性损益及附加 + 10 退补电费 + 11 退补费
= 6576452.43 + 0 + 0 + 0 + 208284.64 + 0 + 376540.64 + (-53709.58) + 0 + 0 + 0
```

**Page 2 — Line item detail**:
```
1 市场上网电费（含规市场中市场交易电量+合同偏差电量）
  电量: 16,961,290.0 kWh  单价: 0.3877330339 元/kWh  金额: 6,576,452.43

5 低碳辅助服务费(3.07%)/(1-3.07%)*市场上网电量/合同电量
  16,961,290 × 0.01228 = 208,284.64

7 系统运行费 = 376,540.64 元
  含: 水库维护费 × 0.002 = 33,922.58
      系统运行费(电能量) × 0.0006 = 10,176.77
      系统运行费(平衡量) × 0.0009 = 15,265.16
      电力市场摊销费 × 0.0008 = 13,569.03
      调频辅助服务结算 × 0.0052 = 88,198.71
      燃煤发电费 × 0.0125 = 212,016.13 (dominant item)

8 力率调整费(功率因数考核) = -53,709.58
  220kV: 功率因数 0.95, 标准 0.90 → 系数 -0.0075
```

**Page 3 — Detail list**:
```
电量明细: meter readings per measurement point
总量: 16,961,290 kWh
```

### 1.5 Capacity compensation table structure (confirmed)

```
列: 序号 | 公司名称 | 机组名称 | 10月储能容量补偿费用(元) | 以前月度清算费用（元）| 10月合计储能容量补偿费用（元）
```

Section I (储能电站享受容量补偿费用明细) — our assets:
- Row 4: 苏尼特右旗景蓝新能源有限公司 → 景蓝乌尔图储能电站 → 5,147,092.62 元
- Row 5: 乌拉特中旗昭瑞新能源有限公司 → 远景乌拉特储能电站 → 4,221,126.93 元
- Row 6: 乌海市远鸿富景新能源科技有限公司 → 富景五虎山储能电站 → 6,050,650.96 元

Resolution: match by `机组名称` (dispatch unit name) against `core.asset_alias_map WHERE alias_type='dispatch_unit_name_cn'`.

### 1.6 Updated component taxonomy (from actual invoice text)

The real component names in the 下网 invoice differ from the initial taxonomy. **Use these canonical names**:

| Item# | Raw invoice text | Canonical `component_name` | Group |
|-------|-----------------|---------------------------|-------|
| 1 | 市场上网电费 | 市场上网电费 | energy |
| 2 | 辅助服务费 | 辅助服务费 | ancillary |
| 3 | 目录电费 | 目录电费 | energy |
| 4 | 贴（补）贴 | 补贴电费 | subsidy |
| 5 | 低碳辅助服务费 | 低碳辅助服务费 | ancillary |
| 6 | 峰谷电费差 | 峰谷电费差 | energy |
| 7 | 系统运行费 | 系统运行费 | system |
| 7a | (sub) 调频辅助服务结算 | 调频辅助服务费 | ancillary |
| 7b | (sub) 燃煤发电费 | 燃煤发电费 | system |
| 7c | (sub) 水库维护费 | 水库维护费 | system |
| 8 | 力率调整费 | 力率调整费 | power_quality |
| 9 | 政策性损益及附加 | 政策性损益 | policy |
| 10 | 退补电费 | 退补电费 | adjustment |
| 11 | 退补费 | 退补费 | adjustment |
| — | 总电费 / 合计 | 总电费 | total |

**Energy quantity extracted from item 1 line detail**: `(\d[\d,.]+)\s*kWh` on the line containing `0.3877` or after `16,961,290`.

---

## 2. Task List and Status

| # | Task | Status | Notes |
|---|------|--------|-------|
| 26 | DDL: settlement_report_knowledge.sql + asset_alias seed | **IN PROGRESS** | Not yet applied to DB |
| 27 | Implement settlement_ingestion.py | Pending | |
| 28 | Implement settlement_fact_extraction.py | Pending | See §1.4 for real patterns |
| 29 | Implement scripts/settlement_ingest.py | Pending | |
| 30 | Smoke-test: B-6 + B-1 + B-7 + comp_oct | Pending | B-6 上网 will be empty (scanned) |
| 31 | settlement_markdown_notes.py + generate_notes.py | Pending | |
| 32 | settlement_retrieval.py + settlement_query.py | Pending | |
| 33 | Full corpus + validation report | Pending | |

---

## 3. Step-by-Step Implementation Instructions

### STEP 1 — Seed addition to asset_alias_map_seed.sql

**File**: `db/ddl/core/asset_alias_map_seed.sql`
**Action**: Append these rows (do NOT truncate or replace existing content):

```sql
-- invoice_dir_code aliases — append to existing seed file
INSERT INTO core.asset_alias_map (asset_code, alias_type, alias_value, province, city_cn) VALUES
('wulanchabu', 'invoice_dir_code', 'B-1',  'Mengxi', '乌兰察布'),
('suyou',      'invoice_dir_code', 'B-6',  'Mengxi', '锡林郭勒'),
('wulate',     'invoice_dir_code', 'B-7',  'Mengxi', '巴彦淖尔'),
('hangjinqi',  'invoice_dir_code', 'B-8',  'Mengxi', '鄂尔多斯'),
('hetao',      'invoice_dir_code', 'B-9',  'Mengxi', '巴彦淖尔'),
('gushanliang','invoice_dir_code', 'B-10', 'Mengxi', '鄂尔多斯'),
('siziwangqi', 'invoice_dir_code', 'B-11', 'Mengxi', '乌兰察布'),
('wuhai',      'invoice_dir_code', 'B-外', 'Mengxi', '乌海')
ON CONFLICT (asset_code, alias_type, alias_value) DO NOTHING;
```

Apply: `psql $PGURL -f db/ddl/core/asset_alias_map_seed.sql`
(The seed file starts with TRUNCATE — that's fine, it also re-inserts everything.)

**VERIFY**: `SELECT asset_code, alias_value FROM core.asset_alias_map WHERE alias_type='invoice_dir_code' ORDER BY alias_value;` → should return 8 rows.

---

### STEP 2 — Create DDL file

**File**: `db/ddl/staging/settlement_report_knowledge.sql`
**Content**: See §4 below (full DDL).
Apply: `psql $PGURL -f db/ddl/staging/settlement_report_knowledge.sql`

**VERIFY**: `\dt staging.settlement_report_*` → 5 tables.

---

### STEP 3 — settlement_ingestion.py

**File**: `services/knowledge_pool/settlement_ingestion.py`
**Purpose**: page extraction + chunking writing to `settlement_report_pages` and `settlement_report_chunks`.
**Key difference from pdf_ingestion.py**: writes to settlement_* tables; `_infer_settlement_period()` uses `YYYY年M月` pattern.

**Full implementation**: See §5 below.

---

### STEP 4 — settlement_fact_extraction.py

**File**: `services/knowledge_pool/settlement_fact_extraction.py`
**Purpose**: filename parser, energy/component extraction, normalization, reconciliation check.

**Critical note from sampling**: Most 上网 PDFs are scanned. Extraction will mainly work on:
- 下网 invoices (grid withdrawal) — numbered items with amounts
- capacity_compensation tables — multi-row table, match by 机组名称

**Full implementation**: See §6 below.

---

### STEP 5 — scripts/settlement_ingest.py

**File**: `scripts/settlement_ingest.py`
**Full implementation**: See §7 below.

---

### STEP 6 — Smoke test (MANDATORY before full corpus)

```bash
# From bess-platform repo root, with DB_URL in apps/spot-agent/.env

# Test 1: B-6 (scanned 上网 + text 下网 + compensation)
/c/ProgramData/anaconda3/python.exe scripts/settlement_ingest.py \
  --asset-dir "B-6 内蒙苏右" --year 2025 --limit-months 10 --init-db

# Test 2: B-1 (different naming, 2024 and 2025)
/c/ProgramData/anaconda3/python.exe scripts/settlement_ingest.py \
  --asset-dir "B-1 乌兰察布" --year 2024 --limit-months 3

# Test 3: B-7 (dual-issuer Feb 2025)
/c/ProgramData/anaconda3/python.exe scripts/settlement_ingest.py \
  --asset-dir "B-7 内蒙乌拉特" --year 2025 --limit-months 2

# Test 4: capacity_compensation only
/c/ProgramData/anaconda3/python.exe scripts/settlement_ingest.py \
  --compensation-only --year 2025

# Check DB
psql $PGURL -c "SELECT asset_slug, invoice_type, ingest_status, COUNT(*) FROM staging.settlement_report_documents GROUP BY 1,2,3 ORDER BY 1,2;"
psql $PGURL -c "SELECT fact_type, component_name, COUNT(*), SUM(metric_value) FROM staging.settlement_report_facts GROUP BY 1,2 ORDER BY 3 DESC LIMIT 30;"
psql $PGURL -c "SELECT COUNT(*) FROM staging.settlement_reconciliation WHERE flagged=true;"
```

Expected:
- B-6 上网 Oct: `ingest_status='empty'` (scanned)
- B-6 下网 Oct: `ingest_status='parsed'`, facts for 市场上网电费, 系统运行费, 力率调整费, 总电费
- capacity_compensation: facts with `fact_type='capacity_compensation'` for suyou, wulate, wuhai, etc.
- B-7 Feb: two grid_injection docs → reconciliation rows (flagged if amounts differ)

---

### STEP 7 — Notes + retrieval (after smoke test passes)

Implement in order:
1. `services/knowledge_pool/settlement_markdown_notes.py`
2. `scripts/settlement_generate_notes.py`
3. `services/knowledge_pool/settlement_retrieval.py`
4. `scripts/settlement_query.py`

Then run full corpus and write validation report.

---

## 4. Full DDL — `db/ddl/staging/settlement_report_knowledge.sql`

```sql
-- db/ddl/staging/settlement_report_knowledge.sql
-- Settlement knowledge pool tables
-- Additive only. Does not touch spot_report_* or any existing table.

CREATE SCHEMA IF NOT EXISTS staging;

-- ============================================================
-- 1. Source document registry
-- ============================================================
CREATE TABLE IF NOT EXISTS staging.settlement_report_documents (
    id                  bigserial       PRIMARY KEY,
    source_path         text            NOT NULL,
    file_name           text            NOT NULL,
    asset_slug          text,           -- NULL for capacity_compensation (multi-asset)
    invoice_dir_code    text,           -- e.g. 'B-6'; display / reverse-lookup
    settlement_year     smallint        NOT NULL,
    settlement_month    smallint        NOT NULL CHECK (settlement_month BETWEEN 1 AND 12),
    period_half         text            NOT NULL DEFAULT 'full',
                        -- full|commissioning_supplement|issuer_trading_center|issuer_plant|other
    invoice_type        text            NOT NULL,
                        -- grid_injection|grid_withdrawal|rural_grid|capacity_compensation
    period_notes        text,
    report_date_min     date,
    report_date_max     date,
    file_hash           text            NOT NULL,
    file_size_bytes     bigint,
    page_count          int,
    ingest_status       text            NOT NULL DEFAULT 'pending',
                        -- pending|parsed|empty|unresolved_asset|error
    parser_version      text            NOT NULL DEFAULT 'v1',
    parse_error         text,
    core_document_id    uuid,           -- optional FK to core.document_registry
    created_at          timestamptz     NOT NULL DEFAULT now(),
    updated_at          timestamptz     NOT NULL DEFAULT now(),
    UNIQUE (file_hash)
);

CREATE INDEX IF NOT EXISTS idx_srd_settl_asset   ON staging.settlement_report_documents(asset_slug);
CREATE INDEX IF NOT EXISTS idx_srd_settl_period  ON staging.settlement_report_documents(settlement_year, settlement_month);
CREATE INDEX IF NOT EXISTS idx_srd_settl_type    ON staging.settlement_report_documents(invoice_type);
CREATE INDEX IF NOT EXISTS idx_srd_settl_status  ON staging.settlement_report_documents(ingest_status);

-- ============================================================
-- 2. Per-page raw text
-- ============================================================
CREATE TABLE IF NOT EXISTS staging.settlement_report_pages (
    id                  bigserial       PRIMARY KEY,
    document_id         bigint          NOT NULL REFERENCES staging.settlement_report_documents(id) ON DELETE CASCADE,
    page_no             smallint        NOT NULL,
    page_date           date,
    extracted_text      text,
    char_count          int,
    extraction_method   text            NOT NULL DEFAULT 'pdfplumber',
    created_at          timestamptz     NOT NULL DEFAULT now(),
    UNIQUE (document_id, page_no)
);

CREATE INDEX IF NOT EXISTS idx_srp_settl_doc  ON staging.settlement_report_pages(document_id);

-- ============================================================
-- 3. Chunked text (GIN FTS)
-- ============================================================
CREATE TABLE IF NOT EXISTS staging.settlement_report_chunks (
    id                  bigserial       PRIMARY KEY,
    document_id         bigint          NOT NULL REFERENCES staging.settlement_report_documents(id) ON DELETE CASCADE,
    page_no             smallint,
    chunk_index         int             NOT NULL,
    chunk_text          text            NOT NULL,
    chunk_type          text            NOT NULL DEFAULT 'body',
                        -- body|table|header|amount_line
    report_date         date,
    created_at          timestamptz     NOT NULL DEFAULT now(),
    UNIQUE (document_id, chunk_index)
);

CREATE INDEX IF NOT EXISTS idx_src_settl_doc ON staging.settlement_report_chunks(document_id);
CREATE INDEX IF NOT EXISTS idx_src_settl_fts ON staging.settlement_report_chunks
    USING gin(to_tsvector('simple', chunk_text));

-- ============================================================
-- 4. Structured extracted facts
-- ============================================================
CREATE TABLE IF NOT EXISTS staging.settlement_report_facts (
    id                  bigserial       PRIMARY KEY,
    document_id         bigint          NOT NULL REFERENCES staging.settlement_report_documents(id) ON DELETE CASCADE,
    asset_slug          text            NOT NULL,
    settlement_year     smallint        NOT NULL,
    settlement_month    smallint        NOT NULL,
    period_half         text            NOT NULL DEFAULT 'full',
    invoice_type        text            NOT NULL,
    fact_type           text            NOT NULL,
                        -- energy_mwh|energy_kwh|charge_component|total_amount|capacity_compensation|penalty
    component_name      text,           -- canonical normalized name
    component_group     text,           -- energy|ancillary|system|power_quality|subsidy|policy|adjustment|total
    metric_value        numeric,
    metric_unit         text,           -- yuan|kWh|MWh|yuan/MWh
    fact_text           text            NOT NULL,
    page_no             smallint        NOT NULL,
    confidence          text            NOT NULL DEFAULT 'medium',
    source_method       text            NOT NULL DEFAULT 'pdf_regex',
                        -- pdf_regex|table_extraction|manual_entry|prior_extract
    created_at          timestamptz     NOT NULL DEFAULT now(),
    UNIQUE (document_id, asset_slug, fact_type, component_name, period_half)
);

CREATE INDEX IF NOT EXISTS idx_srf_settl_asset  ON staging.settlement_report_facts(asset_slug);
CREATE INDEX IF NOT EXISTS idx_srf_settl_period ON staging.settlement_report_facts(settlement_year, settlement_month);
CREATE INDEX IF NOT EXISTS idx_srf_settl_type   ON staging.settlement_report_facts(fact_type);
CREATE INDEX IF NOT EXISTS idx_srf_settl_comp   ON staging.settlement_report_facts(component_name);

-- ============================================================
-- 5. Reconciliation
-- ============================================================
CREATE TABLE IF NOT EXISTS staging.settlement_reconciliation (
    id                  bigserial       PRIMARY KEY,
    asset_slug          text            NOT NULL,
    settlement_year     smallint        NOT NULL,
    settlement_month    smallint        NOT NULL,
    invoice_type        text            NOT NULL,
    fact_type           text            NOT NULL,
    component_name      text,
    version_a_doc_id    bigint          NOT NULL REFERENCES staging.settlement_report_documents(id),
    version_b_doc_id    bigint          NOT NULL REFERENCES staging.settlement_report_documents(id),
    value_a             numeric,
    value_b             numeric,
    delta               numeric GENERATED ALWAYS AS (value_b - value_a) STORED,
    delta_pct           numeric GENERATED ALWAYS AS (
                            CASE WHEN value_a <> 0
                            THEN ROUND((value_b - value_a) / ABS(value_a) * 100, 4)
                            ELSE NULL END
                        ) STORED,
    flagged             boolean         NOT NULL DEFAULT FALSE,
    flag_reason         text,
    flag_threshold_pct  numeric         NOT NULL DEFAULT 1.0,
    flag_threshold_abs  numeric         NOT NULL DEFAULT 500.0,
    created_at          timestamptz     NOT NULL DEFAULT now(),
    UNIQUE (asset_slug, settlement_year, settlement_month, invoice_type,
            fact_type, component_name, version_a_doc_id, version_b_doc_id)
);

CREATE INDEX IF NOT EXISTS idx_srecon_asset  ON staging.settlement_reconciliation(asset_slug);
CREATE INDEX IF NOT EXISTS idx_srecon_period ON staging.settlement_reconciliation(settlement_year, settlement_month);
CREATE INDEX IF NOT EXISTS idx_srecon_flag   ON staging.settlement_reconciliation(flagged);

-- ============================================================
-- 6. Note registry
-- ============================================================
CREATE TABLE IF NOT EXISTS staging.settlement_report_notes (
    id                  bigserial       PRIMARY KEY,
    document_id         bigint          REFERENCES staging.settlement_report_documents(id) ON DELETE SET NULL,
    note_type           text            NOT NULL,
                        -- monthly_asset|asset_summary|charge_component|reconciliation
    note_key            text            NOT NULL,
    note_path           text            NOT NULL,
    note_title          text,
    settlement_year     smallint,
    settlement_month    smallint,
    asset_slug          text,
    generated_at        timestamptz     NOT NULL DEFAULT now(),
    updated_at          timestamptz     NOT NULL DEFAULT now(),
    UNIQUE (note_type, note_key)
);
```

---

## 5. settlement_ingestion.py — Full Implementation

```python
"""
services/knowledge_pool/settlement_ingestion.py

Page extraction + chunking for settlement PDFs.
Writes to staging.settlement_report_pages and staging.settlement_report_chunks.
Analogous to pdf_ingestion.py but uses settlement-specific tables and date inference.
"""
from __future__ import annotations

import re
from pathlib import Path
from typing import Optional
import datetime as dt

import pdfplumber

from .db import get_conn


def _infer_settlement_period(text: str) -> tuple[int | None, int | None]:
    """Extract YYYY and M from page text like '2025年10月'."""
    m = re.search(r"(\d{4})\s*年\s*0?(\d{1,2})\s*月", text)
    if m:
        return int(m.group(1)), int(m.group(2))
    return None, None


def _chunk_text(text: str, chunk_size: int = 500, overlap: int = 100) -> list[str]:
    if not text or not text.strip():
        return []
    text = text.strip()
    chunks, start = [], 0
    while start < len(text):
        end = min(start + chunk_size, len(text))
        chunks.append(text[start:end])
        if end == len(text):
            break
        start += chunk_size - overlap
    return chunks


def _classify_chunk(text: str) -> str:
    t = text.strip()
    if re.search(r"[\d,]+\s*元", t) and re.search(r"(?:电费|服务费|补偿|调整|合计|总计)", t):
        return "amount_line"
    if re.search(r"(?:电量|电费|单价|金额|合计)\s*[\(（]", t):
        return "table"
    if re.search(r"(?:结算单|电费清单|补偿费用统计|容量补偿)", t):
        return "header"
    return "body"


def extract_and_store_settlement_pages(
    doc_id: int,
    pdf_path: Path,
    settlement_year: int,
    settlement_month: int,
) -> tuple[int, Optional[dt.date], Optional[dt.date]]:
    """
    Extract page text, write to staging.settlement_report_pages.
    Returns (page_count, date_min, date_max).
    date_min/max derived from text; falls back to first-day-of-month.
    """
    pdf_path = Path(pdf_path)
    pages_data = []

    with pdfplumber.open(str(pdf_path)) as pdf:
        for i, page in enumerate(pdf.pages, start=1):
            text = page.extract_text() or ""
            pages_data.append({
                "page_no": i,
                "extracted_text": text,
                "char_count": len(text),
            })

    if not pages_data:
        return 0, None, None

    total_chars = sum(p["char_count"] for p in pages_data)

    # Derive date range: use settlement year/month if text extraction found nothing specific
    try:
        date_min = dt.date(settlement_year, settlement_month, 1)
        # last day of month
        if settlement_month == 12:
            date_max = dt.date(settlement_year + 1, 1, 1) - dt.timedelta(days=1)
        else:
            date_max = dt.date(settlement_year, settlement_month + 1, 1) - dt.timedelta(days=1)
    except ValueError:
        date_min = date_max = None

    with get_conn() as conn:
        with conn.cursor() as cur:
            for p in pages_data:
                cur.execute(
                    """
                    INSERT INTO staging.settlement_report_pages
                        (document_id, page_no, extracted_text, char_count)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (document_id, page_no) DO UPDATE SET
                        extracted_text = EXCLUDED.extracted_text,
                        char_count     = EXCLUDED.char_count
                    """,
                    (doc_id, p["page_no"], p["extracted_text"], p["char_count"]),
                )
        conn.commit()

    return len(pages_data), date_min, date_max


def build_and_store_settlement_chunks(doc_id: int) -> int:
    """Read settlement pages for doc_id, build chunks, store in settlement_report_chunks."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT page_no, extracted_text FROM staging.settlement_report_pages "
                "WHERE document_id = %s ORDER BY page_no",
                (doc_id,),
            )
            rows = cur.fetchall()

    chunk_index = 0
    inserts = []
    for page_no, text in rows:
        if not text or not text.strip():
            continue
        for chunk_text in _chunk_text(text):
            inserts.append((doc_id, page_no, chunk_index, chunk_text, _classify_chunk(chunk_text)))
            chunk_index += 1

    if not inserts:
        return 0

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO staging.settlement_report_chunks
                    (document_id, page_no, chunk_index, chunk_text, chunk_type)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (document_id, chunk_index) DO UPDATE SET
                    chunk_text  = EXCLUDED.chunk_text,
                    chunk_type  = EXCLUDED.chunk_type
                """,
                inserts,
            )
        conn.commit()

    return len(inserts)
```

---

## 6. settlement_fact_extraction.py — Full Implementation

```python
"""
services/knowledge_pool/settlement_fact_extraction.py

Fact extraction from settlement invoice page text.
Handles: grid_injection, grid_withdrawal, rural_grid, capacity_compensation.
"""
from __future__ import annotations

import re
from typing import List, Optional

from .db import get_conn

# ── Reconciliation thresholds ────────────────────────────────────────────────
RECON_FLAG_PCT_THRESHOLD = 1.0   # percent
RECON_FLAG_ABS_THRESHOLD = 500.0 # yuan

# ── Component normalization taxonomy ─────────────────────────────────────────
# Based on actual invoice text from B-6 下网 (Inner Mongolia State Grid format)
_COMPONENT_ALIASES: dict[str, str] = {
    # Energy
    "市场上网电费（含规市场中市场交易电量+合同偏差电量）": "市场上网电费",
    "市场上网电量": "上网电量",
    "下网电量": "下网电量",
    "上网电量": "上网电量",
    "发电量": "上网电量",
    # Ancillary
    "低碳辅助服务费": "低碳辅助服务费",
    "调频辅助服务结算": "调频辅助服务费",
    "调频辅助服务费": "调频辅助服务费",
    "备用辅助服务费": "备用辅助服务费",
    "辅助服务费": "辅助服务费",
    # System
    "系统运行费": "系统运行费",
    "燃煤发电费": "燃煤发电费",
    "水库维护费": "水库维护费",
    "电力市场摊销费": "电力市场摊销费",
    # Power quality
    "力率调整费": "力率调整费",
    "功率因数调整费": "力率调整费",
    "无功电量费": "无功电量费",
    # Capacity
    "目录电费": "目录电费",
    "基本电费": "基本电费",
    "峰谷电费差": "峰谷电费差",
    # Compensation
    "储能容量补偿费用": "储能容量补偿费",
    "容量补偿费": "储能容量补偿费",
    "储能容量补偿": "储能容量补偿费",
    # Policy/adjustment
    "政策性损益及附加": "政策性损益",
    "退补电费": "退补电费",
    "退补费": "退补费",
    "补贴电费": "补贴电费",
    # Total
    "总电费": "总电费",
    "合计": "总电费",
    "应付金额": "应付金额",
    "应收金额": "应收金额",
    "结算总金额": "总电费",
}

_COMPONENT_GROUPS: dict[str, str] = {
    "市场上网电费": "energy",
    "上网电量": "energy",
    "下网电量": "energy",
    "目录电费": "energy",
    "峰谷电费差": "energy",
    "低碳辅助服务费": "ancillary",
    "辅助服务费": "ancillary",
    "调频辅助服务费": "ancillary",
    "备用辅助服务费": "ancillary",
    "系统运行费": "system",
    "燃煤发电费": "system",
    "水库维护费": "system",
    "电力市场摊销费": "system",
    "力率调整费": "power_quality",
    "无功电量费": "power_quality",
    "基本电费": "capacity",
    "储能容量补偿费": "compensation",
    "政策性损益": "policy",
    "退补电费": "adjustment",
    "退补费": "adjustment",
    "补贴电费": "subsidy",
    "总电费": "total",
    "应付金额": "total",
    "应收金额": "total",
}


def normalize_component(raw: str) -> str:
    """Return canonical component name; prefix unknown with 'unknown:'."""
    raw = raw.strip()
    if raw in _COMPONENT_ALIASES:
        return _COMPONENT_ALIASES[raw]
    # Partial match for long names
    for alias, canonical in _COMPONENT_ALIASES.items():
        if alias in raw or raw in alias:
            return canonical
    return f"unknown:{raw[:50]}"


def _parse_amount(s: str) -> Optional[float]:
    """Parse '6,576,452.43' or '-53,709.58' to float."""
    try:
        return float(s.replace(",", "").strip())
    except (ValueError, AttributeError):
        return None


# Regex for numbered line items: "7 系统运行费 ... 376,540.64"
_RE_ITEM_LINE = re.compile(
    r"^(?:\d{1,2})\s+"           # item number
    r"([\u4e00-\u9fff\(\)（）a-zA-Z%+\-\d\.]+?)"  # component name
    r"\s+[-−]?\s*([\d,]+\.?\d*)\s*$",
    re.MULTILINE,
)

# Energy quantity: "16,961,290.0 kWh" or "16,961,290 kWh"
_RE_ENERGY_KWH = re.compile(r"([\d,]+\.?\d*)\s*kWh", re.IGNORECASE)

# Total electricity fee: "总电费\n7,107,568.13" or "总电费 7,107,568.13"
_RE_TOTAL = re.compile(r"总[电]?费[（(]?[元Ԫ]?[)）]?\s*([\d,]+\.?\d*)")

# Summary line amounts: 6 numbers separated by + signs
_RE_SUMMARY_AMOUNTS = re.compile(r"([\-\d,]+\.?\d*)\s*[元Ԫ]\s*(?:[\+\-]\s*(?:[\-\d,]+\.?\d*)\s*[元Ԫ]\s*)+")


def _extract_grid_invoice_facts(text: str, page_no: int) -> list[dict]:
    """Extract facts from grid injection / withdrawal / rural_grid invoice text."""
    results = []

    # Extract numbered line items
    for m in _RE_ITEM_LINE.finditer(text):
        raw_name = m.group(1).strip()
        raw_amount = m.group(2).strip()
        canonical = normalize_component(raw_name)
        amount = _parse_amount(raw_amount)
        if amount is None:
            continue
        fact_type = "charge_component" if canonical not in ("总电费", "应付金额", "应收金额") else "total_amount"
        results.append({
            "fact_type": fact_type,
            "component_name": canonical,
            "component_group": _COMPONENT_GROUPS.get(canonical, "other"),
            "metric_value": amount,
            "metric_unit": "yuan",
            "fact_text": m.group(0)[:300],
            "page_no": page_no,
            "confidence": "medium",
            "source_method": "pdf_regex",
        })

    # Extract energy quantity (kWh)
    for m in _RE_ENERGY_KWH.finditer(text):
        kwh = _parse_amount(m.group(1))
        if kwh and kwh > 1000:  # skip small/spurious matches
            results.append({
                "fact_type": "energy_kwh",
                "component_name": "上网电量",
                "component_group": "energy",
                "metric_value": kwh,
                "metric_unit": "kWh",
                "fact_text": text[max(0, m.start()-30):m.end()+10][:200],
                "page_no": page_no,
                "confidence": "medium",
                "source_method": "pdf_regex",
            })
            break  # take first large kWh match only

    # Extract total from 总电费 pattern
    for m in _RE_TOTAL.finditer(text):
        total = _parse_amount(m.group(1))
        if total and total > 0:
            results.append({
                "fact_type": "total_amount",
                "component_name": "总电费",
                "component_group": "total",
                "metric_value": total,
                "metric_unit": "yuan",
                "fact_text": m.group(0)[:200],
                "page_no": page_no,
                "confidence": "medium",
                "source_method": "pdf_regex",
            })
            break

    return results


def _extract_compensation_facts(text: str, page_no: int, asset_map: dict) -> list[dict]:
    """
    Extract per-asset compensation facts from capacity_compensation table PDF.
    asset_map: {dispatch_unit_name_cn: asset_slug}
    """
    results = []
    # Match rows: "景蓝乌尔图储能电站 5,147,092.62 5,147,092.62"
    # or: "company_name  dispatch_name  amount  prior  total"
    for line in text.split("\n"):
        line = line.strip()
        # Look for known dispatch unit names
        for dispatch_name, slug in asset_map.items():
            if dispatch_name in line:
                # Extract last numeric amount on the line (合计 column)
                nums = re.findall(r"([\-\d,]+\.?\d+)", line)
                if nums:
                    # Take the last positive numeric (合计)
                    for n in reversed(nums):
                        val = _parse_amount(n)
                        if val is not None and val > 0:
                            results.append({
                                "asset_slug_override": slug,  # special field for compensation
                                "fact_type": "capacity_compensation",
                                "component_name": "储能容量补偿费",
                                "component_group": "compensation",
                                "metric_value": val,
                                "metric_unit": "yuan",
                                "fact_text": line[:300],
                                "page_no": page_no,
                                "confidence": "high",
                                "source_method": "table_extraction",
                            })
                            break
    return results


def extract_facts_for_settlement_document(
    doc_id: int,
    invoice_type: str,
    asset_slug: str,
    settlement_year: int,
    settlement_month: int,
    period_half: str = "full",
    asset_map: Optional[dict] = None,  # {dispatch_unit_name: asset_slug} for capacity_compensation
) -> int:
    """
    Read pages for doc_id, extract facts, store in staging.settlement_report_facts.
    Returns count of fact rows written.
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT page_no, extracted_text FROM staging.settlement_report_pages "
                "WHERE document_id = %s ORDER BY page_no",
                (doc_id,),
            )
            pages = cur.fetchall()

    all_facts: list[dict] = []

    for page_no, text in pages:
        if not text or not text.strip():
            continue

        if invoice_type == "capacity_compensation":
            if asset_map is None:
                continue
            facts = _extract_compensation_facts(text, page_no, asset_map)
        else:
            # grid_injection / grid_withdrawal / rural_grid
            facts = _extract_grid_invoice_facts(text, page_no)

        all_facts.extend(facts)

    if not all_facts:
        return 0

    # Deduplicate by (component_name, fact_type) — keep highest confidence
    seen: dict[tuple, dict] = {}
    for f in all_facts:
        slug = f.pop("asset_slug_override", asset_slug)
        key = (slug, f["fact_type"], f.get("component_name", ""), period_half)
        if key not in seen or f["confidence"] == "high":
            seen[key] = {**f, "_asset_slug": slug}

    inserts = []
    for (slug, ft, cn, ph), f in seen.items():
        inserts.append((
            doc_id, slug, settlement_year, settlement_month, ph,
            invoice_type, ft, cn, f.get("component_group"),
            f.get("metric_value"), f.get("metric_unit"),
            f["fact_text"], f["page_no"],
            f["confidence"], f["source_method"],
        ))

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO staging.settlement_report_facts
                    (document_id, asset_slug, settlement_year, settlement_month, period_half,
                     invoice_type, fact_type, component_name, component_group,
                     metric_value, metric_unit, fact_text, page_no, confidence, source_method)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (document_id, asset_slug, fact_type, component_name, period_half)
                DO UPDATE SET
                    metric_value   = EXCLUDED.metric_value,
                    fact_text      = EXCLUDED.fact_text,
                    source_method  = EXCLUDED.source_method
                """,
                inserts,
            )
        conn.commit()

    return len(inserts)


def run_reconciliation_check(
    doc_id: int,
    asset_slug: str,
    settlement_year: int,
    settlement_month: int,
    invoice_type: str,
    period_half: str,
) -> int:
    """
    Check if prior parsed docs exist for same asset/period/type/period_half.
    If yes, compare facts and write reconciliation rows.
    Returns number of reconciliation rows written.
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            # Find prior documents
            cur.execute(
                """
                SELECT id FROM staging.settlement_report_documents
                WHERE asset_slug = %s
                  AND settlement_year = %s AND settlement_month = %s
                  AND invoice_type = %s AND period_half = %s
                  AND id != %s AND ingest_status = 'parsed'
                """,
                (asset_slug, settlement_year, settlement_month, invoice_type, period_half, doc_id),
            )
            prior_ids = [r[0] for r in cur.fetchall()]

    if not prior_ids:
        return 0

    recon_rows = []
    with get_conn() as conn:
        with conn.cursor() as cur:
            # Get facts for new doc
            cur.execute(
                "SELECT fact_type, component_name, metric_value FROM staging.settlement_report_facts "
                "WHERE document_id = %s",
                (doc_id,),
            )
            new_facts = {(r[0], r[1]): r[2] for r in cur.fetchall()}

            for prior_id in prior_ids:
                cur.execute(
                    "SELECT fact_type, component_name, metric_value FROM staging.settlement_report_facts "
                    "WHERE document_id = %s",
                    (prior_id,),
                )
                prior_facts = {(r[0], r[1]): r[2] for r in cur.fetchall()}

                for (ft, cn), val_b in new_facts.items():
                    val_a = prior_facts.get((ft, cn))
                    if val_a is None or val_b is None:
                        continue
                    delta = float(val_b) - float(val_a)
                    flagged = False
                    flag_reason = None
                    if val_a != 0:
                        pct = abs(delta) / abs(float(val_a)) * 100
                        if pct >= RECON_FLAG_PCT_THRESHOLD:
                            flagged = True
                            flag_reason = f"{pct:.2f}% threshold exceeded"
                    if abs(delta) >= RECON_FLAG_ABS_THRESHOLD:
                        flagged = True
                        flag_reason = (flag_reason or "") + f"; {abs(delta):.2f} yuan threshold exceeded"

                    recon_rows.append((
                        asset_slug, settlement_year, settlement_month, invoice_type,
                        ft, cn, prior_id, doc_id, val_a, val_b,
                        flagged, flag_reason,
                        RECON_FLAG_PCT_THRESHOLD, RECON_FLAG_ABS_THRESHOLD,
                    ))

    if not recon_rows:
        return 0

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO staging.settlement_reconciliation
                    (asset_slug, settlement_year, settlement_month, invoice_type,
                     fact_type, component_name, version_a_doc_id, version_b_doc_id,
                     value_a, value_b, flagged, flag_reason,
                     flag_threshold_pct, flag_threshold_abs)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT DO NOTHING
                """,
                recon_rows,
            )
        conn.commit()

    return len(recon_rows)
```

---

## 7. scripts/settlement_ingest.py — Full Implementation

```python
#!/usr/bin/env python3
"""
Settlement knowledge pool — PDF ingestion CLI.

Scans data/raw/settlement/invoices/{asset_dir}/{year}年结算单/*.pdf
and data/raw/settlement/compensation/*.pdf.

Usage:
    python scripts/settlement_ingest.py --year 2025
    python scripts/settlement_ingest.py --asset-dir "B-6 内蒙苏右" --year 2025
    python scripts/settlement_ingest.py --compensation-only --year 2025
    python scripts/settlement_ingest.py --init-db
    python scripts/settlement_ingest.py --year 2025 --force
"""
from __future__ import annotations

import argparse
import hashlib
import re
import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_REPO))

try:
    from dotenv import load_dotenv
    for _env in [_REPO / ".env", _REPO / "apps" / "spot-agent" / ".env"]:
        if _env.exists():
            load_dotenv(_env)
            break
except ImportError:
    pass

from services.knowledge_pool.db import get_conn
from services.knowledge_pool.settlement_ingestion import (
    extract_and_store_settlement_pages,
    build_and_store_settlement_chunks,
)
from services.knowledge_pool.settlement_fact_extraction import (
    extract_facts_for_settlement_document,
    run_reconciliation_check,
)

# ── Constants ────────────────────────────────────────────────────────────────
DEFAULT_INVOICES_DIR = _REPO / "data" / "raw" / "settlement" / "invoices"
DEFAULT_COMP_DIR     = _REPO / "data" / "raw" / "settlement" / "compensation"

# Filename regex patterns (two naming conventions)
_RE_FNAME_A = re.compile(
    r"^(\d{1,2})\s*月.+?【(B-[\w\u5916]+)-([上下])(?:-([\w\u4e00-\u9fff]+))?】",
    re.UNICODE,
)
_RE_FNAME_B = re.compile(
    r"(B-[\w\u5916]+).{0,30}(\d{4})\s*年\s*0?(\d{1,2})\s*月.{0,20}(上网|下网|农网)",
    re.UNICODE,
)
_RE_COMP_FNAME = re.compile(r"(\d{4})\s*年\s*0?(\d{1,2})\s*月.*?补偿")

_DIRECTION_MAP = {
    "上": "grid_injection",
    "下": "grid_withdrawal",
    "上网": "grid_injection",
    "下网": "grid_withdrawal",
    "农网": "rural_grid",
}

_PERIOD_HALF_KEYWORDS = {
    "交易中心": "issuer_trading_center",
    "宣定": "issuer_trading_center",
    "场站": "issuer_plant",
    "含1月调试期": "commissioning_supplement",
    "含调试期": "commissioning_supplement",
    "试运行": "commissioning_supplement",
    "上半月": "first_half",
    "下半月": "second_half",
}


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def resolve_asset_slug(b_code: str) -> str | None:
    """Query core.asset_alias_map for invoice_dir_code → asset_slug."""
    # Normalize B-外 variants
    b_code = b_code.replace("【外】", "外").replace("外】", "外")
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT asset_code FROM core.asset_alias_map "
                "WHERE alias_type='invoice_dir_code' AND lower(alias_value)=lower(%s)",
                (b_code,),
            )
            row = cur.fetchone()
    return row[0] if row else None


def get_dispatch_name_map() -> dict[str, str]:
    """Return {dispatch_unit_name_cn: asset_slug} for capacity_compensation matching."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT alias_value, asset_code FROM core.asset_alias_map "
                "WHERE alias_type='dispatch_unit_name_cn'"
            )
            return {row[0]: row[1] for row in cur.fetchall()}


def parse_filename(fname: str, year_from_dir: int) -> dict | None:
    """
    Parse invoice filename → {month, b_code, invoice_type, period_half, period_notes}.
    Returns None if file should be skipped (non-PDF, compensation table placed in asset dir).
    """
    stem = fname
    # Detect compensation table placed in asset dir
    if "容量补偿" in stem:
        return None  # handled separately by compensation scanner

    # Pattern A: "10月 【B-6-上】..."
    m = _RE_FNAME_A.match(stem)
    if m:
        month = int(m.group(1))
        b_code = m.group(2)
        direction = m.group(3)
        sub_tag = m.group(4) or ""
        invoice_type = _DIRECTION_MAP.get(direction, "grid_injection")
        period_half = "full"
        period_notes = None
        for kw, ph in _PERIOD_HALF_KEYWORDS.items():
            if kw in stem:
                period_half = ph
                period_notes = kw
                break
        return {
            "settlement_month": month,
            "settlement_year": year_from_dir,
            "b_code": b_code,
            "invoice_type": invoice_type,
            "period_half": period_half,
            "period_notes": period_notes,
        }

    # Pattern B: "B-6景蓝乌尔图储能2026年01月上网电费结算单"
    m = _RE_FNAME_B.search(stem)
    if m:
        b_code = m.group(1)
        year = int(m.group(2))
        month = int(m.group(3))
        direction_str = m.group(4)
        invoice_type = _DIRECTION_MAP.get(direction_str, "grid_injection")
        period_half = "full"
        period_notes = None
        for kw, ph in _PERIOD_HALF_KEYWORDS.items():
            if kw in stem:
                period_half = ph
                period_notes = kw
                break
        return {
            "settlement_month": month,
            "settlement_year": year,
            "b_code": b_code,
            "invoice_type": invoice_type,
            "period_half": period_half,
            "period_notes": period_notes,
        }

    return None  # unrecognized pattern


def register_settlement_document(
    pdf: Path, asset_slug: str | None, invoice_dir_code: str | None,
    settlement_year: int, settlement_month: int, period_half: str,
    invoice_type: str, period_notes: str | None,
) -> tuple[int, bool]:
    """Register in staging.settlement_report_documents. Returns (doc_id, is_new)."""
    file_hash = sha256_file(pdf)
    file_size = pdf.stat().st_size

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id FROM staging.settlement_report_documents WHERE file_hash=%s",
                (file_hash,),
            )
            row = cur.fetchone()
            if row:
                return row[0], False

            cur.execute(
                """
                INSERT INTO staging.settlement_report_documents
                    (source_path, file_name, asset_slug, invoice_dir_code,
                     settlement_year, settlement_month, period_half, invoice_type,
                     period_notes, file_hash, file_size_bytes, ingest_status)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'pending')
                RETURNING id
                """,
                (str(pdf), pdf.name, asset_slug, invoice_dir_code,
                 settlement_year, settlement_month, period_half, invoice_type,
                 period_notes, file_hash, file_size),
            )
            doc_id = cur.fetchone()[0]
        conn.commit()
    return doc_id, True


def set_status(doc_id: int, status: str, page_count: int | None = None,
               parse_error: str | None = None):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE staging.settlement_report_documents
                SET ingest_status=%s, page_count=COALESCE(%s,page_count),
                    parse_error=%s, updated_at=now()
                WHERE id=%s
                """,
                (status, page_count, parse_error, doc_id),
            )
        conn.commit()


def ingest_one(
    pdf: Path,
    asset_slug: str | None,
    invoice_dir_code: str | None,
    settlement_year: int,
    settlement_month: int,
    period_half: str,
    invoice_type: str,
    period_notes: str | None,
    force: bool,
    asset_map: dict,  # for capacity_compensation
) -> str:  # returns 'processed' | 'skipped' | 'empty' | 'error' | 'unresolved'
    if asset_slug is None and invoice_type != "capacity_compensation":
        print(f"  [UNRESOLVED] {pdf.name} — no asset_slug", flush=True)
        register_settlement_document(pdf, None, invoice_dir_code, settlement_year,
                                     settlement_month, period_half, invoice_type, period_notes)
        # update status
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE staging.settlement_report_documents SET ingest_status='unresolved_asset' "
                    "WHERE file_hash=%s", (sha256_file(pdf),))
            conn.commit()
        return "unresolved"

    doc_id, is_new = register_settlement_document(
        pdf, asset_slug, invoice_dir_code, settlement_year, settlement_month,
        period_half, invoice_type, period_notes,
    )

    if not is_new and not force:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT ingest_status FROM staging.settlement_report_documents WHERE id=%s", (doc_id,))
                row = cur.fetchone()
        if row and row[0] in ("parsed", "empty"):
            print(f"  [SKIP] {pdf.name} (doc_id={doc_id}, status={row[0]})", flush=True)
            return "skipped"

    print(f"  [INGEST] {pdf.name} (doc_id={doc_id})", flush=True)

    try:
        page_count, date_min, date_max = extract_and_store_settlement_pages(
            doc_id, pdf, settlement_year, settlement_month
        )
        print(f"    pages={page_count}", flush=True)

        if page_count == 0 or all(
            p[0] == 0 for p in [(0,)]  # placeholder; check total chars below
        ):
            # Check total chars
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT COALESCE(SUM(char_count),0) FROM staging.settlement_report_pages WHERE document_id=%s",
                        (doc_id,),
                    )
                    total_chars = cur.fetchone()[0]
            if total_chars == 0:
                set_status(doc_id, "empty", page_count=page_count,
                           parse_error="no_text_layer; likely scanned image")
                print(f"    [EMPTY] no text extracted (scanned PDF)", flush=True)
                return "empty"

        chunk_count = build_and_store_settlement_chunks(doc_id)
        print(f"    chunks={chunk_count}", flush=True)

        fact_count = extract_facts_for_settlement_document(
            doc_id=doc_id,
            invoice_type=invoice_type,
            asset_slug=asset_slug or "unknown",
            settlement_year=settlement_year,
            settlement_month=settlement_month,
            period_half=period_half,
            asset_map=asset_map,
        )
        print(f"    facts={fact_count}", flush=True)

        set_status(doc_id, "parsed", page_count=page_count)

        # Reconciliation check
        if asset_slug and invoice_type != "capacity_compensation":
            recon_count = run_reconciliation_check(
                doc_id, asset_slug, settlement_year, settlement_month,
                invoice_type, period_half,
            )
            if recon_count:
                print(f"    recon_rows={recon_count}", flush=True)

        return "processed"

    except Exception as e:
        set_status(doc_id, "error", parse_error=str(e)[:500])
        print(f"    [ERROR] {e}", flush=True)
        return "error"


def init_settlement_tables():
    ddl_path = _REPO / "db" / "ddl" / "staging" / "settlement_report_knowledge.sql"
    if not ddl_path.exists():
        print(f"[ERROR] DDL not found: {ddl_path}", flush=True)
        sys.exit(1)
    import subprocess
    import os
    pgurl = (os.environ.get("PGURL") or os.environ.get("DB_URL") or
             os.environ.get("DATABASE_URL") or os.environ.get("MARKETDATA_DB_URL"))
    if not pgurl:
        print("[ERROR] No PGURL in environment", flush=True)
        sys.exit(1)
    subprocess.run(["psql", pgurl, "-f", str(ddl_path)], check=True)
    print("[DB] Settlement tables initialised.", flush=True)


def main():
    parser = argparse.ArgumentParser(description="Ingest settlement PDFs into knowledge pool")
    parser.add_argument("--year", type=int)
    parser.add_argument("--asset-dir", help="Asset directory name e.g. 'B-6 内蒙苏右'")
    parser.add_argument("--compensation-only", action="store_true")
    parser.add_argument("--limit-months", type=int)
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--init-db", action="store_true")
    args = parser.parse_args()

    if args.init_db:
        init_settlement_tables()

    asset_map = get_dispatch_name_map()  # for capacity_compensation

    stats = {"processed": 0, "skipped": 0, "empty": 0, "error": 0, "unresolved": 0}

    # ── Capacity compensation PDFs ────────────────────────────────────────────
    if not args.asset_dir:  # skip if asset-specific run
        comp_pdfs = sorted(DEFAULT_COMP_DIR.glob("*.pdf"))
        if args.year:
            comp_pdfs = [p for p in comp_pdfs if f"{args.year}年" in p.name]
        for pdf in comp_pdfs:
            m = _RE_COMP_FNAME.search(pdf.name)
            if not m:
                continue
            year, month = int(m.group(1)), int(m.group(2))
            result = ingest_one(
                pdf=pdf, asset_slug=None, invoice_dir_code=None,
                settlement_year=year, settlement_month=month,
                period_half="full", invoice_type="capacity_compensation",
                period_notes=None, force=args.force, asset_map=asset_map,
            )
            stats[result] = stats.get(result, 0) + 1

    if args.compensation_only:
        _print_stats(stats)
        return

    # ── Per-asset invoice PDFs ────────────────────────────────────────────────
    asset_dirs = []
    if args.asset_dir:
        d = DEFAULT_INVOICES_DIR / args.asset_dir
        if not d.exists():
            print(f"[ERROR] Asset dir not found: {d}", flush=True)
            sys.exit(1)
        asset_dirs = [d]
    else:
        asset_dirs = sorted(DEFAULT_INVOICES_DIR.iterdir()) if DEFAULT_INVOICES_DIR.exists() else []

    for asset_dir in asset_dirs:
        if not asset_dir.is_dir():
            continue

        # Extract B-code from directory name
        b_match = re.search(r"(B-[\w\u5916]+)", asset_dir.name)
        dir_b_code = b_match.group(1) if b_match else None
        asset_slug = resolve_asset_slug(dir_b_code) if dir_b_code else None
        if asset_slug is None:
            print(f"[SKIP] {asset_dir.name} — unresolved B-code {dir_b_code}", flush=True)

        # Year subdirectories
        year_dirs = []
        if args.year:
            y = asset_dir / f"{args.year}年结算单"
            if y.exists():
                year_dirs = [y]
        else:
            year_dirs = sorted(asset_dir.glob("*年结算单"))

        for year_dir in year_dirs:
            yr_match = re.search(r"(\d{4})年", year_dir.name)
            if not yr_match:
                continue
            dir_year = int(yr_match.group(1))

            pdfs = sorted(year_dir.glob("*.pdf"))
            if args.limit_months:
                # group by month and take first N months
                seen_months: set[int] = set()
                filtered = []
                for pdf in pdfs:
                    info = parse_filename(pdf.stem, dir_year)
                    if info and info["settlement_month"] not in seen_months:
                        seen_months.add(info["settlement_month"])
                        if len(seen_months) > args.limit_months:
                            break
                    filtered.append(pdf)
                pdfs = filtered

            for pdf in pdfs:
                info = parse_filename(pdf.stem, dir_year)
                if info is None:
                    print(f"  [SKIP-FNAME] {pdf.name}", flush=True)
                    continue

                result = ingest_one(
                    pdf=pdf,
                    asset_slug=asset_slug,
                    invoice_dir_code=dir_b_code,
                    settlement_year=info["settlement_year"],
                    settlement_month=info["settlement_month"],
                    period_half=info["period_half"],
                    invoice_type=info["invoice_type"],
                    period_notes=info["period_notes"],
                    force=args.force,
                    asset_map=asset_map,
                )
                stats[result] = stats.get(result, 0) + 1

    _print_stats(stats)


def _print_stats(stats: dict):
    print(f"\n[DONE] " + " ".join(f"{k}={v}" for k, v in stats.items()), flush=True)


if __name__ == "__main__":
    main()
```

---

## 8. Files Remaining To Implement

After smoke test passes, implement in order:

### `services/knowledge_pool/settlement_markdown_notes.py`
4 templates:
- `generate_monthly_asset_note(asset_slug, year, month, out_dir)` → `knowledge/settlement/01_monthly/{asset_slug}/{YYYY}-{MM}.md`
- `generate_asset_summary_note(asset_slug, out_dir)` → `knowledge/settlement/02_assets/{asset_slug}.md`
- `generate_component_note(component_name, year, month, out_dir)` → `knowledge/settlement/03_components/{name}/{YYYY}-{MM}.md`
- `generate_reconciliation_note(asset_slug, year, month, out_dir)` → `knowledge/settlement/04_reconciliation/{asset_slug}/{YYYY}-{MM}.md`

Key: every amount must cite `doc_id` + `page_no` + `fact_text`. Scanned PDFs render as `⚠ SCAN — no text extracted`.

### `scripts/settlement_generate_notes.py`
CLI wrapper calling the 4 generators.

### `services/knowledge_pool/settlement_retrieval.py`
3 functions:
- `search_settlement_chunks(query, asset_slug, invoice_type, year, month, limit)` — GIN FTS
- `get_settlement_facts(fact_type, asset_slug, component_name, year, month, limit)` — structured
- `get_reconciliation_deltas(asset_slug, flagged_only, year, month)` — reconciliation

### `scripts/settlement_query.py`
CLI for the 3 retrieval functions.

---

## 9. After Full Corpus

Write `docs/settlement_knowledge_pool_validation.md` covering:
- Document counts by asset/year/invoice_type/ingest_status
- Fact counts by fact_type and source_method
- Scanned PDF count and list
- Reconciliation rows: flagged vs total
- Sample facts for 3 assets × 2 months
- Note generation counts
- Known imperfections
