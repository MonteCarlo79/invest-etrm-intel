# 机制竞价 (136号文) Intelligence System — Design Spec
**Date:** 2026-07-11
**Status:** Approved
**Scope:** Structured tracking of 各省新能源机制竞价 results and upcoming calendar, with AI extraction, Hermes internet scanning, and Feishu integration

---

## Problem

Under 136号文, each province runs competitive bidding (机制竞价) for renewable energy capacity mechanism allocation — covering onshore wind, offshore wind, solar PV, and hydro across multiple grid-connection batches. Results and upcoming notices are scattered across provincial energy authority websites, PDFs, and PPTs. There is currently no structured store, no unified display, and no automated monitoring.

---

## Goal

1. Store historical and upcoming 机制竞价 records in structured DB tables
2. Extract structured data automatically from uploaded documents (PPT/PDF/Excel) via Claude
3. Display results in a new Streamlit tab: historical results + upcoming calendar + upload
4. Automate internet scanning in Hermes: find new provincial announcements nightly, push Feishu alerts
5. Support Feishu file upload → auto-extraction and `/机制竞价` query command

---

## Architecture

```
Documents (PPT/PDF/Excel/URL)
        │
        ▼
register_and_ingest()  →  staging.spot_knowledge_docs  (existing KB)
        │
        ▼
JizhiExtractor.extract_bids() / extract_upcoming()
  (Claude tool-use, structured output)
        │
        ├──► staging.jizhi_bids + staging.jizhi_bid_winners
        └──► staging.jizhi_upcoming

Hermes APScheduler (10:07 UTC nightly)
        │
        ▼
internet_agent searches "机制竞价 [year] [province] 公告"
        │
        ▼
extract_upcoming() → INSERT jizhi_upcoming (dedup)
        │
        ▼
If new rows: push Feishu card notification

Spot-market Streamlit tab "机制竞价"
  ├── Sub-tab 1: 历史结果 (filterable table + charts)
  ├── Sub-tab 2: 即将竞价 (upcoming calendar)
  └── Sub-tab 3: 上传 & 录入 (upload → extract → review → save)

Feishu bot
  ├── File upload with "机制竞价" / "136" in name → auto-extract → jizhi_bids
  └── "/机制竞价" command → card: upcoming bids + recent results
```

---

## Data Model

### `staging.jizhi_bids` — completed bid results

```sql
CREATE TABLE IF NOT EXISTS staging.jizhi_bids (
    id                  SERIAL PRIMARY KEY,
    province            TEXT NOT NULL,
    year                INT  NOT NULL,
    batch               TEXT NOT NULL,        -- '存量' | '增量_2025-12' | '增量_2026-12'
    tech_type           TEXT NOT NULL,        -- '陆风' | '海风' | '光伏' | '水电'
    price_floor         NUMERIC,              -- 元/kWh
    price_cap           NUMERIC,              -- 元/kWh
    mechanism_type      TEXT,                 -- '电量' | '比例' | '小时数'
    mechanism_value     NUMERIC,              -- GWh | % | hours depending on mechanism_type
    supply_demand_ratio NUMERIC,              -- e.g. 1.35 means 135% subscribed vs available
    cleared_price       NUMERIC,              -- 元/kWh
    cleared_volume_gwh  NUMERIC,              -- GWh
    bid_date            DATE,
    verified            BOOLEAN NOT NULL DEFAULT FALSE,  -- FALSE = AI-extracted, unconfirmed
    source_doc_id       INT REFERENCES staging.spot_knowledge_docs(id),
    notes               TEXT,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (province, year, batch, tech_type)
);
```

`batch` canonical values:
- `存量` — grid-connected before 2025-05-31
- `增量_2025-12` — grid-connected before 2025-12-31
- `增量_2026-12` — grid-connected before 2026-12-31
- Additional batches added per provincial rules (e.g. `增量_2027-12`)

### `staging.jizhi_bid_winners` — 中标清单 (optional sub-table)

```sql
CREATE TABLE IF NOT EXISTS staging.jizhi_bid_winners (
    id            SERIAL PRIMARY KEY,
    bid_id        INT NOT NULL REFERENCES staging.jizhi_bids(id) ON DELETE CASCADE,
    project_name  TEXT NOT NULL,
    operator      TEXT,
    capacity_mw   NUMERIC,
    cleared_price NUMERIC,
    tech_type     TEXT,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_jizhi_winners_bid ON staging.jizhi_bid_winners(bid_id);
```

### `staging.jizhi_upcoming` — upcoming bid calendar

```sql
CREATE TABLE IF NOT EXISTS staging.jizhi_upcoming (
    id                   SERIAL PRIMARY KEY,
    province             TEXT NOT NULL,
    year                 INT  NOT NULL,
    batch                TEXT NOT NULL,
    tech_type            TEXT NOT NULL,
    price_floor          NUMERIC,
    price_cap            NUMERIC,
    target_volume_gwh    NUMERIC,
    supply_demand_ratio  NUMERIC,
    bid_open_date        DATE,
    bid_close_date       DATE,
    source_url           TEXT,
    announcement_date    DATE,
    verified             BOOLEAN NOT NULL DEFAULT FALSE,
    notes                TEXT,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (province, year, batch, tech_type, bid_open_date)
);
```

---

## Components

### `services/knowledge_pool/jizhi_extractor.py` — new file

Two public functions:

**`extract_bids(text: str, api_key: str) -> list[dict]`**

Uses Claude tool-use with a single tool `save_bid_results` that enforces the schema. Prompt instructs Claude to extract all bid records from the text, normalising:
- batch names to canonical values (`存量`, `增量_2025-12`, etc.)
- tech_type to canonical values (`陆风`, `海风`, `光伏`, `水电`)
- mechanism_type + mechanism_value from whatever format the document uses (小时数/比例/电量)
- prices to 元/kWh (convert if document uses 元/MWh)

Returns `list[dict]` with keys matching `jizhi_bids` columns (excluding `id`, `source_doc_id`, `created_at`).

**`extract_upcoming(text: str, api_key: str) -> list[dict]`**

Same approach with tool `save_upcoming_bids`. Extracts notice details: province, year, batch, tech_type, price bounds, target volume, bid dates.

Returns `list[dict]` matching `jizhi_upcoming` columns.

**`save_bids(records: list[dict], source_doc_id: int | None, pg_url: str) -> int`**

Upserts records to `jizhi_bids` (ON CONFLICT (province, year, batch, tech_type) DO UPDATE if verified=FALSE). Returns count of rows inserted/updated.

**`save_upcoming(records: list[dict], pg_url: str) -> int`**

Upserts to `jizhi_upcoming` (ON CONFLICT DO UPDATE). Returns count.

**`ensure_tables(pg_url: str)`**

Creates all three tables if they don't exist.

---

### `services/hermes/app.py` — additions

**1. Nightly scan job** — registered in `create_app()` alongside existing jobs:

```python
# 机制竞价 scan: 10:07 UTC (18:07 Beijing) — search for new provincial announcements
scheduler.add_job(
    lambda: _run_jizhi_scan(os.environ.get("ANTHROPIC_API_KEY", "")),
    "cron", hour=10, minute=7,
    id="jizhi_scan_nightly",
    max_instances=1,
    misfire_grace_time=3600,
)
```

**`_run_jizhi_scan(api_key: str) -> dict`** helper:

```python
def _run_jizhi_scan(api_key: str) -> dict:
    """Search internet for new 机制竞价 announcements, insert to jizhi_upcoming."""
    # 1. Build search queries for current + next year across key provinces
    # 2. Call internet_agent with queries
    # 3. extract_upcoming() on each result page
    # 4. save_upcoming() → count new rows
    # 5. If new rows > 0: send Feishu card notification
    # Returns {"new_upcoming": int, "provinces": list[str]}
```

Search queries (iterating over key provinces):
- `"[province] 机制竞价 [year] 公告 陆风 海风 光伏"`
- `"[province] 新能源机制竞价 结果 [year]"`

Key provinces to scan: 广东, 山东, 浙江, 江苏, 湖南, 湖北, 四川, 广西, 河南, 安徽, 福建, 贵州, 云南, 内蒙古, 新疆.

**2. Feishu file routing addition** — inside the existing file message handler, add a routing rule:

```python
# If filename suggests 机制竞价 content → auto-extract + save
if any(kw in filename_lower for kw in ["机制竞价", "136", "jizhi"]):
    _handle_jizhi_file(file_bytes, filename, message_id)
```

`_handle_jizhi_file()` calls `register_and_ingest()` + `extract_bids()` + `save_bids()` then replies with a Feishu card summarising extracted records (province, year, cleared_price, cleared_volume_gwh).

**3. `/机制竞价` text command** — inside the Feishu text message handler, add command routing:

```
/机制竞价
```

Returns a Feishu card with two sections:
- **即将竞价** (next 90 days from `jizhi_upcoming`, sorted by bid_open_date)
- **最近结果** (last 5 rows from `jizhi_bids` ordered by bid_date desc)

---

### `apps/spot-market/app.py` — new tab

**New 14th tab** added to `st.tabs()` call: `"机制竞价"` (after Library).

**Sub-tab 1 — 历史结果**

- Filter row: province multiselect, year range slider, batch radio, tech_type multiselect
- Results table (`st.dataframe`) with columns: province, year, batch, tech_type, price_floor, price_cap, mechanism_value (+type), supply_demand_ratio, cleared_price, cleared_volume_gwh, bid_date, verified (⚠️ if False)
- Province+year+batch+tech_type selectbox below the table → expander shows winner list (中标清单) from `jizhi_bid_winners` for the selected bid
- Two charts below table:
  - Bar chart: avg cleared_price by province (grouped by tech_type)
  - Line chart: supply_demand_ratio trend by year and province

**Sub-tab 2 — 即将竞价**

- Table from `jizhi_upcoming` sorted by `bid_open_date`, with a "距今" (days until) computed column
- Filter: province multiselect, tech_type multiselect
- Rows with `bid_open_date` within 14 days highlighted in amber

**Sub-tab 3 — 上传 & 录入**

- File uploader (pdf, docx, pptx, xlsx, txt, jpg, png) — matches existing KB upload pattern
- On upload: calls `register_and_ingest()` + `extract_bids()` + shows editable `st.data_editor` preview
- "Save to DB" button → `save_bids()` with `source_doc_id` set
- URL input field → `register_url()` + `extract_bids()`
- Manual single-record entry form (province, year, batch, tech_type, all numeric fields) as a collapsible expander

All DB reads use a cached `@st.cache_data(ttl=300)` loader keyed on `_ENG_KEY`.

---

## Error Handling

- `extract_bids()` / `extract_upcoming()` catch all Claude API exceptions — return empty list on failure, log error
- `save_bids()` uses ON CONFLICT to avoid duplicate key errors; existing verified rows are never overwritten
- `_run_jizhi_scan()` wraps each province search in try/except — one province failing doesn't abort the scan
- Feishu file handler failures reply with an error card (never silently drop)
- Tables are created via `ensure_tables()` called at Hermes startup and at Streamlit first load

---

## Observability

- Hermes scan logs `[jizhi_scan] new_upcoming=N provinces=[...]` at INFO level after each run
- `POST /hermes/jizhi/scan` endpoint for on-demand triggering (same pattern as `/hermes/knowledge/digest`)
- Streamlit tab shows "Last scan: [timestamp]" pulled from `MAX(created_at)` of `jizhi_upcoming`

---

## Out of Scope

- Per-project winner-level price analytics (future: once sufficient winner data accumulated)
- Cross-province comparison of mechanism rules (future: policy analysis feature)
- Integration with exchange_excel_metrics trends (separate feature)
- Automated result verification (human review via `verified` flag is sufficient for now)

---

## Files Changed

| File | Change |
|---|---|
| `services/knowledge_pool/jizhi_extractor.py` | New — AI extraction + DB persistence |
| `services/hermes/app.py` | New scan job, Feishu file routing, `/机制竞价` command, `POST /hermes/jizhi/scan` |
| `apps/spot-market/app.py` | New 机制竞价 tab (3 sub-tabs) |

No new dependencies. Tables created at runtime via `ensure_tables()`.
