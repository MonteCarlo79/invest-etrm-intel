# Hermes: Recognise 电力现货市场价格与运行月报 PDFs — Design

**Date:** 2026-08-10
**Status:** Approved (design), pending implementation plan
**Scope:** Hermes file router (Feishu + Telegram), `services/spot_ingest/`, new DB tables

---

## 1. Problem

Files named `电力现货市场价格与运行月报（YYYY年M月）.pdf` (national spot market monthly report, ~58 pages, 25 continuous-running provinces) sent to Hermes via Feishu are not recognised:

- They do **not** match `is_spot_pdf` (`services/hermes/spot_ingest_bridge.py:50`) — its patterns only cover the **日报** (daily) report.
- They **do** match `is_exchange_report` (`services/exchange_reports/ingestor.py:909`): the `月报` keyword matches and `（2026年6月）` infers a month. The file is therefore misrouted into the exchange monthly reports pipeline (`staging.exchange_monthly_reports`) with Claude guessing a province — wrong table, wrong schema.

## 2. Report structure (verified against sample `电力现货市场价格与运行月报（2026年6月）.pdf`, 58 pages)

| Pages | Content | Extraction |
|-------|---------|------------|
| 1–2 | 总体情况 — national RT/DA total cleared volume, avg price, 中长期合约覆盖电量 | Parse → national table |
| 2–4 | 表1 省间现货市场运行情况 (seller/buyer provinces, volume, price, MoM) | **Skip** — derivable from daily interprov data already ingested by the 日报 pipeline |
| 5 | Narrative: per-province RT vs 中长期 price deviation | Not parsed (covered by province table metrics) |
| 6–7+ | 表2 连续运行地区运行情况一览表 — per-province: 中长期成交电量/均价, 合约覆盖电量占比, 实时成交电量/均价/环比, 日前(预出清)成交电量/均价/环比, 运行情况 | Parse → province table |
| 8–58 | Per-province charts (daily curves, hourly curves, price distributions) — images, minimal text | **Skip** — daily data already flows via the 日报 pipeline |

Text extraction via pdfplumber verified working. Table 2 has merged/wrapped cells and row-spanning 运行情况 values — deterministic table parsing would be fragile, so structuring is delegated to Claude.

## 3. Decisions (locked)

- **DB scope:** per-province monthly table + national aggregate row. Interprov table (表1) not parsed.
- **Backfill:** yes — CLI script, batch over a folder.
- **Code location:** new module in `services/spot_ingest/` (Option A). Hermes `app.py` stays a thin router.
- **Parsing:** pdfplumber text extraction (pages 1–10) → Claude (sonnet-4-6) structures strict JSON → validation → upsert. Same pattern as capacity ETL's `_capacity_from_file`. Monthly cadence → negligible cost.
- **Yearless filenames:** skip and ask for rename — never stamp the current year (same rule as settlement pipeline, commit 1064925).

## 4. Components

### 4.1 `services/spot_ingest/provinces.py` (new, tiny)

`PROVINCES_MAP` (province_cn → province_en) moved here. `services/hermes/spot_ingest_bridge.py` imports from it (removes one of two existing duplicate copies). `apps/spot-watcher/pipeline.py` copy left untouched (out of scope).

### 4.2 `services/spot_ingest/monthly_report.py` (new)

```python
SPOT_MONTHLY_PATTERNS = ["电力现货市场价格与运行月报"]

def is_spot_monthly_pdf(filename: str) -> bool
    # pattern match + .pdf suffix

def infer_report_month(filename: str) -> Optional[date]
    # r"(\d{4})年(\d{1,2})月" → date(y, m, 1); None if absent

def parse_monthly_report(pdf_bytes: bytes, api_key: str) -> dict
    # pdfplumber: text of pages 1–10
    # Claude sonnet-4-6 → strict JSON {national: {...}, provinces: [...]}
    # validate() → raises on hard failure, warns on soft anomalies
    # returns {"national": {...}, "provinces": [...], "warnings": [...]}

def ingest_monthly_report(filename: str, pdf_bytes: bytes, api_key: str) -> dict
    # infer month → parse → upsert both tables (single transaction)
    # returns summary {"month", "n_provinces", "national_rt_avg", "warnings"}
```

**Validation rules (no DB write on failure):**
- ≥20 provinces expected → soft warning if fewer; hard fail if 0
- prices within 0–2.0 元/kWh; MoM within ±1000%; volumes 0–10000 亿kWh
- `/` in source table → NULL
- province_cn must resolve via `PROVINCES_MAP`; unknown province → row dropped + warning

### 4.3 DB tables (new file `db/ddl/public/spot_monthly.sql` — schema-named DDL dirs; `spot_daily` itself predates the DDL dir)

`public.spot_monthly_province`:

| Column | Type | Notes |
|--------|------|-------|
| report_month | DATE | first of month, part of PK |
| province_en | TEXT | part of PK |
| province_cn | TEXT | |
| run_status | TEXT | 运行情况: 正式运行 / 试运行 etc. |
| mlt_volume_yi_kwh | NUMERIC | 中长期成交电量, 亿kWh |
| mlt_avg_price | NUMERIC | 元/kWh (= yuan/kWh, same as spot_daily) |
| mlt_coverage_pct | NUMERIC | 中长期合约覆盖电量占比, % |
| rt_volume_yi_kwh | NUMERIC | 实时市场成交电量, 亿kWh |
| rt_avg_price | NUMERIC | 元/kWh |
| rt_mom_pct | NUMERIC | 实时环比涨幅, % |
| da_volume_yi_kwh | NUMERIC | 日前(预出清)成交电量, 亿kWh |
| da_avg_price | NUMERIC | 元/kWh |
| da_mom_pct | NUMERIC | 日前环比涨幅, % |
| source_file | TEXT | original filename |
| ingested_at | TIMESTAMPTZ | default now() |

PK `(report_month, province_en)`.

`public.spot_monthly_national`: PK `report_month`; `rt_total_volume_yi_kwh, rt_avg_price, da_total_volume_yi_kwh, da_avg_price, mlt_coverage_volume_yi_kwh, mlt_coverage_pct, mlt_avg_price, source_file, ingested_at`.

**Units:** prices in 元/kWh (= yuan/kWh, matches `spot_daily`); volumes in 亿kWh exactly as printed (explicit `_yi_kwh` suffix); MoM as percent number (`4.82` = 4.82%).

**Upsert:** `ON CONFLICT ... DO UPDATE SET col = COALESCE(EXCLUDED.col, table.col)` — same convention as `services/spot_ingest/db_upsert.py`. Re-sending a corrected PDF is safe.

### 4.4 Hermes wiring (`services/hermes/app.py`)

- Import `is_spot_monthly_pdf, infer_report_month, ingest_monthly_report` from `services.spot_ingest.monthly_report`.
- New branch **before** `is_exchange_report` in **both** handlers: Feishu (~line 2536) and Telegram twin (~line 2741). This ends the misrouting into `staging.exchange_monthly_reports`.
- Flow on match:
  1. Month not inferable → reply `文件名需包含年份和月份，如（2026年6月）`; stop (OneDrive save + route card already happened upstream — unchanged).
  2. KB ingest via `agent.ingest_file_to_kb(filename, file_bytes)` (auto-categorisation) → KB confirmation reply.
  3. `ingest_monthly_report(...)` → success reply, e.g. `📊 现货月报已入库（2026-06）：25省 + 全国汇总，全国实时均价 0.291 元/kWh`. Parse/validation failure → warning reply, no partial DB writes (single transaction), KB copy already safe.

### 4.5 Backfill CLI

```
python -m services.spot_ingest.run_monthly_ingest --dir <folder>
```

Iterates PDFs matching the pattern in a folder, runs the same parse+upsert (DB only — KB backfill already covered by `scripts/ingest_knowledge_bulk.py`).

## 5. Error handling

| Case | Behaviour |
|------|-----------|
| Yearless filename | Reply asking to rename; no DB write |
| pdfplumber failure | Warning reply; KB ingest still proceeds; no DB write |
| Claude extraction / validation failure | Warning reply with reason; no partial writes (transaction rollback) |
| Duplicate send | Upsert — idempotent |
| Unknown province in table | Drop that row, keep others, include in warnings |

## 6. Testing

- Unit tests: recognizer (incl. 日报/exchange-月报 negative cases), `infer_report_month` (valid, yearless, quarterly), JSON validation rules, upsert SQL (mocked conn). Claude call mocked with a saved page-text fixture.
- Local end-to-end before any deploy: run backfill CLI on the sample PDF against dev DB; verify row counts + spot-check 山东/吉林/四川 values against the PDF.
- Check `staging.exchange_monthly_reports` for a previously misrouted row from this file type; report if found (removal is a separate confirmation).

## 7. Deployment

- Hermes image rebuild + redeploy (`bess-hermes`) after local verification — **explicit in-session confirmation required** per deploy protocol.
- DDL applied to RDS first (additive only — two new tables, no changes to existing).

## 8. Out of scope (flagged, not built)

- Strategist agent tool to query the new tables
- Surfacing monthly data in `apps/spot-market`
- 表1 interprov monthly parsing
- Chart digitisation (pages 8–58)
