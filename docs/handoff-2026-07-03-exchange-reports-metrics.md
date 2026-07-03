# Handoff: Exchange Monthly Reports — Metrics Backfill

**Date:** 2026-07-03  
**Branch:** `cost-optimisation`  
**Commit:** `5f63d7c`

---

## What Was Done

### 1. 广西 Province Added (`services/exchange_reports/ingestor.py`)
Added 广西 to both province mapping dicts so files in `广西月报/` folder
(and filenames containing "广西") are recognised and ingested.

```python
_FOLDER_TO_PROVINCE["广西月报"] = "广西"
_NAME_TO_PROVINCE["广西"]       = "广西"
```

All 22 广西 reports (2025-01 through 2026-05, monthly + quarterly) are now in
the DB and KB. Supported provinces are now 10:
`上海, 冀南, 安徽, 山东, 广东, 江苏, 浙江, 福建, 蒙西, 广西`

### 2. `--extract-metrics-only` Flag (`scripts/ingest_exchange_reports.py`)
New backfill mode that finds all rows in `staging.exchange_monthly_reports`
with `ingest_status='ingested'` but no matching row in
`staging.exchange_monthly_metrics`, then re-extracts metrics from disk.

```powershell
py scripts/ingest_exchange_reports.py --extract-metrics-only
py scripts/ingest_exchange_reports.py --extract-metrics-only --province 广西
```

### 3. Multi-Provider LLM (`services/exchange_reports/metrics_extractor.py`)
Provider selection — first match wins:

| Env var | Provider | Notes |
|---|---|---|
| `DEEPSEEK_API_KEY` | DeepSeek `deepseek-chat` | OpenAI-compatible; accessible from China |
| `BEDROCK_REGION` | AWS Bedrock Claude | Blocked from China by Anthropic geo-restriction |
| `ANTHROPIC_API_KEY` | Direct Anthropic API | Blocked from China (403 Forbidden) |

Optional overrides: `DEEPSEEK_MODEL`, `BEDROCK_MODEL_ID`

---

## Remaining Work — ONE TASK LEFT

### Run the metrics backfill with DeepSeek

All ~300+ ingested reports (all 2025 data + all 广西 data) have **no metrics rows** 
in `staging.exchange_monthly_metrics` because the Anthropic API key was invalid
(403) during ingest.

**Steps:**
1. Get a DeepSeek API key from https://platform.deepseek.com
2. Run:

```powershell
$env:DEEPSEEK_API_KEY = "sk-..."
py scripts/ingest_exchange_reports.py --extract-metrics-only
```

Expected: ~300 reports extracted, ~10–15 minutes.

After this completes, the exchange reports section of the app (GB Market / Hermes)
will show cross-province metrics tables and highlights for all 2025 + 2026 data.

---

## Key Files

| File | Purpose |
|---|---|
| `services/exchange_reports/ingestor.py` | Province inference, file ETL, KB ingest |
| `services/exchange_reports/metrics_extractor.py` | LLM metrics extraction + DB upsert |
| `services/exchange_reports/summary_pdf.py` | PDF summary table builder |
| `scripts/ingest_exchange_reports.py` | CLI: full ingest + metrics backfill |
| `data/exchange-monthly-reports/` | Local report files (not in git) |

## Key DB Tables

| Table | Purpose |
|---|---|
| `staging.exchange_monthly_reports` | File registry with SHA256 dedup |
| `staging.exchange_monthly_metrics` | Structured metrics (17 numeric fields + highlights) |
| `staging.spot_knowledge_docs` | KB document index |
| `staging.spot_knowledge_chunks` | KB text chunks |

## Data Folder Structure

```
data/exchange-monthly-reports/
  上海月报/       (monthly + subfolders for settlement breakdown)
  冀南月报/
  安徽月报/
  山东月报/
  广东月报/
  江苏月报/
  浙江月报/
  福建月报/
  蒙西月报/
  广西月报/       ← NEW; 22 files ingested 2026-07-03
```

## Legitimately Skipped Files (no metrics needed)
These 9 files are annual/semi-annual reports with no specific month — correctly skipped:
- `上海市2025年上半年/前三季度/年度电力市场交易信息.pdf`
- `2026年上海市电力供需情况预测.pdf`
- `2025年上海电力中长期交易总体情况（完成年度安全校核后）.pdf`
- `广东电力现货市场2025年年报.pdf`
- `广东电力现货市场结算运行情况2026年年报.pdf`
- `2025年市场信息披露报告.pdf` (江苏, no month)
- `2025年江苏电力市场运营情况通报.pdf` (no month)
