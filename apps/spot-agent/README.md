# spot-agent - China Spot Power Market PDF Ingestion

End-to-end pipeline for ingesting China daily spot power PDF reports into the `marketdata` PostgreSQL database.

## What is supported

| Feature | Status |
|---------|--------|
| Daily DA/RT price summaries (avg/max/min per province) | Implemented |
| Parse status tracking (`spot_parse_log`) | Implemented |
| Source file lineage (`source_file` column) | Implemented |
| Idempotent upserts (sha256 skip) | Implemented |
| One-shot CLI | Implemented |
| Watch / polling mode | Implemented |
| Optional S3-backed source mode | Implemented |
| LLM narrative highlights (optional) | Implemented |
| Hourly price series | Not supported |
| Chart digitization | Not supported |

## Supported location

The supported implementation lives at:

```text
bess-platform/apps/spot-agent/
```

`agent/agent_run.py` is deprecated. The supported entrypoint is `agent/spot_ingest.py`.

## Source modes

`spot_ingest.py` now supports two source modes using the same parser and DB path:

- `source_mode: local`
  - Preserves the existing Windows / OneDrive glob behavior.
  - Reads directly from configured `pdf_globs`.
- `source_mode: s3`
  - Lists PDFs from configured S3 prefixes.
  - Downloads each candidate PDF to temp local disk.
  - Feeds the staged file through the existing parser / DB pipeline.
  - Uses the stable `s3://bucket/key` value in `spot_parse_log.pdf_path` for idempotent skip checks.

## Directory structure

```text
apps/spot-agent/
©À©¤©¤ requirements.txt
©À©¤©¤ agent/
©¦   ©À©¤©¤ spot_ingest.py        <- supported entrypoint
©¦   ©À©¤©¤ tools_pdf.py          <- PDF table parser
©¦   ©À©¤©¤ tools_db.py           <- DB init + upsert + parse log
©¦   ©À©¤©¤ tools_s3.py           <- optional S3 list/download helper
©¦   ©À©¤©¤ tools_llm.py          <- optional LLM highlights
©¦   ©¸©¤©¤ spot_header_bess.yaml <- config
©À©¤©¤ api/
©¦   ©¸©¤©¤ main.py               <- FastAPI: /v1/spot/daily, /v1/spot/parse-log
©À©¤©¤ ui/
©¦   ©¸©¤©¤ spot_dashboard.py     <- Streamlit dashboard at /spot-markets
©¸©¤©¤ ops/
    ©¸©¤©¤ schedule.ps1          <- Windows scheduled one-shot / watch wrapper
```

## Configuration

### Local mode

```yaml
year: 2025
source_mode: local
pdf_globs:
  - "C:/Users/dipeng.chen/OneDrive/ETRM/bess-platform/data/spot reports/2026/*.pdf"
```

### S3 mode

```yaml
year: 2025
source_mode: s3
s3_bucket: your-bucket
s3_region: ap-southeast-1
s3_prefixes:
  - spot-reports/2026/
  - spot-reports/2025/
```

## Local setup

```powershell
cd apps\spot-agent
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

The AWS-safe S3 source path also requires `boto3` to be available in the ingestion runtime.
This code path is optional and does not affect local mode.

## Running the ingestion

### Local backfill

```powershell
cd apps\spot-agent\agent
python spot_ingest.py --header spot_header_bess.yaml --no-llm
python spot_ingest.py --header spot_header_bess.yaml --since 2026-01-01 --no-llm
python spot_ingest.py --header spot_header_bess.yaml --force --no-llm
```

### Local watch / polling

```powershell
cd apps\spot-agent\agent
python spot_ingest.py --header spot_header_bess.yaml --watch --interval 300 --no-llm
```

### S3 backfill

```powershell
cd apps\spot-agent\agent
python spot_ingest.py --header spot_header_bess.yaml --no-llm
```

Use the S3 config shown above before running.

### S3 watch / polling

```powershell
cd apps\spot-agent\agent
python spot_ingest.py --header spot_header_bess.yaml --watch --interval 300 --no-llm
```

## Scheduling guidance

- Local / desktop: `ops/schedule.ps1` remains valid for one-shot scheduled runs.
- AWS: prefer a scheduled ECS one-shot task over a permanently running ECS watcher.
  - This fits the repo's current ECS/EventBridge/RDS pattern.
  - It reuses the existing sha256 + `spot_parse_log` idempotent skip logic.
  - It avoids coupling ingestion uptime to the deployed Streamlit service.

## API / dashboard behavior

The deployed dashboard and API remain daily-only:

- `/v1/spot/daily` reads `spot_daily`
- `/v1/spot/parse-log` reads `spot_parse_log`
- `/v1/spot/hourly` remains HTTP 501

No hourly behavior is re-enabled by this change.

## Database objects

| Table | Purpose |
|-------|---------|
| `spot_daily` | Daily DA/RT summaries per province per date |
| `spot_parse_log` | Per-file ingestion audit trail |
| `spot_hourly` | Reserved for future hourly data only |

## Validation queries

```sql
SELECT status, COUNT(*)
FROM spot_parse_log
GROUP BY status
ORDER BY status;
```

```sql
SELECT pdf_path, status, started_at, finished_at
FROM spot_parse_log
ORDER BY started_at DESC
LIMIT 20;
```

```sql
SELECT province_en, MIN(report_date), MAX(report_date), COUNT(*)
FROM spot_daily
GROUP BY province_en
ORDER BY province_en;
```

For S3 mode only:

```sql
SELECT pdf_path, status, started_at
FROM spot_parse_log
WHERE pdf_path LIKE 's3://%'
ORDER BY started_at DESC
LIMIT 20;
```

## Field mapping assumptions

- `spot_daily.source_file` continues to store only the PDF basename.
- `spot_parse_log.pdf_path` stores the stable source locator:
  - local absolute path in local mode
  - `s3://bucket/key` in S3 mode
- Daily DA/RT summaries are the only populated data path.
- Hourly remains intentionally unsupported.

## Known limitations

1. Hourly data is not supported. The source PDFs store hourly curves as chart images rather than tables.
2. Province matching is exact against the YAML mapping.
3. LLM highlights require `OPENAI_API_KEY`. Use `--no-llm` to skip.
4. Year inference still relies on folder path or S3 key naming when page headers are incomplete.
5. The current database appears to contain `spot_daily` rows through `2026-12-31`, while the visible local PDF corpus appears to end in March 2026. This change does not alter date inference logic. Treat that mismatch as a validation item to review separately.
