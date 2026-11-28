# Feishu → shared knowledge base

Browser automation on the Mac exports picked Feishu documents through Feishu's own
**Download As / Download** menu; a one-off ECS task ingests them into the shared
knowledge base (`staging.spot_knowledge_docs`, `app='shared'`,
`source_name='feishu:<folder path>'`).

Respects Feishu permissions: documents whose owner disabled download, or that the
account cannot view, are reported and skipped — never scraped.

## Setup (once)

`~/feishu-ingest/config.json` (local, not in the repo):

```json
{"base": "https://<tenant>.feishu.cn", "home": "https://<tenant>.feishu.cn/drive/home/"}
```

```bash
python services/feishu_ingest/login.py      # headed window: sign in (QR); saves ~/feishu-ingest/state.json (0600)
```

The session cookie is a browser-session cookie: re-run `login.py` when exports start
landing on the login page.

## Export

```bash
# list a folder tree (UI pages, read-only) -> logs/tree.json [{path,name,kind,url}]
python services/feishu_ingest/list_folder.py <folder-url> 4 "<top-level name>,<...>" logs/tree.json
# export: URLs, picks.txt, or a manifest; files land in ~/feishu-ingest/downloads/<date>/<feishu path>/
FEISHU_SETTLE_MS=10000 FEISHU_MENU_MS=3000 python services/feishu_ingest/export.py --manifest logs/tree.json
```

Results append to `~/feishu-ingest/logs/export_results.jsonl`. On a slow connection keep
the longer settle/menu waits — with the defaults, slow pages look "download disabled"
(2026-11-26: five such false negatives exported fine on recheck).

## Ingest (AWS)

Upload files to `s3://<bucket>/tmp/<batch>/NNN/<file>` plus `_manifest.json`
(`{s3 key: feishu path}`), `worker/ingest_one.py` as `_one.py`, `worker/ingest_batch.py`
as `_run.py`; then run-task on the spot-markets task definition (2 vCPU / 8 GB override)
with `python /tmp/run.py tmp/<batch>/`. Each file ingests in its own child process (an
OOM loses one file, not the batch); embedding and synthesis run synchronously; the S3
copy is deleted after success.

## Weekly re-sync (launchd `ai.pjh-etrm.feishu-weekly`, Sundays 01:30)

`bash services/feishu_ingest/setup_weekly_launchd.sh` registers it (`remove` to unregister); log:
`~/feishu-ingest/logs/weekly_sync.log`; manual run: `python services/feishu_ingest/weekly_sync.py [--dry-run]`.

1. Session check on both tenants (Envision + the Tradingthink cross-tenant share). Expired -> Mac
   notification, stop; re-run `login.py` / `login_tenant.py <tradingthink file url>`.
2. Re-list 内部-各省专题研究, both 外部 folders, 对内-项目测算情况 and 官方-各省披露文件 (with modified times; 4–5 h for 官方).
   A folder that comes back empty but had files last time = failed page load -> re-list once, else stop
   without changing state.
3. Select URLs never seen before (`logs/known_urls.json`) plus documents modified after their last export.
   官方 keeps the subset rule (政策/规则 folders + quarterly/half-year/annual reports, this and last year).
4. Export (3 workers above 60 items), stage, ingest. The pre-step replaces an edited document's previous KB
   entry (same file name + `feishu:<path>`); failed files are re-run once.
