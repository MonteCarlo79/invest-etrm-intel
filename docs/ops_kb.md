# Operating-Assets Knowledge Base (ops KB)

**Purpose:** a DB-backed document store for operating-asset materials — 复盘/backtest/incident/maintenance reports — so the asset-risk app (and later, agents) can search and cite them. Parallel to the spot-market knowledge pool (`staging.spot_knowledge_*`), scoped to operations.

## Tables

- `staging.ops_knowledge_docs` — one row per document (sha256 `file_hash` dedup key, `category`, nullable `asset_id` FK to `rm_assets` inferred from filename, `source_path`, `ingest_status`)
- `staging.ops_knowledge_chunks` — 500-char overlapping chunks per document page, with a GIN FTS index

Canonical DDL: `db/ddl/staging/ops_knowledge.sql` (also auto-created idempotently by `init_ops_knowledge_tables()` at first ingest — the platform's auto-migration pattern).

## Categories

| Key | Matches |
|---|---|
| `operational_review` | 复盘， 运营统计， 半年度/年度总结， 运营分析 |
| `incident_report` | 停机， 故障， 缺陷， 跳闸， 告警 |
| `backtest_report` | 回测， 策略验证， 完美收益， perfect foresight |
| `maintenance_record` | 检修， 维护， 涉网试验， 定检 |
| `dispatch_plan` | 调度计划表， 交易调度 |
| `other` | (Haiku fallback: `claude-haiku-4-5-20251001` when API key available) |

## Ingestion

**Module:** `services/knowledge_pool/ops_knowledge_docs.py` — `register_and_ingest()` (sha256 dedup, extractors reused from `services/knowledge_pool/knowledge_docs.py`: PDF, PPTX incl. chart XML as structured text, DOCX, XLSX/XLS, TXT, HTML, images via Claude vision). Auto-categorization: keyword heuristic → Haiku fallback (two defects from the source module fixed here).

**Watcher:** `services/knowledge_pool/ops_watcher.py` — single-pass scan of `assets/operating/复盘/` (skips `~$` locks and `*_Error.txt` OneDrive stubs; per-file timeout guards hydration stalls; local checkpoint at `~/Library/Logs/bess-ops-kb/checkpoint.log`).

Manual run:
```bash
python -m services.knowledge_pool.ops_watcher            # real ingest
python -m services.knowledge_pool.ops_watcher --dry-run  # list only
```

**Schedule (launchd, hourly):**
```bash
bash scripts/setup_ops_kb_launchd.sh        # registers ai.pjh-etrm.ops-kb-ingest
launchctl kickstart gui/$(id -u)/ai.pjh-etrm.ops-kb-ingest   # run now
```
Logs: `~/Library/Logs/bess-ops-kb/` (local disk — launchd/TCC cannot execute from or reliably write into `~/Library/CloudStorage`; the plist runs with `HOME` set explicitly).

## Search

```python
from services.knowledge_pool.ops_knowledge_docs import search_ops_docs
hits = search_ops_docs("出清校核", category="operational_review", limit=5)
```
CJK bigram ILIKE for Chinese queries, tsvector for Latin. Optional `asset_id` filter (asset-specific docs + multi-asset docs).

## Notes

- Parse failures still register (`ingest_status='failed'`, `parse_error` kept) so they surface instead of vanishing.
- Deleting a file from the folder does NOT remove it from the KB (soft-delete via `active=FALSE` is the only removal path; deliberate — the KB is an archive).
- The waterfall tab's 偏差定义 expander is static text in v1 (extracted from the 复盘 deck); a KB-backed version can later source it via `search_ops_docs("出清校核")`.
