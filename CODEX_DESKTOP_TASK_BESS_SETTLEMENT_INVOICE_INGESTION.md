# Codex Desktop Task: Automated BESS Settlement Invoice Ingestion

## Purpose

This file is a direct handoff brief for local Codex Desktop running on Windows.

Use it when Codex Desktop is the active implementation engine for the `invest-etrm-intel` / `bess-platform` repo and AWS OpenClaw cannot directly invoke the local Codex runtime.

Read this together with:
- `CODEX_AGENT.md`
- `PLATFORM_HANDOFF.md`
- `docs/agents/codex/investment_trading_asset_intelligence_sop v2.md`

Follow repo governance and keep changes narrow, additive, production-conscious, and reviewable.

---

## Operating constraints

- additive changes only
- production-ready
- no broad rewrite
- preserve current repo patterns
- preserve `marketdata` compatibility
- persist raw file metadata and parse lineage
- idempotent ingestion
- explicit parse status and error handling
- one active write branch for Codex only
- do not overlap active writes with OpenClaw or other tools

Recommended branch name:
- `feature/bess-settlement-invoice-ingestion`
  or
- `fix/bess-settlement-invoice-ingestion`

If a more specific repo convention is discovered during inspection, follow that convention.

---

## Business objective

Raw BESS settlement PDFs are saved under:

`C:\Users\dipeng.chen\OneDrive\ETRM\bess-platform\data\raw\settlement\invoices`

Implement an automated ingestion path that:
1. reads settlement PDFs
2. extracts them into a standardized structured format
3. saves them into `marketdata`
4. aligns normalized output as closely as practical to the `Trades` tab structure in:
   - `bess-platform\data\raw\settlement\Trade Capture 20260112.xlsx`
5. runs continuously / automatically whenever new files appear in the folder

---

## Required delivery sequence

### Phase 1 — inspect and report first

Before coding, inspect the repo and produce a concise implementation report covering:

1. existing relevant files/modules
   - settlement PDFs
   - invoice parsing
   - uploader
   - marketdata writes
   - staging schemas
   - any settlement / compensation / trade capture logic already present

2. recommended schema

3. recommended parser architecture

4. exact files to add/modify

Do this inspection before implementation.

### Phase 2 — implement narrow diffs

After inspection, implement the solution with narrow, additive diffs only.

---

## Required work packages

## A. Source analysis

Inspect existing repo code related to:
- settlement PDFs
- invoice parsing
- uploader
- marketdata writes
- staging schemas
- settlement logic already present
- compensation logic already present
- trade capture logic already present

Infer the target normalized schema from the workbook’s `Trades` tab:
- `bess-platform\data\raw\settlement\Trade Capture 20260112.xlsx`

Document the inferred column mapping and any unavoidable ambiguities.

---

## B. Standard schema design

Create a normalized ingestion pipeline with at least these layers.

### 1) Raw file registry table

Required fields:
- `file_path`
- `file_name`
- `file_hash`
- `file_modified_time`
- `discovered_at`
- `parse_status`
- `parse_error`
- `parser_version`
- `source_type = settlement_invoice_pdf`

Preferred additions if aligned with repo patterns:
- surrogate primary key
- ingestion timestamps
- retry count
- first_seen / last_seen
- file_size_bytes
- source system / source folder

### 2) Extracted line-item staging table

Minimum fields:
- `invoice_id`
- `asset` / `plant`
- `trade_date`
- `settlement_date` if available
- `product` / `charge_type`
- `charge`
- `quantity`
- `price`
- `amount`
- `unit`
- `counterparty` if available
- `currency`
- `raw_text_span`
- `source_page`
- `confidence` if helpful

Also preserve linkage back to the raw file registry row.

### 3) Normalized trade-like table or view

Align to the `Trades` tab semantics as closely as practical.

Requirements:
- standardized columns matching trade capture semantics where possible
- include `source_file_id`
- include parse lineage / source lineage columns
- do not lose invoice-specific information
- preserve extras in `jsonb` or equivalent if the repo pattern supports it

---

## C. Parsing implementation

Parser requirements:
- choose a robust PDF parser strategy already available in the environment or aligned with repo patterns
- prefer text extraction first
- use table extraction when required
- build parser rules specific to known invoice formats if necessary
- keep the parser modular so new vendor/template formats can be added later
- add unit-testable extraction helpers
- do not rely on fragile one-off regex only
- use layered extraction logic

Preferred parsing strategy:
1. file classification / template detection
2. page text extraction
3. structural section detection
4. line-item extraction
5. normalization into staging rows
6. reconciliation / validation checks
7. write parse status and lineage

Persist enough source evidence for debugging:
- source page number
- raw span / raw row text
- parser version
- template identifier if inferred

---

## D. Ingestion workflow

Implement a watcher / ongoing process for:

`C:\Users\dipeng.chen\OneDrive\ETRM\bess-platform\data\raw\settlement\invoices`

Expected behavior:
- detect new or changed files
- hash file
- skip already-processed identical files
- parse
- write raw registry + staging + normalized outputs
- mark success / failure
- produce logs

Workflow requirements:
- idempotent on rerun
- safe if the same file is observed repeatedly
- safe if parsing fails midway
- failed files clearly visible in DB
- support reprocess by file or date range

---

## E. Runtime mode

Support both:

### 1) one-shot backfill
Use for initial historical load and manual reruns.

### 2) daemon / watch mode
Use for ongoing ingestion when new files appear.

Suggested interfaces:
- Python script / CLI entrypoint
- Windows-friendly watcher mode
- optional future ECS-compatible mode if local folder watching is later replaced

---

## F. Required deliverables

Codex should produce:
- DB DDL / migrations
- parser module(s)
- ingestion runner
- watcher script
- sample config / env variables
- README runbook
- backfill command
- watch command
- troubleshooting notes
- reprocess command for file/date range if practical within current repo patterns

---

## G. Operational requirement

The implementation must be safe for daily operation by non-developers.

Required properties:
- simple command to start watcher
- log location clearly documented
- failed files visible in DB
- rerun does not duplicate rows
- parse status is explicit
- parse error is explicit and queryable

---

## Implementation style guidance

Prefer:
- additive schema additions
- service-layer code over page-layer logic
- reusable parser helpers
- clear separation of raw registry, staging, and normalized outputs
- small, reviewable commits if working locally

Avoid:
- broad schema redesign outside this scope
- invasive refactors unrelated to invoice ingestion
- baking logic only into UI/app pages
- assuming deployment/runtime details not already established in repo

---

## Expected final report from Codex

At completion, provide:

1. files added
2. files modified
3. schema summary
4. parser strategy summary
5. watch/backfill commands
6. test/check plan
7. assumptions
8. runtime risks / operational caveats
9. any items still requiring AWS or DB environment validation

---

## Suggested execution prompt for local Codex Desktop

Use this prompt in Codex Desktop if needed:

> Read `CODEX_AGENT.md`, `PLATFORM_HANDOFF.md`, and `CODEX_DESKTOP_TASK_BESS_SETTLEMENT_INVOICE_INGESTION.md`. First inspect the repo and report: (1) existing relevant files/modules, (2) recommended schema, (3) recommended parser architecture, and (4) exact files to add/modify. Then implement a narrow, additive, production-ready solution for automated BESS settlement invoice PDF ingestion into marketdata, aligned as closely as practical to the `Trades` tab in `data/raw/settlement/Trade Capture 20260112.xlsx`, with raw file registry, staging, normalized outputs, idempotent ingestion, explicit parse status/error handling, backfill mode, and watch mode. Keep diffs narrow, preserve repo patterns, and finish with a concise implementation report.
