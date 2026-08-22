# GB REMIT Daily Ingestion — Design

**Date:** 2026-08-20 · **Status:** approved by user · **Branch:** feat/deal-structurer-bedrock-migration

## Context

The GB Market app (bess-gb-market, currently v101/td:20) has no REMIT tracking. For GB BESS
economics, generation unavailability is a primary system-tightness signal: unplanned CCGT /
nuclear / interconnector outages drive imbalance price spikes, which drive BESS revenue.

**Goal:** ingest Elexon REMIT messages daily into a structured table, surface them in a new
GB app tab, and distil a nightly digest so the Strategist agent can answer outage questions
from data.

User decisions (2026-08-20): table + tab + agent digest; all generation types.

## Source

Elexon Insights API — same public API already used by `services/gb_knowledge/elexon_ops.py`
(settlement prices, wind forecast) with no auth:

```
GET https://data.elexon.co.uk/bmrs/api/v1/remit/list?from=<iso>&to=<iso>
```

Notes:
- The API is unreachable from the user's Mac (IP-level 403 on ALL endpoints, including the
  known-good settlement-prices endpoint that works from ECS daily). Field names below follow
  the Elexon documentation shape but are **unverified against live responses**. The connector
  therefore maps fields tolerantly (multiple candidate keys) and logs one raw message on its
  first successful run; a one-commit follow-up fixes any mapping drift, verified via CloudWatch.
- REMIT messages are revised over their lifetime (outage extended/shortened/cancelled).
  Nightly pull window = **last 48h** so revisions are captured; upsert by `message_id`.

## Storage

New table `intl_market.gb_remit_messages` (created idempotently by the job):

| Column | Type | Notes |
|---|---|---|
| `message_id` | TEXT PK | REMIT mRID / message id |
| `published_at` | TIMESTAMPTZ | publication time of this revision |
| `event_start` | TIMESTAMPTZ | outage start (nullable) |
| `event_end` | TIMESTAMPTZ | outage end (nullable — open-ended outages) |
| `asset_name` | TEXT | station / unit name |
| `fuel_type` | TEXT | normalised: CCGT/OCGT/Nuclear/Coal/Wind/Interconnector/Storage/Other |
| `affected_mw` | NUMERIC | unavailable capacity (nullable) |
| `outage_type` | TEXT | `planned` / `unplanned` / `unknown` |
| `cause` | TEXT | free-text cause |
| `raw` | JSONB | full message payload |

Index on `(event_start, event_end)` for the "active now / next 7 days" queries.

## Ingestion job

New module `services/gb_knowledge/elexon_remit.py`, following the `elexon_ops.py` pattern
(requests session, 3× retry with backoff, ops logging). Functions:

- `ensure_table(conn)` — CREATE TABLE/INDEX IF NOT EXISTS
- `fetch_messages(session, from_dt, to_dt) -> list[dict]`
- `upsert_messages(conn, rows) -> int`
- `run(conn, days_back=2) -> int` — fetch + upsert, returns row count
- `build_digest(conn, today) -> str` — markdown digest of significant messages
  (affected_mw > 300 OR unplanned), written as one KB doc `remit://YYYY-MM-DD`
  via the existing `upsert_doc` KB path (ON CONFLICT DO NOTHING per day)

Scheduler: `_remit_job` in `apps/gb-market/scheduler_service.py` at **03:05 SGT**
(after the 03:00 market job). Wraps `run()` in try/except and logs rows ingested;
failures log and skip (no partial state — upsert transaction per batch).

## UI — new "REMIT" tab (apps/gb-market/app.py)

1. **Off-line now** — messages whose event window covers now: asset, fuel, affected MW,
   until when; summary tile row: total MW unavailable by fuel type.
2. **Next 7 days** — stacked area/bar of daily unavailable MW by fuel type (forward
   tightness curve).
3. **Latest messages** — full table, newest revision first; filters: fuel type,
   planned/unplanned.

Data via the app's existing DB connection helper; queries filter on event windows vs `now()`.

## Agent digest

Nightly KB doc (source=`elexon_remit`, doc_type matches existing KB convention) containing:
count of new/updated messages, top outages by MW, any unplanned >300 MW flagged.
The Strategist's existing `search_reference_docs` tool picks it up (FTS over KB).

## Error handling

- API non-200 / timeout → log warning, skip run (ops-log row records failure).
- Unknown/missing fields → tolerant mapping to NULL; raw JSONB always stored.
- Zero messages returned → valid state (quiet day), logs `0 rows`, digest says so.

## Testing / verification

1. **Local unit-level:** mapper function against a hand-written sample message dict
   (validates tolerant key mapping without network).
2. **Local UI render:** tab renders against the RDS table (may be empty pre-deploy —
   must render empty-state cleanly, no crash).
3. **First prod run:** CloudWatch `_remit_job` lines — row count + the one-time raw-sample
   log; verify table rows in RDS; verify tab on production; ask agent an outage question.

## Rollout

Single deploy: image `bess-gb-market:v102`, new task-def revision under family
`bess-gb-market` (NOT the dead `bess-platform-gb-market` family), update service.
Known side effect: task swap wipes `/tmp/modo_session.json` → that night's 20:00 SGT
Modo job does one fresh login (one email), self-heals next day.

## Out of scope

- Alerts/notifications on new large outages
- Gas-market REMIT; interconnector nomination data
- Backfill beyond the 48h window (available on demand via `run(conn, days_back=N)`)
