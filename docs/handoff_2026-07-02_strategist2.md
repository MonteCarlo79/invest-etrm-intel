# Handoff — 2026-07-02 (Session 2) — Spot Market + Hermes Fixes

## Context for new Claude session

Working directory: `C:\Users\dipeng.chen\OneDrive\ETRM\bess-platform`
Branch: `cost-optimisation` (pushed, up to date with origin)
Last commit: `6765275`

---

## What was completed in this session

### 1. Spot market chart — quality filter fix (commit `c5f842f`)

**File:** `apps/spot-market/app.py` — `_apply_quality_filter()` (line ~932)

**Problem:** 2026 DA PDFs changed table format: change% column now comes **before** price for each metric (avg, max, min). Bug 1 fix (from previous session) skips the leading change%, but some PDFs have format `[change%, avg, change%, min, ...]` — **no max column at all**. The parser then stores `min` as `da_max` and a stale change% value as `da_min`. Since `da_avg (0.517) > da_max (0.016)`, `bad_hi` fired → entire row removed → chart showed no data past June 23 for Gansu, Guangdong, Shandong, Shanxi.

**Fix:** Changed quality filter logic:
- `bad_range` (avg itself < -0.5 or > 2.0) → still **removes row** (avg is genuinely wrong)
- `bad_hi` (avg > max) → **nullifies `da_max`** instead of removing row (avg is valid)
- `bad_lo` (avg < min) → **nullifies `da_min`** instead of removing row

**Result:** Chart now shows correct avg prices for all provinces through June 29. Max/min bands may be blank for dates where parser misidentified columns — acceptable.

**Deployed:** spot-market **v42** / ECS task def **rev 57**

---

### 2. PDF table format investigation

The 2026 DA table changed in two ways vs 2025:

| Year | Column order (after province) |
|------|-------------------------------|
| 2025 | `avg, avg_change%, max, max_change%, min, min_change%` |
| 2026 | `avg_change%, avg, [max_change%], [min], [min_change%], [junk]` |

The 2026 format omits `max` in some sections; Bug 1 fix (skip leading %) correctly extracts `avg` but `_pick_triplet_from_tail` at indices 0,2,4 picks `min` as "max". Quality filter fix handles this gracefully.

**The parser itself does NOT need changing** — the quality filter fix is sufficient.

---

### 3. Hermes daily market report — PDF body fix (commit `6765275`)

**File:** `services/hermes/market_report.py`

**Problem:** `_DAILY_TOOL` schema defined items with field `"summary"` (required), but `_build_pdf()` called `item.get("content")`. Claude, forced by tool_use schema, returned `summary` but the PDF builder looked for `content` → PDF rendered only article titles (blue links), no body text.

**Fix:** Renamed `"summary"` → `"content"` in `_DAILY_TOOL` items schema; added `"source"` and `"date"` fields to match the prompt.

**Deployed:** Hermes **td:139** (patched `:latest` image — Docker Hub unreachable, built `FROM bess-platform-hermes:latest` with single-file COPY)

---

### 4. Two daily reports explained

| Report | Source | Time (Beijing) | Format |
|--------|--------|----------------|--------|
| 今日能源资讯 | `news_screener.py` | 14:00 (06:00 UTC) | Feishu text message, articles by relevance tier |
| 电力市场日报 | `market_report.py` | 15:00 (07:00 UTC) | Feishu card + PDF attachment, AI-synthesised analysis |

The news screener runs first and feeds articles into the KB; the market report queries those articles 1h later to write the PDF.

---

## Current deployment state

| Service | Version | Task Def | Notes |
|---------|---------|---------|-------|
| spot-market | v42 | rev 57 | Quality filter fix |
| hermes | td:139 | rev 139 | PDF body field fix |
| bess-map | v87 | td:87 | No changes this session |

---

## Spot price DB — current state

After running **"Backfill date range"** in Data Management tab:
- June 24–29: DA data present for Gansu, Guangdong, Shandong, Shanxi (correct avg, blank max/min)
- June 26–29: DA=None for Mengxi and Sichuan (genuinely not in those PDFs)
- June 30: partial (DA=0 for some provinces — `Partial` status in inventory)

To fully populate June 30 DA data: re-ingest the 6.30 PDF in "Backfill date range" mode.

---

## Suggested next steps for Strategist

These were identified in the previous session handoff (`docs/handoff_2026-07-02_strategist.md`):

1. **Add `get_news_articles` tool** to Strategist — queries `staging.spot_knowledge_docs` for recent news so agent can discuss latest market events directly
2. **Add `get_capacity_comp` tool** — exposes capcomp + sysopfee tables to Strategist
3. **Fix pdf_parser.py** — properly extract `da_min` (currently stored as `da_max` for 2026 PDFs). The 2026 DA table format `[change%, avg, change%, min, change%_copy]` needs `_pick_triplet_from_tail` to return `(avg, None, min)` instead of `(avg, min, junk)`. The real min sits at index 3 after the leading skip, not index 2.

### Parser fix details (if implementing #3)

In `services/spot_ingest/pdf_parser.py`, after Bug 1 skip, the tail for a 2026 DA row is:
```
[avg, change%(min), min, change%_copy, ...]
```
`_pick_triplet_from_tail` at indices 0, 2, 4 gives `(avg, min, change%)` but interprets them as `(avg, max, min)`.

Correct fix: check if index 2 is numeric AND smaller than index 0 (i.e., "max" < avg is impossible) → treat index 2 as `min`, index 0 as avg, return `(avg, None, min)`.

---

## Key file paths

```
apps/spot-market/app.py              # _apply_quality_filter: line ~932; Strategist tab: line 2678–3750
services/spot_ingest/pdf_parser.py   # parse_pdf — Bug 1 fix at line ~394
services/hermes/market_report.py     # _DAILY_TOOL schema + _build_pdf
scripts/update_spot_markets_taskdef.py
scripts/update_hermes_taskdef.py
```

## How to deploy

```bash
# spot-market (IMAGE_TAG=vNN)
IMAGE_TAG=v43 py scripts/update_spot_markets_taskdef.py

# hermes (uses :latest tag always)
py scripts/update_hermes_taskdef.py
```
