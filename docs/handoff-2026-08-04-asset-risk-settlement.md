# Handoff: Asset Risk — Settlement Ingestion Complete

**Date:** 2026-08-04  
**Branch:** `feat/deal-structurer-bedrock-migration`  
**Repo:** `MonteCarlo79/invest-etrm-intel`

---

## What was done this session

### 1. Settlement PDF Ingestion System (`services/settlement_ingest/`)

Built automated ingestion for BESS monthly settlement invoices from `data/raw/settlement/invoices/`.

| File | Purpose |
|------|---------|
| `scanner.py` | Recursive folder scanner, SHA-256 dedup, filename classification (上/下/清单/农网/发票), month extraction |
| `parser_charge.py` | Regex-based parser for text-extractable charging cost PDFs (电费清单/下网结算单) |
| `parser_discharge.py` | Claude Vision OCR for scanned discharge settlement PDFs (电费结算单/上网结算单) |
| `folder_mapper.py` | Maps B-1..B-13 folders to rm_assets names |
| `watcher.py` | Entry point for scheduled execution: `python -m services.settlement_ingest.watcher` |

### 2. Charging Cost Parser — Pattern Coverage

Handles all Mengxi BESS invoice formats:
- **Standard format** (杭锦旗/苏右/乌拉特): extracts 电能电费, 输配电费, 系统运行费, 功率因数调整, 政府基金, 上网线损 individually
- **退补 format** (四子王旗): all line items are 0, total extracted from 退补电费 section or 电费构成 fallback
- **农网 format** (乌拉特 agricultural grid): parsed correctly
- Zero-amount items filtered out
- `extract_billing_period()`: extracts YYYY-MM from PDF text when filename lacks year

**Tested: 224 charging PDFs, 0 failures.**

### 3. Discharge Parser (Vision OCR)

For scanned image PDFs (电费结算单):
- Extracts embedded JPEG from PDF
- Sends to Claude Vision (`claude-sonnet-4-6` via Bedrock)
- Parses JSON response: 现货→discharge_energy, 非市场化→capacity_compensation, 调频→frequency
- Works on ECS (ap-southeast-1); blocked from China locally

### 4. App Upload Flow Updates

- **Multi-file upload** — drag-and-drop multiple PDFs at once
- **Auto-detect month** — from filename (`2026-01`, `1月份`, `YYYY年M月`) or PDF content (`extract_billing_period`)
- **Overwrite mode** — deletes existing data for same book+month+filename before re-inserting
- **Scanned PDF detection** — auto-routes to Vision parser when `page.chars == 0`
- **Correct parser** — uses `parser_charge.py` (not generic `libs/settlement/parser.py`) for text PDFs

### 5. Settlement Analytics (Chinese UI)

Monthly breakdown table with:
- Chinese category labels (充电电费, 放电收入, 容量补偿/非市场化, etc.)
- **价差收入** = 放电收入 + 容量补偿 + 充电电费
- **度电总价差** = 价差收入 ÷ 放电电量
- **容量补偿价差** = 容量补偿 ÷ 放电电量
- **套利价差** = (放电收入 + 充电电费) ÷ 放电电量
- **YTD 合计** row with correctly recalculated per-MWh metrics
- Stacked bar chart by month
- 分类汇总 table

### 6. PDF Classification Rules

| Pattern | Classification |
|---------|---------------|
| `上网`, `上】`, `上网结算单` | discharge (放电) |
| `下网`, `下】`, `农网`, `电费清单`, `月份电费清单` | charge (充电) |
| `电费结算单` (without 上/下) | discharge |
| `发票` | skip |

### 7. Data Seeded

- 8 BESS assets + linked books in `rm_assets` / `rm_books`
- 5 province CRM configs in `rm_crm_import_configs` (Hunan, Hubei, Zhejiang, Shandong, Jiangsu)
- 34 charging cost settlements ingested to DB (local batch run)
- Discharge settlements: ingested via app upload (Vision OCR on ECS)

---

## Current Deployment

| App | ECR Image | Task Def |
|-----|-----------|----------|
| Asset Risk | `bess-asset-risk:v15` | `bess-platform-asset-risk:17` |
| Retail Risk | `bess-retail-risk:v2` | `bess-platform-retail-risk:2` |
| Portal | `bess-platform-portal:v10` | `bess-platform-portal:65` |

---

## Known Issues / TODO

1. **WAF allowlist** — `/asset-risk/*` and `/retail-risk/*` still need corporate WAF update for `pjh-etrm.ai` access. Currently accessible via ALB direct URL only.

2. **Discharge PDFs locally** — Cannot be processed locally (Bedrock blocked from China). Must upload via app UI (ECS has Bedrock access) or wait for watcher to run on ECS.

3. **B-3 (谷山梁) PDFs** — Some return 0 items. These appear to be a different format (合并结算/统计对账 rather than standard 电费清单). May need a dedicated parser.

4. **B-4/B-5 (查干哈达/四益堂) older PDFs** — Extract ¥0 amounts. These may be placeholder/zero-value invoices or need different extraction patterns.

5. **Folder-to-asset mapping** — `folder_mapper.py` covers B-1 through B-11 + B-【外】. New assets (B-12, B-13) exist in folders but aren't in `rm_assets` yet — need to be registered.

6. **Watcher scheduling** — Not yet set up as Windows Task Scheduler job or ECS scheduled task.

7. **Retail Risk app** — Deployed but not actively developed further this session. CRM import, load profiles, retail settlement features are stubbed.

---

## File Inventory (settlement ingestion)

```
services/settlement_ingest/__init__.py
services/settlement_ingest/scanner.py
services/settlement_ingest/parser_charge.py
services/settlement_ingest/parser_discharge.py
services/settlement_ingest/folder_mapper.py
services/settlement_ingest/watcher.py
services/operating_assets/parsers/load_profiles.py (Shandong/Jiangsu format)
```

---

## Prompt for new session

```
Read these docs:
1. docs/handoff-2026-08-04-asset-risk-settlement.md (this file — latest status)
2. docs/handoff-2026-07-23-risk-management-complete.md (full app architecture)
3. docs/superpowers/specs/2026-07-16-asset-risk-design.md (App 1 spec)

Asset Risk app is deployed (v15) with full settlement PDF ingestion.
Key areas for continuation:

1. Ingest remaining discharge PDFs for all assets (upload via app UI)
2. Fix B-3 谷山梁 parser (different format: 合并结算/统计对账)
3. Register B-12/B-13 assets in rm_assets
4. Set up watcher as ECS scheduled task for automatic ingestion
5. Build out Realised P&L tab (waterfall chart from settlement data)
6. Forward curve integration (LingFeng → rm_forward_curves)
7. Continue Retail Risk app development

Technical notes:
- Charging PDFs: services/settlement_ingest/parser_charge.py (regex on pdfplumber text)
- Discharge PDFs: services/settlement_ingest/parser_discharge.py (Claude Vision on ECS)
- App upload: apps/asset_risk/tab_settlement.py (auto-detects scanned vs text)
- All 224 charging PDFs parse successfully across all BESS assets
- Settlement analytics shows monthly breakdown with 价差收入/套利价差/容量补偿价差
- Uses Bedrock (shared/anthropic_client.make_client), BEDROCK_REGION=ap-southeast-1

Branch: feat/deal-structurer-bedrock-migration
Working directory: C:\Users\dipeng.chen\OneDrive\ETRM\bess-platform
```
