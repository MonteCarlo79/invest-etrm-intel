# Handoff — Excel Monthly Reports Ingestion (2026-07-09)

## What was built this session

### Goal
Ingest vendor-curated provincial Excel databases from `data/exchange-monthly-reports/` into the DB and surface structured data in spot-markets + bess-map dashboards.

### New files (committed on `cost-optimisation`, commit `38698c7`)

| File | Purpose |
|---|---|
| `services/exchange_reports/excel_ingestor.py` | Province parsers for 23 provinces; `parse_excel_file()`, `upsert_excel_metrics()`, `excel_to_kb_text()` |
| `scripts/ingest_excel_reports.py` | Walk `data/exchange-monthly-reports/`, parse every `.xlsx`, upsert to DB; run with `PGURL=... python scripts/ingest_excel_reports.py` |
| `apps/spot-market/app.py` | Added 4th radio tab "🗺️ 省级Excel数据" in 交易所月报管理 expander |

### DB tables populated

| Table | Rows | Notes |
|---|---|---|
| `staging.exchange_excel_metrics` | 450 rows | 23 provinces, Jan 2025–May 2026 |
| `marketdata.province_installed_monthly` | +193 upserted | 22 provinces now have `bess_mw` (was ~4) |

### Deployed versions
- **v51** (td:69) — initial Excel data tab
- **v52** (td:70, current) — fix 云南 price unit bug + replace bar chart with plotly

---

## Schema: staging.exchange_excel_metrics

Key columns (all nullable except province + report_month):

```
province, report_month          -- UNIQUE key
source_file

-- Capacity (MW)
total_capacity_mw, thermal_capacity_mw, hydro_capacity_mw,
nuclear_capacity_mw, wind_capacity_mw, solar_capacity_mw,
bess_capacity_mw, other_capacity_mw

-- Generation (GWh, stored as 亿kWh)
total_generation_gwh, thermal/hydro/wind/solar_generation_gwh

-- Market volumes (亿kWh)
total_traded_gwh, spot_traded_gwh, contract_traded_gwh

-- Prices (yuan/MWh)
avg_settlement_price, spot_avg_price, contract_avg_price,
thermal/wind/solar/bess/retailer_settlement_price

-- Market structure
market_participants_total, retailers, generators, bess_participants

-- Flows (亿kWh)
incoming_gwh, outgoing_gwh

-- Load
max_load_mw

-- FR / ancillary (million yuan) — 山东 only
fr_pool_million_yuan, peak_shaving_million_yuan,
renewable_deviation_million_yuan, total_ancillary_million_yuan

-- Retail
retailer_traded_gwh, retailer_service_fee_million_yuan
```

---

## Province coverage

| Province | BESS cap | Vol | Price | Notes |
|---|---|---|---|---|
| 蒙西 | ✓ | ✓ | partial | 千kW units; settlement file also parsed |
| 安徽 | ✓ | | | 万千瓦 |
| 广西 | ✓ | | | 万kW; sheet[2]=cap, sheet[5]=settlement |
| 山西 | ✓ | | | 千kW, no header |
| 宁夏 | ✓ | ✓ | ✓ | 万kW; sheet[0]=cap, sheet[1]=settlement |
| 江苏 | ✓ | | ✓ | generic parser |
| 河南 | partial | | partial | generic parser |
| 海南 | ✓ | | ✓ | generic parser |
| 湖北 | partial | | ✓ | generic parser |
| 甘肃 | ✓ | | ✓ | year+month int cols; sheet[0]=核心指标 |
| 蒙东 | partial | | | 万kW; year+month cols |
| 青海 | ✓ | | ✓ | generic parser |
| 山东 | | ✓ | ✓ | 万kW; 17 sheets; FR costs in sheet[7] |
| 云南 | | ✓ | ✓ | header=0 sheet[2]; price in yuan/kWh×1000 |
| 冀南/冀北 | | ✓ | ✓ | jinan parser (year+month+type+vol+price) |
| 吉林/天津/湖南/辽宁/陕西/贵州/黑龙江 | | | ✓ | generic parser |
| 新疆 | partial | | | custom parser (TOC-style file) |

---

## What still needs work / possible next steps

### 1. Remaining provinces with 0 records
- `冀南-电力市场信息报告数据汇总_2025-01_2026-05.xlsx` — 0 records (column structure not matched by generic parser)
- `新疆信息披露月报数据汇总` — 0 records (sheet[0] is a TOC; actual data in sheets 1–12, need to map sheet index to correct parser)

### 2. Data quality
- Check provinces where only `avg_settlement_price` is set but no capacity:
  - 天津, 湖南, 陕西, 黑龙江, 贵州, 辽宁 — prices look correct but no capacity data
- Add cross-check: if `avg_settlement_price > 2000`, likely wrong unit (flag/fix)
- 宁夏 `bess_capacity_mw` = 0 for some months (the 储能容量(万kW) column is col[7] but may shift)

### 3. KB ingestion not yet done
The `--kb` flag in `ingest_excel_reports.py` will also push full sheet text to `staging.spot_knowledge_docs` for Strategist agent retrieval. Run:
```bash
PGURL=... python scripts/ingest_excel_reports.py --kb
```

### 4. Re-ingest when new monthly data arrives
When vendor provides updated Excels (e.g. June 2026), just copy to the relevant `data/exchange-monthly-reports/<province>月报/` folder and re-run:
```bash
PGURL=... python scripts/ingest_excel_reports.py
```
The script uses `ON CONFLICT (province, report_month) DO UPDATE SET` so re-runs are safe.

### 5. bess-map FR market tab
Currently shows only 甘肃 (0.4 bn¥/yr) and 广西 (3.56 ¥/kW·h) in `marketdata.province_fr_market`.  
The 山东 monthly ancillary costs (调频 ~87–240M¥/month, 调峰, 偏差) are in `staging.exchange_excel_metrics` but not yet surfaced in bess-map.  
To add: query `staging.exchange_excel_metrics WHERE province='山东'` in bess-map and show a line chart under the FR market section.

### 6. spot-markets Trends tab (existing PDF-extracted metrics)
The existing "📈 趋势分析 Trends" tab queries `staging.exchange_monthly_metrics` (PDF-extracted, 10 provinces).  
These could be unified with the new Excel data for a combined 30+ province view.

---

## How to deploy

```powershell
# In bess-platform/ directory
$ECR_REPO = "319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-spot-markets"
docker build -t bess-spot-markets:vNN -f apps/spot-market/Dockerfile .
docker tag bess-spot-markets:vNN "$ECR_REPO`:vNN"
docker push "$ECR_REPO`:vNN"
$env:IMAGE_TAG = "vNN"
python scripts/update_spot_markets_taskdef.py
```

## Current prod state (2026-07-09)
- Spot-markets: v52 / task def rev 70 / branch `cost-optimisation` / commit `38698c7`
- DB: `staging.exchange_excel_metrics` = 450 rows; `marketdata.province_installed_monthly` = 22 provinces with bess_mw
- Live URL: https://pjh-etrm.ai/spot-markets → Data Management → 交易所月报管理 → 🗺️ 省级Excel数据
