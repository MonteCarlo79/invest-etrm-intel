# Handoff: Risk Management Apps — Session 2026-07-16

**Repo:** `MonteCarlo79/invest-etrm-intel`, branch `cost-optimisation`  
**Spec files:**
- `docs/superpowers/specs/2026-07-16-asset-risk-design.md` — App 1 (Asset Risk), **Approved**
- `docs/superpowers/specs/2026-07-16-retail-risk-design.md` — App 2 (Retail Risk), **Approved**

---

## What was designed in this session

Two new Streamlit apps for the bess-platform, following the existing ECS Fargate + Cognito + PostgreSQL `marketdata` schema pattern.

### App 1 — Asset Risk Management
- **Path:** `apps/asset-risk/` | **Port:** 8512 | **ECR:** `bess-asset-risk`
- Covers: power generation (wind/solar/BESS/thermal) asset books
- 6 tabs: Asset Config, Settlement, Realised P&L, Positions & MtM, VaR & Greeks, Agent

### App 2 — Retail Risk Management
- **Path:** `apps/retail-risk/` | **Port:** 8513 | **ECR:** `bess-retail-risk`
- Covers: retail load books, customer CRM, procurement positions
- 6 tabs: CRM, Settlement, Realised P&L, Positions & MtM, VaR & Greeks, Agent

---

## Key architecture decisions

| Decision | Choice | Rationale |
|---|---|---|
| App structure | Two separate Streamlit apps | Independent deployment, clear ownership |
| DB | Shared PostgreSQL `marketdata` schema, `rm_` prefix | Reuse existing RDS, shared libs |
| Shared libs | `libs/risk/` (mtm, var, pnl, greeks), `libs/settlement/` | DRY; App 2 reuses everything from App 1 |
| Instruments at launch | bilateral + spot only | Open enum for futures/options/forwards/profiles |
| VaR methodology | Historical simulation + parametric delta-normal | Monte Carlo added when options traded |
| Forward curve | LingFeng near-term + manual CSV long-term | Existing pipeline reused |
| Position volume schema | Unified book-level date×hour table | User confirmed: same format for asset + retail |

---

## Core database schema (key tables)

All tables in `marketdata` schema, prefix `rm_`.

### Shared by both apps

**`rm_assets`** — wind/solar/BESS/thermal asset registry  
**`rm_books`** — trading books (`book_type`: 'asset' or 'load')  
**`rm_positions`** — individual trade records (field: `channel` IN da/rt/monthly_auction/monthly_listed/intramonth_match/annual/ancillary/capacity)  

**`rm_position_volumes`** — **unified hourly position table** (book_id, delivery_date, hour):
- 6 channel prices: `da_price`, `rt_price`, `monthly_auction_price`, `monthly_listed_price`, `intramonth_match_price`, `annual_price` (all `_cny_mwh`)
- 6 channel volumes: same names with `_volume_mwh`
- Derived: `market_price_cny_mwh`, `actual_price_cny_mwh`, `pnl_cny`
- Volume waterfall: `nominated_mwh`, `cleared_mwh`, `settled_mwh`
- Deviation attribution: `deviation_bid_mwh`, `deviation_equipment_mwh`, `deviation_sysop_mwh`, `deviation_grid_flow_mwh`

**`rm_forward_curves`** — forward price curves (LingFeng + manual upload)  
**`rm_settlements`** + **`rm_settlement_items`** — settlement invoices and line items  
**`rm_pnl_snapshots`** — monthly P&L snapshots (includes wind KPIs: `curtailment_mwh`, `curtailment_rate_pct`, `curtailment_opportunity_cost_cny`, `equivalent_hours`)  
**`rm_var_snapshots`** — VaR snapshots  
**`rm_dispatch_plan`** — 15-min dispatch plan (BESS: nominated/forecast/dispatched/actual MW; Wind: D+1 forecast + actual output)  
**`rm_dispatch_daily`** — daily BESS ops summary (charge/discharge windows, cycle count, financials)  

### Retail-only tables
**`rm_customers`** — customer master (includes: `channel_name`, `fixed_spread_cny_mwh`, `revenue_share_ratio` from 用户渠道信息)  
**`rm_customer_contracts`** — contracts (type: fixed/indexed/peak_offpeak/indexed_band; `price_formula` JSONB; K1/K2/K3 adjustments)  
**`rm_customer_profiles`** — daily×hourly customer load  
**`rm_crm_import_configs`** — province-keyed column mapping for 各省份台账.xlsx  

---

## Known asset portfolio

| Asset | Type | Province | Migration source |
|---|---|---|---|
| 零碳46风电 | wind | Inner Mongolia (Mengxi) | 零碳46风电经营统计_YYYYMMDD.xlsx |
| 裕昭沙子坝 | bess | Inner Mongolia (Mengxi) | WeCom daily + Trade Capture.xlsx |
| 远景乌拉特 | bess | Inner Mongolia (Mengxi) | WeCom daily |
| 景怡查干哈达 | bess | Inner Mongolia (Mengxi) | WeCom daily |
| 景通四益堂 | bess | Inner Mongolia (Mengxi) | WeCom daily |
| 四子王旗 | bess | Inner Mongolia (Mengxi) | WeCom daily |
| 悦杭独贵 | bess | Inner Mongolia (Mengxi) | WeCom daily |
| 景蓝乌尔图 | bess | Inner Mongolia (Mengxi) | WeCom daily |

---

## Migration sources

### Source 1 — Trade Capture.xlsx (BESS settlement ledger)
- Path: `C:\Users\dipeng.chen\OneDrive\Envision Energy\Asset Investment Platform\Operating Assets\System\Trade Capture 20260211.xlsx`
- Sheet: `Trades` — Date, Market, Station Name, Volume (MWh), Price (¥/MWh), Total (¥)
- Target: `rm_settlement_items` (row-level direct import)

### Source 2 — 零碳46风电经营统计_YYYYMMDD.xlsx (wind farm ops)
- Path: `C:\Users\dipeng.chen\OneDrive\Envision Energy\Asset Investment Platform\Operating Assets\零碳\零碳46风电经营统计_20250627.xlsx`
- 7 sheets; 3 are ingested:

| Sheet | Target | Notes |
|---|---|---|
| 风场功率 | `rm_dispatch_plan` | 日期+时间 (15-min), D+1预测MW, 实际出力MW |
| 结算明细 | `rm_position_volumes` | 27 cols; 15-min → hourly aggregation; all channel prices+volumes |
| 市场价格 | `rm_forward_curves` | TOU monthly: 谷/平/峰 ¥/MWh |
| 经营统计 | `rm_pnl_snapshots` | Monthly KPIs including curtailment metrics |

- **Key finding:** April 2025 curtailment rate was **58%** (severe grid curtailment); May 23%; Jun 8%. Curtailment column (弃风量) maps to `deviation_grid_flow_mwh` in `rm_position_volumes`.
- **Mengxi wind settlement rule:** DA volume settled at DA price → residual at RT node price → bilateral contract premium on top. Min/max comparison applied. Implemented in `libs/settlement/categorizer.py` with `province='inner_mongolia_mengxi', asset_type='wind'`.

### Source 3 — 各省份台账.xlsx (retail CRM)
- Path: `C:\Users\dipeng.chen\OneDrive\Envision Energy\Trading\2026 Trades\台账\各省份台账.xlsx`
- One sheet per province (Hunan, Hubei, Zhejiang, Shandong, others)
- Target: `rm_customers` + `rm_customer_contracts`

### Source 4 — 各省基础数据 (retail position + P&L data)
- Path: `C:\Users\dipeng.chen\OneDrive\Envision Energy\Trading\2025 Trades\Year End\各省基础数据\各省基础数据\`
- Contains per-province folders (安徽, 山东, 浙江). Key files:
  - `山东/交易销售PL.xlsx` → P&L categories: 售电分成, 服务费, 滚搓, 代理用电量
  - `山东/202501/零售侧结算单.xlsx` → monthly per-customer ledger → `rm_settlement_items`
  - `山东/202501/用户渠道信息.xlsx` → channel + 固定差价 + 分成 → `rm_customers`
  - `山东/202501/日清算单/` → daily per-company settlement → `rm_settlement_items`
  - `山东/202501/日用电曲线/YYYY-MM-DD.xls` → hourly customer load → `rm_customer_profiles`
  - `浙江/交易与价格汇总(2).xlsx` → full trading waterfall (年度/月度/日前/实时)

### Source 5 — Jiangsu exchange data (15-min customer profiles)
- Path: `C:\Users\dipeng.chen\OneDrive\Envision Energy\Trading\2025 Trades\江苏\Data\远景\`
- Format: `营销历史数据_YYYY-MM-DD (N).csv`
- Columns: 日期, 户号, 用户名称, 售电公司名称, then 96 × 15-min intervals (00:15…24:00) in kWh
- Target: `rm_customer_profiles` (aggregate 4 × 15-min → hourly)

---

## Retail procurement channels

| `channel` value | Chinese name | Description |
|---|---|---|
| `annual` | 年度双边 | Annual bilateral procurement |
| `monthly_auction` | 月度竞价 | Monthly exchange auction |
| `monthly_listed` | 月度挂牌 | Monthly OTC listed trades |
| `intramonth_match` | 月内撮合 | Intramonth rolling match |
| `DA` | 日前 | Day-ahead spot |
| `RT` | 实时 | Real-time balancing |

Green power (绿电) is a position with `energy_source='wind'/'solar'` in the contract, using whichever channel it was procured through.

---

## Province-specific pricing

| Province | Structure | `price_formula` example |
|---|---|---|
| Hunan | Fixed + K1/K2/K3 adjustments | `{type:"fixed_with_k", base:370, k1:0.5, k2:0.5}` |
| Hubei | Fixed ¥/MWh | `{type:"fixed", price:399}` |
| Zhejiang | Indexed (benchmark+spread) or peak/offpeak | `{type:"indexed", spread:2.0}` |
| Shandong | Indexed band or fixed | `{type:"indexed_band", floor:360, cap:420}` |
| Jiangsu | Fixed or indexed | `{type:"fixed", price:380}` |

---

## Automation

### BESS daily data (production path)
WeCom self-built app (自建应用) → webhook on file post to group 康富资产管理-储能场站日报群 → S3 → ingest:
- 运营统计 Excel → `rm_dispatch_daily`
- 调度计划表 Excel → `rm_dispatch_plan`

Credentials needed: `corpid`, `corpsecret`, `token`, `encoding_aes_key` from WeCom admin console.

Short-term fallback: Windows Task Scheduler folder watcher on `assets/operating/` at 08:00 daily.

### Wind farm (manual at launch)
Upload 零碳46风电经营统计_YYYYMMDD.xlsx via Tab 2 upload panel.

---

## What the next session should do

**The next step is to invoke the `writing-plans` skill to create the implementation plan for App 1 (Asset Risk Management).**

App 1 builds first; App 2 starts after `libs/risk/` and `libs/settlement/` are stable.

### App 1 build sequence (from spec §7)
1. DB migrations — all `rm_` tables
2. `libs/settlement/parser.py` — multi-format ingestion (PDF, Excel, wind farm migration)
3. `libs/settlement/categorizer.py` — category rules + Mengxi wind settlement rule
4. `libs/risk/mtm.py` + `libs/risk/pnl.py`
5. `libs/risk/var.py` + `libs/risk/greeks.py`
6. `services/forward_curve/` — LingFeng pull + manual upload
7. `services/operating_assets/` — WeCom receiver + folder watcher + ingestion pipeline
8. App tabs 1–5
9. Tab 6 agent
10. Docker + ECS deploy

### Prompt to use in the new session

```
Read docs/superpowers/specs/2026-07-16-asset-risk-design.md and 
docs/superpowers/specs/2026-07-16-retail-risk-design.md. Both specs are 
Approved. The full design context is in docs/handoff-2026-07-16-risk-management.md.

We are ready to start implementing. Invoke the writing-plans skill to create 
a detailed implementation plan for App 1 (Asset Risk Management) first. 
App 2 builds after libs/risk/ and libs/settlement/ from App 1 are stable.
```

---

## Important file paths

| What | Path |
|---|---|
| Asset risk spec | `docs/superpowers/specs/2026-07-16-asset-risk-design.md` |
| Retail risk spec | `docs/superpowers/specs/2026-07-16-retail-risk-design.md` |
| This handoff | `docs/handoff-2026-07-16-risk-management.md` |
| Wind farm Excel | `C:\Users\dipeng.chen\OneDrive\Envision Energy\Asset Investment Platform\Operating Assets\零碳\零碳46风电经营统计_20250627.xlsx` |
| BESS Trade Capture | `C:\Users\dipeng.chen\OneDrive\Envision Energy\Asset Investment Platform\Operating Assets\System\Trade Capture 20260211.xlsx` |
| Settlement samples | `C:\Users\dipeng.chen\OneDrive\Envision Energy\Asset Investment Platform\Operating Assets\settlement\` |
| BESS daily reports | `C:\Users\dipeng.chen\OneDrive\ETRM\bess-platform\assets\operating\` |
| CRM source | `C:\Users\dipeng.chen\OneDrive\Envision Energy\Trading\2026 Trades\台账\各省份台账.xlsx` |
| Retail province data | `C:\Users\dipeng.chen\OneDrive\Envision Energy\Trading\2025 Trades\Year End\各省基础数据\各省基础数据\` |
| Jiangsu exchange CSV | `C:\Users\dipeng.chen\OneDrive\Envision Energy\Trading\2025 Trades\江苏\Data\远景\` |
