# Price Forecasting — Merit-Order + PCA Shape Models (bess-map)

**Status:** design approved 2026-09-14 (architectural path; cross-region import/export folded in per user request)
**App:** `apps/bess-map` (Pillar 2 / Quant)
**New tab:** "Price Forecast / 价格预测"

---

## 1. Goal

Per province, forecast day-ahead hourly RT price curves using two complementary model families:

1. **Merit-order stack (structural):** build each province's supply stack from coal/gas fleet segments and fuel costs; marginal price = variable cost of the marginal unit at the hour's residual demand, with imports as priced blocks from interconnector data and exports as added demand.
2. **PCA shape model (statistical):** decompose historical daily price shapes into principal components; forecast component scores from grid forecasts (load/renewable/bidding-space D-1) and interprovincial flow features; reconstruct the hourly curve.

Headline forecast = **hybrid**: merit-order anchors the price *level*, PCA provides the *shape*. Each model is also viewable standalone in the tab.

**Consumer (phase decision, confirmed):** analysis/visualization tab first. Pipeline integration (as a `bess_capture_daily` forecast model driving realized-revenue simulation and IRR) is a later, separate step if backtests prove the model predictive.

## 2. Data foundation (all confirmed present in DB except fuel/fleet)

| Data | Source table | Notes |
|---|---|---|
| RT prices, 29 provinces | `marketdata.spot_prices_hourly` | training/backtest target |
| Load, renewable, bidding space, wind, solar — realized + D-1/D-2/D-3 | `marketdata.spot_fundamentals_hourly` | `load_mw`, `load_d1_mw`…; `renewable_total_mw`, `renewable_total_d1_mw`…; `bidding_space_mw`, `bidding_space_d1_mw`… |
| Net export, realized + D-1 | same | `net_export_mw` |
| Channel registry (physical) | `staging.interconnector_channels` | send_prov→recv_prov, capacity GW, category (电源绑定/大基地/制度创新) |
| Monthly trades with prices | `staging.interconnector_trades` | `recv_province`, channels, volumes, **send_price / land_price**, channel_fee |
| Monthly MLT snapshots | `staging.interconnector_mlt_snapshot` | send→recv volume + landing_price by trade_type |
| Daily interprov report prices | `staging.spot_interprov_flow` | report_date, direction, price_yuan_kwh, volume |
| **Coal/gas fuel costs + fleet composition** | **NOT IN DB — sourced via Hermes extraction (§4)** | the one genuinely new data asset |

## 3. Architecture

```
services/bess_map/price_lab/          # NEW — pure-computation package (no DB, no I/O)
    merit_order.py                    # stack builder + marginal price solver + markup calibration
    pca_shapes.py                     # PCA decomposition + score regression + curve reconstruction
    hybrid.py                         # level (merit-order) + shape (PCA) combination
services/hermes/fuel_fleet_screener.py  # NEW — KB/web extraction (capcomp_screener pattern)
services/hermes/fuel_fleet_etl.py       # NEW — upsert + conflict detection (capcomp_etl pattern)
apps/bess-map/price_forecast_tab.py     # NEW — tab module (app.py imports it)
tests/services/price_lab/               # NEW — unit tests
tests/hermes/test_fuel_fleet_etl.py     # NEW
```

Follows existing repo patterns: `forecast_engine.py` (pure compute), `province_cap_comp` (draft/confirmed review workflow), `interconnector_tab.py` (tab module imported by the app).

## 4. Fuel & fleet data — Hermes auto-extraction (user-confirmed sourcing)

**New table `marketdata.province_fuel_fleet`:**

```sql
province TEXT NOT NULL,
effective_date DATE NOT NULL,
coal_price_yuan_t NUMERIC,            -- 省级动力煤标杆/指数价
gas_price_yuan_m3 NUMERIC,            -- 天然气门站价
fleet_segments JSONB NOT NULL,        -- [{fuel:"coal"|"gas", capacity_mw, heat_rate_kj_kwh, vom_yuan_mwh, label}]
source TEXT DEFAULT '',               -- provenance doc / URL
status TEXT DEFAULT 'draft',          -- draft | confirmed | superseded (capcomp pattern)
notes TEXT DEFAULT '',
ingested_at TIMESTAMPTZ DEFAULT NOW(),
UNIQUE (province, effective_date, source)
```

- `services/hermes/fuel_fleet_screener.py`: per province, KB search (`spot_knowledge_docs` FTS) + web search for 标杆煤价/动力煤指数/天然气门站价/发电装机结构; Claude extraction → structured rows.
- `services/hermes/fuel_fleet_etl.py`: upsert with conflict detection (same-province same-date differing values → `conflict` status, human resolves), modeled on `capcomp_etl.py`.
- Review flow: rows land as `draft`; user confirms in bess-map Data Management tab (new small section) or via Feishu reply. Only `confirmed` rows feed the model.
- Coverage order: 山东, 山西, 蒙西, 广东, 甘肃, 江苏, 浙江, 河北(南网/冀北), 河南, 新疆 first (top BESS provinces); remaining provinces follow. Provinces without confirmed data show "无燃料数据" in the tab, not fabricated numbers.
- Fleet segments start coarse (2–4 segments per province: e.g. coal high-eff / coal low-eff / CCGT / OCGT) — refinement is a data-quality iteration, not a code change.

## 5. Merit-order model (`merit_order.py`)

**Stack construction (per province, per day):**
- Segments ordered by variable cost = fuel_price × heat_rate + VOM.
- Wind/solar at ~zero variable cost, capped at that hour's forecast output (`wind_d1`, `solar_d1`).
- **Imports as priced blocks:** per receiving province, one block per active channel from `staging.interconnector_trades` / `mlt_snapshot`: price = latest **landing price** (send_price + channel_fee + losses), capacity-limited (registry `gw` as ceiling; realized recent monthly volume as the practical cap). Blocks sit in the stack by landing price.
- **Exports as added demand:** export volume joins residual demand (competes with local load for the stack).

**Hourly solve:**
- residual_demand = load_d1 − renewable_d1 − net_export_d1_mw (sign convention: positive net_export = outflow adds demand, inflow reduces; `_d1_mw` variant used where populated, realized `net_export_mw` as fallback).
- Marginal price = variable cost of the marginal segment + scarcity markup.
- **Markup calibration (approved judgment call):** pure fuel-cost stacks underprice tight hours. Fit per province `markup(tightness)` from history — tightness = residual_demand / available capacity; piecewise-linear, bounded ≥ 1.0, fit on last 90d, refit weekly by the tab loader (cached).

**Outputs:** per-province hourly marginal-cost series + the stack itself (for the explorer chart).

## 6. PCA shape model (`pca_shapes.py`)

- Per province: matrix of last 365 days × 24h of `rt_price − daily_mean(rt_price)` (deviation separates shape from level).
- Top K components, K chosen at ≥ 85% variance explained (typically 3–5: level-adjacent, duck-curve depth, evening peak).
- **Score forecasting:** ridge regression per component on:
  - grid forecasts: `load_d1_mw`, `renewable_total_d1_mw`, `bidding_space_d1_mw`, `wind_d1`, `solar_d1`
  - **interprovincial features (user addition):** net import share of load (from `net_export_d1`), receiving-side landing price (`interconnector_trades` latest), channel utilization vs registry capacity
  - calendar: day-of-week, month
- Reconstruction: shape = Σ score_k × loading_k.
- **Backtest:** rolling-origin over last 90d; MAE + sMAPE per province vs `naive_rt_lag1` and `ols_rt_time_v1` baselines. Results table shown in tab ③ — this is the evidence for later pipeline integration.

## 7. Hybrid (`hybrid.py`)

- level_t = merit-order marginal price (structural anchor)
- shape_t = PCA reconstructed deviation
- forecast_t = level_t + shape_t, clipped to province-observed price bounds (e.g. [−80, 1500] ¥/MWh sanity clip per province historical 0.1%/99.9% quantiles).

## 8. Tab UX (`price_forecast_tab.py`, 3 sections)

1. **Merit-order explorer:** province + date → step-curve stack chart (segments incl. import blocks colored distinctly), vertical marker at residual demand, marginal price readout; editable fuel price / fleet inputs for what-if scenarios (session-state only, no DB writes).
2. **PCA decomposition:** scree plot, top-K component shapes (24h loadings), score time series with the fundamental drivers overlaid.
3. **Forecast vs actual:** date picker → D-1 hybrid forecast curve vs realized RT; per-province backtest table (MAE/sMAPE, hybrid vs both baselines, plus merit-order-only and PCA-only columns).
- D-1 first; D-2/D-3 selectable where `*_d2_mw`/`*_d3_mw` columns are populated.
- i18n EN/ZH like the rest of the app; model registry strings follow existing `_ENG_KEY` pattern.

## 9. Phasing

| Phase | Deliverable | Depends on |
|---|---|---|
| 1 | `province_fuel_fleet` table + Hermes screener/ETL + Data Management review section | — |
| 2 | `merit_order.py` + tab ① explorer | Phase 1 (≥3 provinces confirmed) |
| 3 | `pca_shapes.py` + `hybrid.py` + tabs ②③ + backtest | Phase 2 (can start in parallel; hybrid needs it) |

## 10. Testing & deploy

- Unit tests: stack ordering/marginal solve (synthetic fleets), import-block placement, markup bounds, PCA fit/reconstruct round-trip, score-regression determinism (fixed seed), hybrid clip bounds, ETL upsert/conflict paths (capcomp-test pattern).
- No module-level DB calls in the tab module beyond the app's existing loader pattern; AppTest headless smoke before deploy (repo pre-deploy standard).
- Deploy: bess-map v65 via the Aliyun-mirror Dockerfile trick (PyPI still unreliable from this machine); hermes image redeploy for the screener (jq-swap from current td:178, env guard ≥31).
- **No DB migration risk:** one new table, additive only; no changes to existing tables/views.

## 11. Explicitly out of scope (YAGNI)

- Pipeline/capture/IRR integration (later, evidence-gated)
- Nodal/asset-level prices (province-level only)
- Confidence intervals / stochastic simulation
- Auto-refresh of fuel prices on a schedule (manual re-scan via Feishu/Data Mgmt for now)

## 12. Approved judgment calls (do not re-litigate during implementation)

1. Markup(tightness) calibration inside the merit-order model — the stack alone underprices scarcity hours.
2. Hybrid = stack level + PCA shape as the headline forecast; both components also standalone.
3. Hermes auto-extraction with `draft` → human `confirmed` review before model use.
4. Analysis tab first; no capture-pipeline changes in this iteration.
