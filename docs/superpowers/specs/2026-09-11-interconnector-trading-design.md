# Interconnector Trading Tab — Design Spec

**Date:** 2026-09-11
**Status:** Pending user review
**App:** `apps/spot-market` (Strategist, Pillar 1)
**Prototype:** `debug/interconnector_topology.html` (v3, user-validated through 3 iterations)

---

## 1. Context & Goals

New tab **"Interconnector / 跨区通道"** in the spot-market app for cross-region interconnector trading study. Three goals (user's words):

1. **Topology** — show how provinces are connected based on all interconnectors (physical DC channels)
2. **MLT price/volume analysis** — mid-to-long-term contract trading prices and volumes; find patterns
3. **Spread analysis** — sending vs receiving province market prices (spot from LingFeng DB; monthly exchange reports where needed)

Visual/interaction design already validated via prototype: physical channel view (converter-station lines, width ∝ capacity, color = voltage class), trade-flow view (province-pair arcs), province-click → linked-channels panel, channel table.

## 2. Datasets

| # | Source | Content | Storage | Load path |
|---|--------|---------|---------|-----------|
| 1 | `data/interconnector/华东跨省数据汇总.xlsx` (user-curated, updated continuously) | 234 channel-level MLT trades into 江苏/上海/浙江/福建, Jan–Sep 2026: 标的月份/期间类型, sender (incl. multi-province blends), 通道1–3, 校核前/后电量, 上网价, 落地价, 景融量价, 通道费 | `staging.interconnector_trades` | In-app upload in tab; full replace per upload (cumulative snapshot) + name+size fingerprint guard against double-ingest (structurer pattern) |
| 2 | `data/interconnector/跨区组织交易情况（多条通道网损）.xlsx` (May 2026, static) | 413 rows national MLT by channel + trade type (中长期/上海 sheets), volume 亿kWh, 落地均价 | `staging.interconnector_mlt_snapshot` (snapshot_label `2026-05`) | In-app upload, one-time; re-upload replaces same label |
| 3 | Channel registry | 26 channels: name, formal name, stations, provinces, coords, kV, GW, 投运, km, category, note | `staging.interconnector_channels` | Seeded from `services/interconnector/registry.py` (mirror of `knowledge/interconnectors/channel_registry.md`); upsert on app start |
| 4 | `staging.spot_interprov_flow` (existing) | Daily 省间现货 avg prices/volumes by direction | — | Read-only |
| 5 | `marketdata.spot_prices_hourly` (existing, LingFeng) | Provincial DA/RT spot prices | — | Read-only |
| 6 | `staging.exchange_monthly_metrics` (existing, `services/exchange_reports/`) | Parsed provincial monthly exchange reports incl. 省间受入/送出 volumes | — | Read-only (S3 phase 2) |

### `staging.interconnector_trades` schema

`id SERIAL PK, target_month_raw TEXT, period_type TEXT (月度/月内/多月/年度), month_start DATE, month_end DATE, recv_province TEXT, send_raw TEXT, send_anchor TEXT, channel_1/2/3 TEXT, vol_pre_mwh NUM, vol_post_mwh NUM, send_price NUM, land_price NUM, jingrong_vol_mwh NUM, jingrong_price NUM, channel_fee NUM, source_file TEXT, uploaded_at TIMESTAMPTZ`

Month normalization: `1月`→Jan; `3-12月`→Mar–Dec; `1-12月`/`2026`→full year; `5-8月`→May–Aug; `6月 `(trailing space) tolerated; 年度 type → Jan–Dec. `month_start` = first day of first month; `month_end` = **last day of last month** (inclusive range).

### `staging.interconnector_channels` schema

`name PK, formal_name, send_station, send_prov, send_lon, send_lat, recv_station, recv_prov, recv_lon, recv_lat, kv, gw, commissioned, km, category, note, updated_at`

### `staging.interconnector_mlt_snapshot` schema

`id SERIAL PK, snapshot_label TEXT, sheet TEXT, send_region TEXT, send_prov TEXT, recv_prov TEXT, trade_type TEXT, channel TEXT, volume_100m_kwh NUM, landing_price NUM, UNIQUE(snapshot_label, sheet, send_prov, recv_prov, trade_type, channel)`

## 3. Tab Sections

### S1 通道拓扑 (Topology)

Prototype v3 behavior, fed from tables 1+3:

- **物理通道视图**: one line per channel between converter-station coords; width ∝ GW; color by voltage class (±100/±400/±500/±660/±800/±1100/500kV AC, sequential blue ramp); click line → detail card; click province → panel of linked channels (送出/受入, capacities, 2026 traded volume, VWAP landing price), non-linked lines dim.
- **交易流向视图**: province-pair arcs from trades aggregated by (send_anchor, recv_province); width ∝ volume; color by receiving province (江苏 blue/上海 rose/浙江 teal/福建 amber, validated OKLab ≥15).
- Stats strip; full channel table (sorted by GW); flows aggregation on the fly from `interconnector_trades`.
- China provinces GeoJSON (0.6MB, DataV) committed to `assets/geo/china_provinces.json`, shipped in image.

### S2 中长期交易 (MLT Explorer)

- Filters: 受入省 / 送出方(raw) / 通道 / 月份范围 / 期间类型 (月度/月内/多月/年度).
- Trade-level table (all columns from source).
- Charts: monthly volume stacked by receiving province; landing-vs-sending price scatter per trade (x=上网价, y=落地价, size=volume, color=recv province; diagonal = channel fee); 景融均价 vs 落地均价 comparison where 景融 data exists (146/234 rows).
- May snapshot (`interconnector_mlt_snapshot`) as separate sub-section: pivot by trade type × channel.

### S3 价差分析 (Spread Analysis)

- Pair selector: send_anchor → recv_province (from trades), e.g. 锡盟→江苏.
- Monthly series per pair: VWAP MLT landing price (trades), VWAP sending-side price, channel fee.
- Overlay: monthly avg spot price for receiving province and sending-anchor province from `marketdata.spot_prices_hourly` (DA; RT if available), matched to the trade's 标的月份 (month_start..month_end → weighted by month span).
- Derived lines: 落地 − 受端现货 (MLT premium over receiving spot); 落地 − 上网 − 通道费 (residual margin check).
- Daily 省间现货 avg price overlay from `spot_interprov_flow` where direction matches.
- **Phase 2:** provincial MLT market prices from `exchange_monthly_metrics` for provinces without spot coverage; 景融 platform spread.

## 4. Map Rendering (key technical decision)

**Primary:** `streamlit-echarts` (`st_echarts` with `on_events={"click": ...}`) — native click → Python round-trip drives the side panel. ECharts JS and the GeoJSON are shipped inside the Docker image (no browser CDN dependency; jsdelivr is flaky in China).

**Fallback** (if streamlit-echarts misbehaves in the container): province `st.selectbox` + regenerated static echarts HTML via `st.components.v1.html` — same panel, no map click.

**Verification gate:** click-through works on localhost:8505 before anything else is built on top. This is the riskiest piece and gates S1.

## 5. Files

| File | Purpose |
|---|---|
| `services/interconnector/__init__.py` | Package marker |
| `services/interconnector/registry.py` | 26-channel seed data (mirror of knowledge/interconnectors/channel_registry.md) |
| `services/interconnector/ingest.py` | Excel parsers (华东汇总 + 跨区组织), month normalization, fingerprint dedup, upserts, CREATE TABLE IF NOT EXISTS |
| `services/interconnector/data.py` | Query helpers: flows agg, per-channel trade agg, spread series, MLT filters |
| `services/interconnector/topology.py` | ECharts option builders (physical/flows views, theme-aware) |
| `apps/spot-market/interconnector_tab.py` | Tab render (S1/S2/S3 sections, uploaders, EN/CN labels) |
| `apps/spot-market/app.py` | Tab registration + i18n strings |
| `apps/spot-market/requirements.txt` | + `streamlit-echarts` |
| `assets/geo/china_provinces.json` | DataV GeoJSON |
| `knowledge/interconnectors/channel_registry.md` | ✅ Done — reviewed registry + analytical framework |
| `tests/interconnector/test_ingest.py` | Fixture Excels → parse asserts; month normalization; fingerprint dedup; registry↔trade channel name match (catches 宝合-type gaps) |
| `tests/interconnector/test_data.py` | VWAP math, per-channel volume attribution (multi-channel trades count full volume on each), spread series alignment |

## 6. Analytical Framing (from reviewed registry — drives future work)

- Channel categories: 电源绑定型 (planned, limited market freedom) vs 大基地型 (incremental market resource) vs 制度创新型 (云霄: transmission-rights pilot from 2026-06) vs 联网型.
- ATC framing: 800万kW physical ≠ tradeable; ATC = capacity − 配套优先 − MLT − 保供 − 安全约束. Future S3 extension when 输电权 price data exists (云霄).
- Margin formula: Cross-regional Trading Margin = Power Spread − Transmission Right Price − Losses.
- Research priority (user): 宝合 > 云霄 > 庆东 ≈ 坤渝 > 宜华/林枫.

## 7. Testing & Verification

- `pytest tests/interconnector/` green (fixtures synthetic, no DB needed for parsers; DB-touching paths mocked).
- Local: `streamlit run apps/spot-market/app.py --server.port 8505` — tab renders; upload both Excels via UI; S1 click works; S2 filters/charts correct against hand-checked numbers from the Excel; S3 series sane for 锡盟→江苏.
- Full `tests/spot_ingest` + app-adjacent suites stay green.

## 8. Deploy

Separate in-session confirmation required. `bess-spot-markets` v32 → v33, terraform apply, verify at `https://www.pjh-etrm.ai/spot-markets/`.

## 9. Out of Scope

- Strategist agent tools for interconnector data (add later once tables prove out).
- RAG/knowledge-pool ingestion of the reference PDFs in `data/interconnector/`.
- ATC modeling, 输电权 price tracking (云霄) — needs data sources that don't exist yet in the platform.
- `exchange_monthly_metrics` joins in S3 (phase 2).
