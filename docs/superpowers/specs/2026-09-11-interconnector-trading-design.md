# Interconnector Trading Tab — Design Spec

**Date:** 2026-09-11 (v2 — incorporates user's 5-part analytical framework)
**Status:** Pending user review
**App:** `apps/spot-market` (Strategist, Pillar 1)
**Prototype:** `debug/interconnector_topology.html` (v3, user-validated through 3 iterations)

---

## 1. Context & Goals

New tab **"Interconnector / 跨区通道"** in the spot-market app for cross-region interconnector trading study.

**Topology goal:** show how provinces connect via all interconnectors (physical DC channels).

**Analytical framework (user's, 2026-09-11):**

- **A1 — Spare capacity:** from interconnector capacity and historical flow volume, derive spare capacity and volume for the balance of the year.
- **A2 — Renewable export requirement:** from the MLT-contract percentage of renewable generation in sending provinces, derive how much volume must be exported to meet their MLT percentage — otherwise they face revenue recycle of the MLT-vs-spot price spread.
- **A3 — Month-ahead spread backtest:** month-ahead contract prices in both export and import provinces (exchange-published reports) vs spot price of the delivery month → backtest actual price spread.
- **A4 — Historical MLT trade patterns:** 2025 + 2026 cross-region MLT trades → patterns.
- **A5 — Daily 省间现货 patterns:** spot reports' daily cross-region spot trading prices and volumes → patterns.

Visual/interaction design validated via prototype v3: physical channel view (converter-station lines, width ∝ capacity, color = voltage class), trade-flow view (province-pair arcs), province-click → linked-channels panel, channel table.

## 2. Datasets

| # | Source | Content | Storage | Load path |
|---|--------|---------|---------|-----------|
| 1 | `data/interconnector/华东跨省数据汇总.xlsx` (user-curated, 2026 Jan–Sep, updated continuously) | 234 channel-level MLT trades into 江苏/上海/浙江/福建: 标的月份/期间类型, sender (incl. blends), 通道1–3, 校核前/后电量, 上网价, 落地价, 景融量价, 通道费 | `staging.interconnector_trades` | In-app upload; full replace per upload + name+size fingerprint guard |
| 2 | `data/interconnector/跨区组织交易情况（多条通道网损）.xlsx` (**2025 full-year**, per user; file has no period column) | 中长期 413 rows + 上海 59 rows: 送出区域/省, 受入省, 交易类型 (其他市场化/省间绿电市场化/省间绿电优先计划/其他优先计划/优先计划配套大水电 + 汇总 subtotal rows), 输电通道, volume 亿kWh, 落地均价 | `staging.interconnector_mlt_snapshot` | In-app upload; **user enters snapshot label at upload** (e.g. `2025-full`); re-upload replaces same label. `汇总` rows ingested with `is_subtotal` flag, excluded from aggregations |
| 3 | Channel registry | 26 channels: name, formal name, stations, provinces, coords, kV, GW, 投运, km, category, note | `staging.interconnector_channels` | Seeded from `services/interconnector/registry.py` (mirror of `knowledge/interconnectors/channel_registry.md`); upsert on app start |
| 4 | `staging.spot_interprov_flow` (existing) | Daily 省间现货 avg price, price change, total volume by direction (2024–2026 parsed from daily reports) | — | Read-only (A1 volume, A5) |
| 5 | `marketdata.spot_prices_hourly` (existing, LingFeng) | Provincial DA/RT spot prices | — | Read-only (A3 delivery-month spot) |
| 6 | `marketdata.spot_fundamentals_hourly` (existing, LingFeng, 29 markets) | Provincial fundamentals incl. renewable output | — | Read-only (A2); **verify at build whether actual or day-ahead forecast renewable; prefer actual column if present** |
| 7 | `staging.exchange_monthly_metrics` (existing, `services/exchange_reports/`) | Parsed provincial monthly exchange reports: avg transaction/settlement price, spot-vs-MLT volume split, 省间受入/送出 volumes, renewable share | — | Read-only (A3 month-ahead MLT prices) |
| 8 | `knowledge/interconnectors/mlt_contract_requirements.md` (new) | MLT contract-percentage requirement per sending province | — | **Extracted from policy docs in the knowledge pool (`staging.spot_knowledge_docs`) + `data/policies/`; reviewed by user before use.** Tab treats missing province as "not set", never guesses |

### `staging.interconnector_trades` schema

`id SERIAL PK, target_month_raw TEXT, period_type TEXT (月度/月内/多月/年度), month_start DATE, month_end DATE, recv_province TEXT, send_raw TEXT, send_anchor TEXT, channel_1/2/3 TEXT, vol_pre_mwh NUM, vol_post_mwh NUM, send_price NUM, land_price NUM, jingrong_vol_mwh NUM, jingrong_price NUM, channel_fee NUM, source_file TEXT, uploaded_at TIMESTAMPTZ`

Month normalization: `1月`→Jan; `3-12月`→Mar–Dec; `1-12月`/`2026`→full year; `5-8月`→May–Aug; `6月 `(trailing space) tolerated; 年度 type → Jan–Dec. `month_start` = first day of first month; `month_end` = **last day of last month** (inclusive range).

### `staging.interconnector_channels` schema

`name PK, formal_name, send_station, send_prov, send_lon, send_lat, recv_station, recv_prov, recv_lon, recv_lat, kv, gw, commissioned, km, category, note, updated_at`

### `staging.interconnector_mlt_snapshot` schema

`id SERIAL PK, snapshot_label TEXT, sheet TEXT, send_region TEXT, send_prov TEXT, recv_prov TEXT, trade_type TEXT, channel TEXT, is_subtotal BOOL, volume_100m_kwh NUM, landing_price NUM, UNIQUE(snapshot_label, sheet, send_prov, recv_prov, trade_type, channel)`

## 3. Tab Sections

### S1 通道拓扑 (Topology) — prototype v3 behavior

Physical channel view (converter-station lines, width ∝ GW, color = voltage class, click line → detail card, click province → linked-channels panel) + trade-flow view toggle (province-pair arcs from 2026 trades) + stats strip + channel table. Fed from tables 1+3. GeoJSON (0.6MB, DataV) committed to `assets/geo/china_provinces.json`, shipped in image.

### S2 通道裕度 (A1 + A5: capacity & flows)

- **Per-channel balance-of-year card/table:** capacity GW → full-year energy capability (GW × 8760h), 2026 YTD traded volume (table 1, per-channel attribution: multi-channel series trades count full volume on each), implied spare energy for remaining months. **Labeled as traded-volume proxy** — true physical flow data (dispatch-center disclosures) is not ingested; caveat shown in-section. (Table 4 is province-level national aggregate, not per-channel — it feeds the A5 panel and province context, not this table.)
- **A5 daily 省间现货 panel:** date-range selector → daily total volume + avg price trend (送出/受入 direction), province share table, day-type pattern (weekday/weekend), price-change distribution. From table 4.
- Utilization % = traded ÷ capability, per channel, with the ATC caveat from the registry framework (physical ≠ tradeable).

### S3 中长期交易 (A4 + A2: MLT patterns & export requirement)

- **A4 pattern explorer:** 2025 snapshot (table 2, label `2025-full`) vs 2026 trades (table 1): volume by trade type （市场化/绿电/优先计划）, by channel, by province pair, YoY comparison where pairings exist; price levels （落地均价） by trade type; green-power premium （绿电 vs 其他市场化 same pair).
- **A2 export-requirement table:** per sending province (from trades + snapshot senders): monthly renewable output (table 6), MLT % requirement (table 8, user-reviewed), required MLT volume = renewable × %, actual exported MLT volume (tables 1+2) → **gap = required − actual** → recycle-risk exposure (gap × |MLT − spot| spread from A3 inputs). Provinces without a reviewed % show "未设定".
- **S2/S3 shared MLT explorer** (from earlier design, retained): filters （受入/送出/通道/月份/期间类型）, trade-level table, monthly volume stacked by receiving province, landing-vs-sending price scatter (diagonal = channel fee), 景融 vs 落地 comparison (146/234 rows).

### S4 价差回测 (A3: month-ahead spread backtest)

- Pair selector: send province → recv province + delivery month.
- Inputs: month-ahead MLT price for delivery month M (from exchange reports, table 7 — published in month M−1 reports); delivery-month actual spot avg for both provinces (table 5); cross-region MLT landing price for the pair where it exists (table 1).
- Output: **backtest table** per delivery month — 送端月前价 vs 送端现货， 受端月前价 vs 受端现货， cross-region landing vs 受端现货 (premium), realized spread = 受端现货 − 送端现货 − 通道费； hit-rate stats (% months MLT landing beat receiving spot).
- Coverage: only provinces with parsed exchange reports (table 7) + LingFeng spot (table 5); missing → "无数据", never interpolated.

## 4. Map Rendering (key technical decision)

**Primary:** `streamlit-echarts` (`st_echarts` with `on_events={"click": ...}`) — native click → Python round-trip drives the side panel. ECharts JS and the GeoJSON are shipped inside the Docker image (no browser CDN dependency; jsdelivr is flaky in China).

**Fallback** (if streamlit-echarts misbehaves in the container): province `st.selectbox` + regenerated static echarts HTML via `st.components.v1.html` — same panel, no map click.

**Verification gate:** click-through works on localhost:8505 before anything else is built on top.

## 5. Files

| File | Purpose |
|---|---|
| `services/interconnector/__init__.py` | Package marker |
| `services/interconnector/registry.py` | 26-channel seed data (mirror of `knowledge/interconnectors/channel_registry.md`) |
| `services/interconnector/ingest.py` | Excel parsers (华东汇总 + 跨区组织), month normalization, subtotal flagging, fingerprint dedup, upserts, CREATE TABLE IF NOT EXISTS |
| `services/interconnector/data.py` | Query helpers: flows agg, per-channel trade agg, spread/backtest series, A2 gap computation |
| `services/interconnector/topology.py` | ECharts option builders (physical/flows views, theme-aware) |
| `apps/spot-market/interconnector_tab.py` | Tab render (S1–S4 sections, uploaders, EN/CN labels) |
| `apps/spot-market/app.py` | Tab registration + i18n strings |
| `apps/spot-market/requirements.txt` | + `streamlit-echarts` |
| `assets/geo/china_provinces.json` | DataV GeoJSON |
| `knowledge/interconnectors/channel_registry.md` | ✅ Done — reviewed registry + analytical framework |
| `knowledge/interconnectors/mlt_contract_requirements.md` | MLT % per province, extracted from policy KB, user-reviewed (see §7) |
| `tests/interconnector/test_ingest.py` | Fixture Excels → parse asserts; month normalization; subtotal flagging; fingerprint dedup; registry↔trade channel name match |
| `tests/interconnector/test_data.py` | VWAP math; per-channel attribution; A2 gap math; backtest alignment (M−1 report → delivery month M) |

## 6. Analytical Framing (from reviewed registry)

- Channel categories: 电源绑定型 (planned, limited market freedom) vs 大基地型 (incremental market resource) vs 制度创新型 (云霄: transmission-rights pilot from 2026-06) vs 联网型.
- ATC framing: 800万kW physical ≠ tradeable; ATC = capacity − 配套优先 − MLT − 保供 − 安全约束. Future extension when 输电权 price data exists (云霄).
- Margin formula: Cross-regional Trading Margin = Power Spread − Transmission Right Price − Losses.
- Research priority (user): 宝合 > 云霄 > 庆东 ≈ 坤渝 > 宜华/林枫.

## 7. MLT % Rules Extraction (A2 prerequisite)

1. Search knowledge pool (`staging.spot_knowledge_docs`) + `data/policies/` for provincial rules on 新能源中长期签约/交易比例 (e.g. 蒙西/甘肃/宁夏/新疆/青海/山西/黑龙江 requirements).
2. Draft `knowledge/interconnectors/mlt_contract_requirements.md`: province → required %, source doc + quote, effective period.
3. **User reviews and corrects before the tab reads it.** Missing/unreviewed provinces render as 未设定.
4. Rules change mid-year — the file carries an effective-date column; tab uses the rule active for the delivery month.

## 8. Testing & Verification

- `pytest tests/interconnector/` green (parsers pure-python on fixtures; DB paths mocked).
- Local: `streamlit run apps/spot-market/app.py --server.port 8505` — tab renders; upload both Excels via UI (snapshot label prompt for #2); S1 click works; A4 numbers hand-checked against the Excels; A3 backtest for 山西→江苏 sane.
- Full `tests/spot_ingest` + app-adjacent suites stay green.

## 9. Deploy

Separate in-session confirmation required. `bess-spot-markets` v32 → v33, terraform apply, verify at `https://www.pjh-etrm.ai/spot-markets/`.

## 10. Out of Scope

- Strategist agent tools for interconnector data (add later once tables prove out).
- RAG/knowledge-pool ingestion of the reference PDFs in `data/interconnector/`.
- ATC modeling beyond the traded-volume proxy; 输电权 price tracking (云霄) — needs data sources not yet in the platform.
- True physical flow ingestion (dispatch-center disclosures).
