# 跨省绿电 Landing-Cost & Channel Screener — Design

**Date:** 2026-10-21
**Status:** Approved design, pre-implementation
**App touched:** `apps/bess-map` (new tab). Read-only data from `marketdata.spot_prices_hourly` + `staging.spot_interprov_flow`. **No DB schema changes. No spot-market changes.**
**Source model:** `models/【卡师傅27】跨省绿电落地测算-第四周期.xlsx` (发改价格〔2026〕1077号, 2026-07-22)

---

## 1. Intent and why

Quant (Pillar 2) needs a cross-region interconnector pricing tool, ported from the 卡师傅27 Excel and extended into a channel screener. Three uses, in priority order:

1. **Procurement offers (primary):** evaluate a live green-power offer at true landed cost — offer price + per-segment transmission tariffs + compounded line losses — before signing.
2. **Thesis screening (secondary):** where does landed green power beat local spot? Landed cost vs. trailing-12-month **monthly-average RT price** in the receiving province.
3. **Channel congestion:** which channels will be crowded in 2027 — big spread × big contracted (省间协议) volume — and the 2027 flow map by channel and receiving province.

The Excel's own engine is the authority on math. It has been fully decoded and verified: its formulas reproduce its displayed outputs to the cent (§5).

## 2. Goals / non-goals / anti-goals

**Goals**
- New bess-map tab "跨省绿电" with five sections (S1–S5, §4).
- Exact port of the 4th-cycle pricing engine: 27 送出省, 5 区域, 40 专项, 18+2 paths, custom path builder.
- Agreement-volume dataset (2027) seeded from the user's industry intel + public confirmations, every row source-labeled.
- Congestion score and agreement-baseline 2027 flow map.

**Non-goals (v1)**
- DB-backed parameter editing (rejected option B — user chose repo params, A).
- Statistical forecast of 2027 flows (agreement-baseline only, §7).
- Channel rated-capacity params and capacity-utilization flags (v2 candidate).
- 到户全价: end-user 输配电费/系统运行费/基金附加 are OUT of the result — the caveat is displayed wherever landed cost appears (the Excel's own 结果边界 note).
- 上海 receiving-province spread: no RT series in our DB (not a LingFeng market); 江苏/浙江 shown as proxy with an explicit flag on the panel.

**Anti-goals**
- False precision on 2027 flows (anything finer than agreement-baseline + seasonality).
- Silent zeros on missing parameters — missing/invalid input stops output with a message, per the Excel's rules (西藏 unpriced, 辛洹线/云霄 capacity-only, blank inputs, 环境价 > 总价).
- Calling the output "到户全价".

## 3. Decisions (from the conversation)

| Decision | Choice |
|---|---|
| Usage modes | Both — procurement offers first, thesis screening second |
| Parameter storage | **A — repo, versioned** (`libs/interconnector_pricing/params.py`); update = edit + redeploy; git history = audit trail |
| Spread comparison | Monthly average **RT** price, trailing 12 months, receiving province from the 29 LingFeng markets |
| 上海 spread | Proxied by 江苏/浙江 (no LingFeng 上海 market) — flagged on the panel |
| Agreement data | Seeded from user intel + public confirmations; per-row `source_type` (公告 / 行业信息) + `confidence` (公告确认 / 行业信息 / 推算) |
| 2027 flows | Agreement-baseline flow map with historical seasonality — explicitly not a statistical forecast |
| Location | bess-map new tab; engine in app-agnostic `libs/interconnector_pricing/` (spot-market may import later) |

## 4. Tab sections

### S1 测算工作台 (calculator)
- Inputs: 送出省 (27), 报价口径 (`发电侧` | `出省节点`), path (18 presets + custom builder: up to 5 ordered segments from 40 专项 + 5 区域; 省网段 auto-prepended when 发电侧), 绿电总价 + 环境价 (元/MWh 含税).
- Outputs: 落地成本 (元/MWh and 元/kWh), 输送增加成本 (= landed − 绿电总价), per-segment detail table (环节, 段前成本, 输电单价, 线损率, 损耗折价, 段后成本), Σtariff, Σloss-cost, composite loss rate.
- Validation stops (message, no output): inputs blank; 环境价 > 总价 or < 0; 送出省=西藏; any segment capacity-only (辛洹线, 云霄); path empty or unknown.
- 出省节点 basis: no 省网 segment (label 省网已包含); only valid when the offer already includes province-grid fees — helper text says so.

### S2 方案对比 (offer comparison)
- "Save as scenario" on any S1 result → session-state table: 方案名, 送出省, 口径, 路径, 绿电总价, 环境价, Σtariff, Σloss, 输送增量, 落地总价.
- Ranked by 落地总价 ascending; per-row delete. Empty by default (live offers, not the Excel's examples).

### S3 现货价差 (spread panel)
- Receiving province selector (29 LingFeng markets; 上海 proxied → 江苏/浙江 with a visible flag), trailing 12 months.
- Chart: monthly-average RT (`marketdata.spot_prices_hourly` → monthly mean of rt_price) vs. landed-cost lines from saved S2 scenarios.
- **Profile-matched comparison (user decision 2026-10-21):** sending-energy time profile matters — 光伏-dominated sends land mostly at midday where prices run low; 风电 runs higher-priced hours. Each scenario and agreement carries a `resource_mix` (§6); the spread uses the matching RT average:
  - `solar` → monthly RT averaged over solar hours (10:00–15:00 local — builder's call)
  - `wind` → monthly RT averaged over wind hours (19:00–06:00 local — builder's call)
  - `mixed`/`hydro` → flat monthly RT average
- Metrics per scenario: average profile-matched spread (monthly RT − landed), % months with positive spread. The panel shows which hour bucket each scenario's spread uses.

### S4 参数库 (read-only browser)
- All 4th-cycle tables verbatim: 送出省 (27), 区域 (5), 专项 (40), 路径 (18 preset + 2 custom slots), 四川外送特例 (manual-only note), 区域容量分摊 (informational — NOT added to paths, per the Excel's own warning).
- Each table shows source PDF page refs; the 使用与来源 caveats displayed: 京津唐例外, 报备线损清算差额, 未列示≠声明取消, 结果边界 (不含受端到户费用).

### S5 通道拥挤度 (congestion + 2027 flow map)
- **S5a 协议电量库**: table of 2027 agreements (schema §6), source-labeled — the curated code-seeded layer is primary. Below it, a secondary table renders `staging.interconnector_gov_agreements` rows (the spot-market tab's extracted-agreements pipeline) labeled 历史协议提取 — read-only, distinct from the curated layer (builder's call: surface, don't merge).
- **S5b 拥挤度评分**: per channel — congestion score = **spread × agreement volume**. Spread basis (user decision 2026-10-21): **reference landed cost** — panel-adjustable 参考绿电总价 (default 240 元/MWh) + 环境价 (default 5), grossed through each agreement's path via the S1 engine; spread = receiver trailing-12m monthly-avg RT − reference landed, **profile-matched by the agreement's `resource_mix`** (solar → solar-hours RT, wind → wind-hours RT, else flat — same buckets as S3, user decision 2026-10-21). Always-on (no offers needed). Saved S2 scenarios override the reference for their sender+path; when several scenarios cover one channel, their spreads are averaged (builder's call). Receivers without a LingFeng RT series show the proxy rule (§S3).
- **S5c 2027 流向图**: channel × receiver matrix, agreement-baseline volumes with monthly profile from historical seasonality (same-month trailing-24-month actual flow shares where available, flat otherwise). **Undershoot flag: trailing-12-month actual flow < 80% of the 2027 contracted volume** (delivery-feasibility proxy — my call, not a judgment on the contract itself). Confidence label per row (公告确认 / 行业信息 / 推算).

## 5. Engine (exact port — verified against the Excel)

Per scenario: `电能量价 = 绿电总价 − 环境价` (must be ≥ 0). Segments: when 发电侧, prepend `省网:<送出省>`; then the path's segments in order.

Per segment i with tariff E_i (元/MWh) and loss rate F_i:
```
损耗折价 G_i = D_i × F_i / (1 − F_i)
段后成本 D_{i+1} = D_i + E_i + G_i          (D_1 = 电能量价)
```
The tariff is added flat (charged on received energy at segment end — the Excel's 各段输电费按该段受端电量计收 convention); only the accumulated cost is grossed up for losses.

```
落地总价 K = D_last + 环境价                 (环境价 flat, no loss gross-up)
Σtariff L = ΣE_i    Σloss M = ΣG_i
综合线损率 O = 1 − Π(1 − F_i)
```

**Golden test values (from the Excel, must reproduce exactly):**
| Scenario | Landed | Σtariff | Σloss | Composite |
|---|---|---|---|---|
| 青海 发电侧 240+5 via 西北—宝合—华东 | 377.782511764198 | 108.43 | 29.3525117641982 | 0.0949728426040001 |
| 甘肃 发电侧 240+5 via 西北—宝合—华东 | 370.357208258708 | 102.53 | 27.8272082587081 | 0.0914852620360001 |
| 甘肃 发电侧 240+30 (电能量210) same path | 367.839768028707 | 102.53 | 25.3097680287065 | 0.0914852620360001 |
| 青海 发电侧 240+30 same path | 375.159031302096 | 108.43 | 26.7290313020963 | 0.0949728426040001 |
| 甘肃 出省节点 240+5 via 西北—宝合—华东 | 337.236446709552 | 74.53 | 22.7064467095517 | 0.08221564 |
| 山西 发电侧 353+20 via 雁淮—华东 | 436.751853292325 | 66.8 | 16.9518532923255 | 0.044298249976 |
| 安徽 发电侧 376.45+0 via 华东区域 | 421.923395613323 | 39.2 | 6.2733956133225 | 0.0152 |

## 6. Data structures

### Params (`libs/interconnector_pricing/params.py`, `CYCLE = "第四周期 (发改价格〔2026〕1077号, 2026-07-22)"`)
Verbatim port of the Excel sheets:
- `PROVINCE_GRID: dict[str, {tariff, loss}]` — 27 送出省 (西藏 → entry with `priced: False` → stop).
- `REGIONAL_GRID: dict[str, {tariff, loss}]` — 5 区域.
- `SPECIAL_PROJECTS: dict[str, {tariff, loss, status, pdf_page, computable}]` — 40 专项; 辛洹线 + 云霄 `computable: False` → stop if in path.
- `PATHS: dict[str, list[str]]` — 18 presets (segment labels `区域：华北` / `专项：宝合直流`) + 2 custom slots.
- `SICHUAN_EXCEPTIONS`, `REGIONAL_CAPACITY_SHARE` — informational only (display, never in engine).

### Agreements (`libs/interconnector_pricing/agreements.py`, seed `YEAR = 2027`)
```python
Agreement = {
  "sender": str, "receiver": str,
  "path_id": str | None,        # key into PATHS where mappable (e.g. 甘肃→上海 via 宝合 = RT01)
  "channel_label": str,         # free text, e.g. "宝合直流"
  "volume_100gwh": float,       # 亿kWh
  "green": bool,
  "resource_mix": str,          # "solar" | "wind" | "mixed" | "hydro" — drives profile-matched spreads (S3/S5b)
  "source_type": str,           # "公告" | "行业信息"
  "source_ref": str,            # URL or "用户行业信息 2026-10"
  "confidence": str,            # "公告确认" | "行业信息" | "推算"
}
```
**Seed rows (2027):** 宁夏→福建 20 solar (公告确认, sohu/解放日报 links); 青海→福建 10 solar (行业信息); 新疆→上海 25 solar (行业信息, cumulative-20亿 public context); 甘肃→上海 20 solar via 庆东 (行业信息); 甘肃→上海 20 solar via 宝合 (行业信息); 甘肃→上海 10 solar via 甘浙线 H2-2027 (公告确认, 陇电入浙 2027 operational); 陕西→上海 7.5±2.5 mixed (行业信息, range noted); 山西→上海 10 mixed (行业信息, 煤电+新能源打捆); 青海→上海 40/yr 2026–2028 solar (公告确认, 北京日报 link). Context row (not a path agreement): 甘肃 2026 green deals 127亿 total to 7 provinces (公告确认, 甘肃经济网 link). S2 scenarios gain the same `resource_mix` input (selector: solar/wind/mixed/hydro; default solar for green offers).

### DB reads (no writes)
- `marketdata.spot_prices_hourly` — RT monthly averages per receiving province (S3).
- `staging.interconnector_trades` — channel-level 2026 trade flows (S5b actuals, S5c seasonality): the spot-market Interconnector tab's table; read-only cross-pillar. NOT `staging.spot_interprov_flow` — builder-check established that table holds only the daily NATIONAL 省间现货 summary (送端/受端 totals + extreme-province metric rows), no per-channel series.
- `staging.interconnector_mlt_snapshot` — per-channel 2025 MLT volumes (seasonality depth).
- `staging.interconnector_gov_agreements` — extracted agreement rows, rendered as S5a's secondary layer.

## 7. Files

- Create `libs/interconnector_pricing/__init__.py`, `params.py`, `engine.py` (pure calc + validation, returns segments detail + totals or a stop reason), `agreements.py` (dataset + congestion scoring + flow-map builders — pure functions).
- Create `tests/interconnector_pricing/test_engine.py` (golden values §5 + all validation stops + custom path), `test_agreements.py` (seed integrity: volumes>0, receivers known, confidence labels valid; congestion ordering; seasonality shares sum to 1).
- Modify `apps/bess-map/app.py` — new tab + i18n keys (en/zh) in existing dict style; S1–S5 sections.
- No DB migrations; no other apps touched.

## 8. Testing & verification

- Golden values (§5) reproduce within 1e-9.
- Every validation stop has a test (blank, 环境价>总价, 西藏, 辛洹线, 云霄, empty path).
- Custom path: segment ordering, >5 rejected, unknown segment rejected.
- S3 aggregation: monthly mean correctness on a synthetic series; 上海 shows proxy flag.
- S5: score ordering matches spread×volume; undershoot flag at <80%; seasonality fallback to flat when no history.
- Local run of bess-map: tab renders, S1 calc matches golden, S2 save/rank/delete, S3 chart loads 江苏, S5 tables render.

## 9. Out of scope (v1)

- DB-backed param editing (rejected B); 情报录入 of new pricing cycles (params stay code).
- Rated capacities per UHV channel + capacity-utilization flags (v2).
- Statistical 2027 flow forecasting; hourly flow profiles.
- Quant-agent tool exposure of the calculator (candidate fast-follow: `get_landed_cost` tool mirroring the agent-tool pattern — not in v1).
- spot-market Interconnector tab changes (it stays as-is; this tool reads the same underlying tables read-only).

## 10. Builder's calls (marked, not user-approved)

- 上海 proxy → 江苏/浙江 with visible flag.
- S2 scenarios live in session state (not persisted); persistence is a v2 if wanted.
- Tab position: appended after existing tabs in bess-map; label 跨省绿电 / Interconnector Green.
- Seasonality fallback: flat monthly profile when a channel lacks 24 months of history.
- S5a secondary layer: `interconnector_gov_agreements` rows surfaced (separate table, labeled 历史协议提取), never merged into the curated code-seed.
- Multi-scenario channel coverage in S5b: spreads averaged when several S2 scenarios share a channel.
- Undershoot flag: trailing-12-month actual < 80% of 2027 contracted volume — feasibility proxy, not a contract judgment.
- Profile hour buckets (user-directed feature, my bucket definitions): solar = 10:00–15:00 local, wind = 19:00–06:00 local, mixed/hydro = flat monthly average. Bucket edges are params in `params.py` for later tuning.
