# 零碳46 (悦盛昌渠) 结算复制模型 — 2026-09-28

Wind farm settlement replication: md.* market data + 中长期成交单 → book 6 账单.
Built in `services/wind_settlement/`; views in asset_risk Tab 7/8 (wind mode).

## Identifiers

| Layer | Value |
|---|---|
| 发电单元 (trades) | `新_悦盛昌渠#1期` |
| Nodal node (md_rt_nodal_price) | `内蒙.悦盛昌渠风光储电站/220kV.1M` |
| Plant (md_id_cleared_energy) | `悦盛昌渠风光储电站` |
| Settlement book | `rm_books` id=6 (W-B-2, parser_wb2) |
| Station | 130083341 悦盛昌渠风光储电站 (风+光+储, bill = station total) |

## Bill structure (上网电费结算单, 2026)

- `放电结算: 现货` = **全电量 × 结算电价** — one row netting spot value AND
  contract CfD (not RT price × volume!)
- `放电结算: 绿电/填电` = green premium (合约电量 × 环境价值)
- Fees: 储能容量补偿 (≈−25 元/MWh of settlement volume), 调频, 市场平衡/调节类,
  不平衡资金, 偏差 — allocated market-wide, NOT computable bottom-up
- July spot price 0.2234 元/kWh = deck 电能电价 223 ✓

## Key calibrations (validated vs 7 months of bills + 复盘 decks)

1. **md_id_cleared_energy stores MW, not MWh** (column name lies).
   Energy = Σ cleared × 0.25. Feb +0.08%, Jun +0.25% vs bill volume.
2. **Capture price** = Σ(gen×RT_nodal)/Σgen: Jul 219.5 vs deck 219 ✓
3. **Contract position** = 省内 汇总 rows (net of 置换) + 跨省 rows:
   Jul 50,724 MWh = 106% of bill volume — matches deck 仓位106% exactly

## Rules-derived settlement model (2026 rules, absorbed 2026-09-28)

Sources: `政策规则/2026中长期交易方案宣贯.pptx` (106 slides) +
`20251230 通知（蒙西）.pdf` (内能源电力字〔2025〕783号, scanned — read via vision) +
`202412 规则体系.pdf` (271pp, text layer).

**发电侧电能量电费 (规则体系 第十四条):**
- 现货全电量电费 = Σ_t 上网电量_t × 节点电价_t
- 中长期差价合约电费 = Σ_t Σ_c 合约电量_c,t × (合约价_c − **用户侧区域结算参考点电价_t**)
- 省间售电中标合约 → 中标价 − **送出节点所在区域**结算参考点 (= 呼包以西 for 悦盛)

**Zone map (counterparty 所属地区):**
- east (呼包以东): 呼和浩特, 乌兰察布, 锡林郭勒
- west (呼包以西): 包头, 鄂尔多斯, 巴彦淖尔, 乌海, 薛家湾, 阿拉善, 跨省送出
- sys (全网统一): 电网代理工商业 (代购)
- **2026-07 起合约东西合区**: all contracts settle at system ref (deck + data agree)

**绿电溢价 = Σ_t min(合约曲线_t, 实际计量_t) × env** (曲线合理度 basis,
slide 88): settles per-interval on the LESSER of contract curve vs metered.
Bill sits inside [flat-curve Σmin, aggregate Σvol×env] in ALL 7 months;
≈ aggregate in 5 (near-full coverage), Mar 89.9% of aggregate (171% position,
real uncovered volume). Flat-curve Σmin is a conservative floor (60–90%)
because real 96-point/四小时 curves concentrate in high-wind hours.

**CfD zone model validation (vs bill-implied, 2026-01..07):**
- Jun **97.6%** (−5.57M vs −5.71M), Feb 112%, Apr 111% — model correct in structure
- Jul: unified (合区) sys ref ✓ (implied proxy-contaminated by +10% volume)
- Jan/Mar/May off — implied contaminated by dispatch data gaps (25/27/29 days)
  and 置换 daily-granularity effects
- **CRITICAL data trap**: 省内 总表 所属地区 changes semantics across file
  versions (Jan–Mar = user region; Jun–Jul = generator region, constant
  鄂尔多斯). Zone mapping MUST use 供电局 (bureau), region as fallback only.

**Fees are rule-defined but allocated**: 市场平衡类 (阻塞盈余/结构平衡/计量平衡,
by 上网量), 市场调节类 (风险防范补偿/回收 vs 自身中长期合约均价 band,
新能源平衡补偿), 签约比例考核 (年度 20% of 同类型新能源年度平均交易电价 ×
shortfall; 月度/分时 bands — slides 94-97). Take from bills, flagged allocated.

**置换**: energy price + env follow the ORIGINAL contract; 置换价格 =
撮合价 − 电能量 − 环境价值 (cash-settled, see 置换费 files).

## Exchange daily clearing (日清算) — the ground truth (2026-09-28)

`日清算202601~202608.xlsx` → `marketdata.wind_daily_clearing` (25,632 rows,
2026-01-01→09-24, parser `clearing.py`, loader `load_clearing.py`).
Per-15-min: 计量电量, 电能电费, 省内实时节点电价, 中长期合约电量/电价
(aggregate contract curve), 曲线合理度取小值 = min(合约,计量), 取均值.

**This replaces all proxies.** Validations:
- Metered = bill volume to the MWh, all 7 months (vol_ratio 1.000)
- Mar/Apr 电能电费 = bill 现货电费 to the CENT; Jul gap = exactly the bill's
  second 现货 row (78,504.03, 退补); Jan 0.6%, May 1.8%, Jun 1.8%,
  Feb 8.5% (1.09M unexplained premium — largest residual)
- CfD now exchange-exact: cfd_exchange = Σ电能电费 − Σ计量×RT
- Backed-out effective ref (per-interval identity): Jan 347 vs 东 341.8,
  May 278 vs 281.5, Jul 305 vs sys 302.8 (合区 confirmed), Jun 368 (73% east mix)
- Exchange contract curve ≈ trades-derived position (Mar/Jun/Jul exact match)
  except Feb (48,280 vs 62,445) and May (91,749 vs 93,577)

**Zone model vs exchange CfD**: validated Jan/Feb/Apr/Jun/Jul (83–115%);
Mar/May fail (+7.6M/+7.0M vs +1.7M/+0.8M) — over-hedged months (171%/112%)
where 送出-heavy positions (Mar 57% 跨省) don't follow the west-ref rule;
needs per-product 送出 reference detail. Zone model = forward attribution
tool; exchange clearing = ground truth for the covered period.

**Replication residuals (waterfall)**: 5 of 7 months within 1.7%;
Feb 7.6% (退补-like premium), Mar 6.2% (green aggregate model overshoot
at 171% position). Green: bill inside [Σmin, aggregate] all months.

## Open discrepancies

- ~~Feb +1.09M bill premium~~ **RESOLVED 2026-09-28**: the 日清算 file carries a
  PARTIAL contract basis for Feb (48,280 MWh of the 62,445 in the trades
  confirmations — 月度挂牌+月度撮合 ≈ 14.2 GWh registered at month-end close).
  The bill settles on the full basis; the zone model on trades replicates the
  Feb bill CfD to **0.7%** (8,952,803 vs 8,893,745). May shows the same
  provisional-basis effect at 1.8%. Rule: clearing = ground truth for
  volumes/spot/curve; **when its contract basis diverges >5% from the trades
  confirmations, trust trades+zone for the CfD** (flagged in the waterfall).
- 送出 (跨省) CfD reference rule for mixed-source products (锡泰直流) —
  effective ref in Mar/May sits east of the west-rule expectation

## Data pipeline

- `trades.py` — parse 省内/跨省 全月成交单 (41,253 rows → wind_trades);
  汇总 rows are platform-netted positions; only (source_file, row_no) is a
  safe natural key (置换 fragments repeat identical values)
- `clearing.py` / `load_clearing.py` — 日清算 → wind_daily_clearing (COPY);
  时刻 cells are mixed time/datetime/1900-epoch types; 00:00 = 24:00 convention
- `backfill_dispatch.py` — md_id × md_rt_nodal → wind_dispatch_15min (COPY via
  staging; plain executemany = hours over cross-Pacific link)
- `report.py --persist` — monthly replication → wind_settlement_monthly
  (exchange clearing preferred; ID-cleared proxy fallback)
