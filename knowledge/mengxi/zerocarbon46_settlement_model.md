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
3. **Green premium** = Σ contract vol × 环境价值: 98.5–99.5% of bill 绿电 rows
   (March outlier 167% — unresolved)
4. **Contract position** = 省内 汇总 rows (net of 置换) + 跨省 rows:
   Jul 50,724 MWh = 106% of bill volume — matches deck 仓位106% exactly
5. **CfD model**: contract vol × (contract price − 系统参考价) — right variant,
   but monthly-avg reference misses delivery-window effects (月内融合 trades
   settle at their window's prices). July (合区后) nails it; Feb/Jun off 30–40%.
   Waterfall uses **bill-implied CfD** (bill_spot − spot_value) for exactness.
6. **Deck vs bill bases**: 复盘 decks are wind-only (Jul 上网电量 31,450 MWh);
   bill is station total (47,805 MWh = wind + solar + storage net).

## Open discrepancies

- **Jul volume**: proxy 110% of bill (52,860 vs 47,805) — curtailment hypothesis
  (deck notes 大风日); ID schedule > metered actual
- **Jan volume**: proxy 80% of bill — ID-cleared data gaps early Jan
- **March**: green 167% of bill; contract position 170% of bill volume —
  possible oversold month with buybacks

## Data pipeline

- `trades.py` — parse 省内/跨省 全月成交单 (41,253 rows → wind_trades);
  汇总 rows are platform-netted positions; only (source_file, row_no) is a
  safe natural key (置换 fragments repeat identical values)
- `backfill_dispatch.py` — md_id × md_rt_nodal → wind_dispatch_15min (COPY via
  staging; plain executemany = hours over cross-Pacific link)
- `report.py --persist` — monthly replication → wind_settlement_monthly
