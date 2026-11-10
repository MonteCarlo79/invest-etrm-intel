# Retail-risk P2 — coverage, benchmarks, CRM (2026-11-10)

Trigger: user review — attribution dead (no benchmarks), recon all red, only 山东 had contracts.
Root cause for all three: parser/coverage gaps, not missing data.

## A. Trades coverage + reconciliation
- 冀南: every non-日滚动 file is the counterparty format (sheet 交易结果, header row 3:
  购方/售方/时间段/分时段编码/成交电量 period total). Direction from 景融 as 购方/售方;
  volume spread evenly over 时间段 days; channel from folder (年度双边/年度竞价) or filename
  (月度竞价*, 上/中/下旬竞价). Net 中长期 now equals invoice 中长期 volume for Jan–Jun and Aug;
  Jul +157 MWh (0.7%) unexplained.
- 浙江 Jan–Mar (CSV starts 2026-04): 日滚动撮合汇总 *分日持仓量 sheet — side table (label/量/价,
  read until 滚撮) + daily 滚撮 rows (signed); 年度 grids give hourly shape, other channels spread
  evenly (旬 = days 1-10/11-20/21-end). Monthly VWAP ties to the sheet's own 撮合后日均价.
- Hourly position volumes now net sells (were summed).
- Recon: MIDLONG_MODE — 浙江/山东 settle 中长期 as CfD vs DA (expected = Σ_h net vol × (price − DA)),
  whole load at spot; 冀南/安徽 physical. Status: matched (midlong+total tie) / explained (midlong
  ties, spot estimate off) / flagged (midlong disagrees) / no_invoice|no_trades|no_prices (grey).
  Tab lists only the selected book's months.

## B. Benchmarks (信息汇总 workbooks have no 2026 rows)
| Province | Source | Months | source tag |
|---|---|---|---|
| 山东 | 信息披露月报 PDF 表2 (一) | Jan–Apr | disclosure |
| 浙江 | 复盘 deck 月份×渠道 clearing-price table (later deck wins) | Jan–May | review |
| 冀南 | 复盘 deck hourly 全网 年度/月度/月内 (month from slide title) | Jan–Mar | review_hourly |
| 安徽 | none machine-readable (Jul/Aug PDFs state it in prose only) | — | — |
- 冀南 hourly → monthly weighted by OUR hourly buys: (B − our buy VWAP)·V = Σ_h v_h(mkt_h − p_h) exactly.
- Trader alpha: best-ranked source per channel (disclosure > review > infohub), volume-weighted
  across matching rows (月度集中竞价 + 月度双边 → monthly_auction).
- 亿kWh → MWh factor fixed: ×100,000 (was ×10,000).

## C. CRM contracts (冀南/浙江/广东/福建/上海/江苏; 山东 unchanged source)
- share_ratio = CHANNEL's share of the margin (冀南 '80%*上浮', 浙江 执行调整后 > 签约, 福建 渠道分成比例,
  上海 分成部分政策). P1 assumed our share of revenue — changed.
- 渠道费 = vol × (per-MWh 居间费 + share × max(margin, 0)) + annual lump/12;
  margin = spread for 价差 packages, else price − blended wholesale benchmark. Losses not shared.
- Spread/fee fields live in rm_customer_contracts.price_formula (JSONB) — no schema change.
- Units: 浙江/广东/江苏 万度 ×10; 浙江/江苏 元/度 ×1000. 浙江/上海 lack end dates → 2026-12-31.
- Writer upserts (re-runs refresh BD/channel/share).

## Not done / open
- Live backfill + redeploy (need explicit confirmation; RDS unreachable from Mac → ECS run-task).
- New SQL (CfD hourly UNION, DA query, per-book months, contract fields) untested on Postgres.
- 浙江 Jun–Aug and 冀南 Apr+ benchmarks; 安徽 benchmarks; 山东 渠道分成比例 semantics unconfirmed.
