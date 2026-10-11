# GB储能机队收入结构与水平（自有数据）

- source: 自有复现查询 intl_market.gb_bess_monthly_index（2h系统，£/MW/月，机队实测）· license: own
- retrieved_at: 2026-10-11
- query: |
    select month, market, revenue_permw from intl_market.gb_bess_monthly_index
    where duration='2' and month >= '2026-01-01' order by month, market

> 2026年1—10月，GB 2h储能机队实测总收入（£/MW/月）：4537、3078、6775、6044、3652、5963、5432、4654、8528（9月峰值）、2772，月均约5144。
> 2026年10月收入结构：批发套利2449（占88%）、调频373（13%）、容量市场201（7%）、备用99、不平衡43（合计口径2722，各市场覆盖率不同）。
> 单位换算：2h系统1MW=2MWh，月均5144 £/MW ≈ 2572 £/MWh/月 ≈ 86 £/MWh/日；按£1≈9.3元折合约800元/MWh/日。
> 对照：中国成熟省份同期2h理论最优日均收益200—300元/MWh/日（见 cn_arb_decline_own），实际捕获率约为理论值的40%—60%（自有 capture 口径）；GB机队实测均值已高于中国最优省份的理论最优（吉林724元/MWh/日，2026年1—9月）。
