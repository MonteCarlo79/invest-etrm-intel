# GB储能机队收入结构与水平（自有数据）

- source: 自有复现查询 intl_market.gb_bess_monthly_index（2h系统，£/MW/月，机队实测）· license: own
- retrieved_at: 2026-10-11
- query: |
    select month, market, revenue_permw from intl_market.gb_bess_monthly_index
    where duration='2' and month >= '2026-01-01' order by month, market

> 2026年1—10月，GB 2h储能机队实测总收入（£/MW/月）：4537、3078、6775、6044、3652、5963、5432、4654、8528（9月峰值）、2772，月均约5144。
> 收入结构（2026年1—9月，£/MW/月均值）：批发交易3230（占合计约60%）、平衡机制656（约12%）、容量市场581（约11%）、调频776（约14%）、备用400、不平衡345，合计5407。能量类（批发+平衡机制）合计约72%。2—3月批发交易为负值（纯日前交易亏损月），由平衡机制与容量/调频收入补足。
> 单位换算：2h系统1MW=2MWh，月均5144 £/MW ≈ 2572 £/MWh/月 ≈ 86 £/MWh/日；按£1≈9.3元折合约800元/MWh/日（按额定储能容量归一化的日均收入，非每放1度电的收益）。仅批发交易部分：3230 £/MW/月 ≈ 500元/MWh/日。
> 对照：中国成熟省份同期2h理论最优日均收益200—300元/MWh/日（见 cn_arb_decline_own），实际捕获率约为理论值的40%—60%（自有 capture 口径）；GB机队实测均值已高于中国最优省份的理论最优（吉林724元/MWh/日，2026年1—9月）。
> 溯源：intl_market.gb_bess_daily/monthly_index，每日定时摄取（gb_ingestion_log 最近连续成功至2026-10-09），按市场分项（wholesale/bm/cm/frequency_response/reserve/imbalance/total）聚合公开结算数据；覆盖机队规模逐月增长（2026-01约2981MW→10月约4622MW）。
