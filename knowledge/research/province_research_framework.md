# 省份电力市场调研主线框架

**来源:** Dipeng Chen, 2026-10-16(首次用于安徽调研)
**用途:** 各省/各地实地调研的统一问题骨架。数据团队先逐题尝试从平台数据与知识库找答案(A 档,作为现场核实口径),找不到的列为现场必问(B 档)。调研回来后,答案回灌知识库,框架本身随实践修订。

---

## Q1. 供需与装机基本盘
各种发电装机规模、发电量、用电量、外来/外送电量、储能规模。

## Q2. 火电经营状况
火电发电小时数、容量补偿情况。

## Q3. 136 机制电价
历史情况和未来情况:供需比、电价水平、机制电量比例。

## Q4. 装机增长
今年到年底、明年、十五五装机新增情况。

## Q5. 跨省通道与交易
跨省通道和交易情况、省间协议电量情况;大基地/外送装机规模(可选)。

## Q6. 用电增长
用电新增情况。

## Q7. 储能收益
价差和调频收益;调频标准多少;一次/二次调频资金池多大;是否有储能参与,收入情况如何。

## Q8. 系统运行费
情况及未来水平。

## Q9. 售电侧
售电公司收益情况、批零价差多少;电厂和售电公司中长期协议比例。

## Q10. 现货市场分摊
分摊水平多少。

## Q11. 新能源限电
限电率多少。

## Q12. 交易政策与策略
今年到年底前以及明年电力交易政策;是否取消双边交易;发电侧是否抱团;发电的长协比例多少;长协强制比例偏差的回收系数;哪些交易策略可以盈利;市场风险在哪里;后续供需情况如何。

## Q13. 煤价
煤价情况如何。

## Q14. 签约策略
明年的签约策略:固定价、浮动价格;跨省 PPA 价格和情况。

## Q15. 绿电绿证
绿电绿证需求和交易情况。

---

## 使用规则
1. 逐题先查平台数据 + 知识库(见各题数据入口,下表),有答案的写 A 档并注明来源与日期。
2. 无覆盖的题列入 B 档,设计成现场索取具体文件/口径的问题。
3. 调研纪要回灌知识库(Intel Intake / Feishu INGEST),更新对应省份的 A 档。

## 数据入口速查(平台现有)
| 题 | 数据入口 |
|---|---|
| Q1 | market fundamentals Excel (`data/market-fundamentals/`)、`spot_fundamentals_hourly`、`province_installed_monthly` |
| Q2 | fundamentals Excel(利用小时)、`province_cap_comp` |
| Q3 | KB(136号文/机制电价文档)、政策时间线 |
| Q4 | `province_installed_monthly`、`marketdata.province_storage_pipeline`(储能) |
| Q5 | `staging.spot_interprov_flow`、跨区通道知识(`knowledge/interconnectors/`) |
| Q6 | fundamentals、`spot_fundamentals_hourly`(负荷趋势) |
| Q7 | `bess_capture_daily`(价差/capture)、`province_cap_comp`、调频 intake 路由(ancillary_revenue) |
| Q8 | 系统运行费 ETL(hermes sysopfee) |
| Q9-Q12 | 主要依赖 KB 制度文档与现场 — 多数省份为 B 档 |
| Q13 | 煤价指数 ETL(hermes coal_index) |
| Q14-Q15 | KB + 现场 |
