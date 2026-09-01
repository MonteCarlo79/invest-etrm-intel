# 蒙西 BESS 节点注册表 (Mengxi BESS Node Registry)

Asset → grid connection point → Fengxing price node. Sources: 系统接入报告/评审意见 in `reports/系统可研报告/` (extracted 2026-09-01). Machine-readable: standard markdown table, `|` delimited.

## Current portfolio (6)

| asset_code | plant_name | capacity_mw | substation | conn_kv | fengxing_own_node | fengxing_parent_nodes |
|---|---|---|---|---|---|---|
| suyou | 景蓝乌尔图储能电站 | | 锡西二500kV变电站 | 220 | 内蒙.景蓝乌尔图储能电站/220kV.1M | (锡西二站尚未出现在节点表) |
| hangjinqi | 悦杭独贵储能电站 | | 谷山梁500kV变电站 | 220 | 内蒙.悦杭独贵储能电站/220kV.1M | 内蒙.谷山梁站/220kV.1M–4M |
| siziwangqi | 景通四益堂储能电站 | | 杜尔伯特220kV变电站 | 110 | (无自有节点) | 内蒙.杜尔伯特站/220kV.1M–2M |
| gushanliang | 裕昭沙子坝储能电站 | 500 | 谷山梁500kV变电站 | 220 | 内蒙.裕昭沙子坝储能电站/220kV.1M | 内蒙.谷山梁站/220kV.1M–4M |
| bameng | 景怡查干哈达储能电站 | | 河套500kV变电站 | 220 | 内蒙.景怡查干哈达储能电站/220kV.1M | 内蒙.河套站/220kV.1M–4M |
| wulate | 远景乌拉特储能电站 ⚠️ | 100 | 德岭山500kV变电站 | 220 | 内蒙.远景乌拉特储能电站/220kV.1M | 内蒙.德岭山站/220kV.1M–4M |

## Upcoming (3 — for simulation study)

| asset_code | capacity | substation | conn_kv | fengxing_parent_nodes |
|---|---|---|---|---|
| alashan | 1000MW/4000MWh | 阿拉腾敖包500kV变电站 | 220 | (阿拉腾敖包站尚未出现在节点表) |
| wuchuan | 1000MW/4000MWh | 武川500kV变电站 | 220 | 内蒙.武川站/220kV.1M–4M; 500kV.1M–2M |
| xixier | 1000MW/4000MWh | 锡西二500kV变电站 | 220 | (锡西二站尚未出现在节点表) — 与苏右同一母站 |

## Per-asset notes & source documents

- **suyou (苏右)** — 苏右旗100万储能项目接入系统评审意见.pdf: 评审意见内容实为锡西二站项目(与24.263同文) — 锡西二站220kV侧接入, 待最终确认. 运营数据自 2025-02-12.
- **hangjinqi (杭锦旗)** — 杭锦旗谷山梁电网侧智慧独立新型储能电站示范项目接入系统报告20240424.pdf: 远景储能220kV升压站打捆明阳储能, 约30km线路接入谷山梁变220kV侧. 运营数据自 2026-01-01.
- **siziwangqi (四子王旗)** — 关于乌兰察布四子王旗新型储能电站项目接入系统报告审定.pdf: 110kV升压站约2km接入杜尔伯特220kV变110kV侧 (T接杜塔线过渡→切改正式间隔). 无自有节点, 价格节点=杜尔伯特站220kV (价格聚类分析独立验证一致). 运营数据自 2026-01-01.
- **gushanliang (谷山梁)** — 谷山梁500千伏变电站电源侧独立储能电站子项目1、2--接入系统（收口）/: 子项目1(500MW) 0.1km串入子项目2升压站, 再约4km接入谷山梁变220kV侧. 裕昭沙子坝=子项目1或2 待确认. 运营数据自 2026-01-06.
- **bameng (巴盟)** — 25.041.…巴彦淖尔河套新型储能专项行动项目接入系统设计的评审意见(1).pdf + 巴彦淖尔河套…设计报告（3.17收口）.pdf: 约3km接入河套500kV变220kV侧. 运营数据自 2026-01-05.
- **wulate (乌拉特中旗)** — 乌拉特中旗德岭山新型储能电站项目接入系统报告0425.pdf: 100MW, 约1.5km接入德岭山500kV变220kV侧, 打捆国电投乌中旗钠离子储能. ⚠️ plant_name=远景乌拉特储能电站为推断 (cleared data 中唯一乌拉特命名BESS), 待确认. 运营数据自 2025-01-22.
- **alashan (阿拉善)** — 阿拉善盟阿拉腾敖包新型储能专项行动项目（电网侧）接入系统报告2410.pdf: 约5km接入阿拉腾敖包500kV变220kV侧. 投产后以 cleared data plant_name 补全.
- **wuchuan (武川)** — 25.022.…评审意见.pdf + 关于转发…的通知.pdf + 呼和浩特武川…报告（修编收口稿）.pdf: 约10km接入武川500kV变220kV侧.
- **xixier (锡西二)** — 锡林郭勒盟苏尼特右旗锡西二站…接入系统报告1016.pdf + 24.263.…评审意见.pdf: 约5km接入锡西二500kV变220kV侧. 与苏右(景蓝乌尔图)同一母站 — 未来同节点邻居.

## Co-node / congestion-zone relationships

- **谷山梁 220kV bus zone**: hangjinqi + gushanliang (各自升压站计量, 相距数km-30km — 平日同价, 阻塞日可分叉, 2026-05-20 实证) + 库布其凝光400MW光储 / 库布其洁能800MW光储 / 库布其云恒800MW光储 / 明阳(亚什图,把栅).
- **锡西二 220kV bus zone**: suyou + xixier (upcoming).
- 节点表中变电站多表记 (.1M–.4M) 对应不同母线/线路计量点, 阻塞时同站不同表价可分叉.
