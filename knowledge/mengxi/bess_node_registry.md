# 蒙西 BESS 节点注册表 (Mengxi BESS Node Registry)

Asset → grid connection point → Fengxing price node. Sources: 系统接入报告/评审意见 in `reports/系统可研报告/` (extracted 2026-09-01). Machine-readable: standard markdown table, `|` delimited.

## Current portfolio (6)

| asset_code | plant_name | capacity_mw | substation | conn_kv | fengxing_own_node | fengxing_parent_nodes |
|---|---|---|---|---|---|---|
| suyou | 景蓝乌尔图储能电站 | 100 | 苏尼特500kV变电站 | 220 | 内蒙.景蓝乌尔图储能电站/220kV.1M | 内蒙.苏尼特站/220kV.1M;2M;4M;内蒙.苏尼特站/500kV.1M |
| hangjinqi | 悦杭独贵储能电站 | | 谷山梁500kV变电站 | 220 | 内蒙.悦杭独贵储能电站/220kV.1M | 内蒙.谷山梁站/220kV.1M–4M |
| siziwangqi | 景通四益堂储能电站 | | 杜尔伯特220kV变电站 | 110 | (无自有节点) | 内蒙.杜尔伯特站/220kV.1M–2M |
| gushanliang | 裕昭沙子坝储能电站 | 500 | 谷山梁500kV变电站 | 220 | 内蒙.裕昭沙子坝储能电站/220kV.1M | 内蒙.谷山梁站/220kV.1M–4M |
| bameng | 景怡查干哈达储能电站 | | 河套500kV变电站 | 220 | 内蒙.景怡查干哈达储能电站/220kV.1M | 内蒙.河套站/220kV.1M–4M |
| wulate | 远景乌拉特储能电站 | 100 | 德岭山500kV变电站 | 220 | 内蒙.远景乌拉特储能电站/220kV.1M | 内蒙.德岭山站/220kV.1M–4M |

## Upcoming (3 — for simulation study)

| asset_code | capacity | substation | conn_kv | fengxing_parent_nodes |
|---|---|---|---|---|
| alashan | 1000MW/4000MWh | 阿拉腾敖包500kV变电站 | 220 | (阿拉腾敖包站尚未出现在节点表) |
| wuchuan | 1000MW/4000MWh | 武川500kV变电站 | 220 | 内蒙.武川站/220kV.1M–4M; 500kV.1M–2M |
| xixier | 1000MW/4000MWh | 锡西二500kV变电站 | 220 | (锡西二站尚未出现在节点表) — 与苏右同一母站 |

## Per-asset notes & source documents

- **suyou (苏右=景蓝乌尔图, 100MW)** — 母站=**苏尼特变**: 2025-12-22 主接线图变电站清单中 "苏尼特变" 栏直接列有 "景蓝乌尔图100" (图面目读确认, 2026-09-01); 其价格交集亦含 苏尼特站/220kV.1M,2M,4M,500kV.1M — 一致. "苏右旗100万储能项目评审意见" 实为 upcoming 锡西二项目文件(锡西二站未建成), 与本资产无关. 运营数据自 2025-02-12.
- **hangjinqi (杭锦旗)** — 杭锦旗谷山梁电网侧智慧独立新型储能电站示范项目接入系统报告20240424.pdf: 远景储能220kV升压站打捆明阳储能, 约30km线路接入谷山梁变220kV侧. 运营数据自 2026-01-01.
- **siziwangqi (四子王旗)** — 关于乌兰察布四子王旗新型储能电站项目接入系统报告审定.pdf: 110kV升压站约2km接入杜尔伯特220kV变110kV侧 (T接杜塔线过渡→切改正式间隔). 无自有节点, 价格节点=杜尔伯特站220kV (价格聚类分析独立验证一致). 运营数据自 2026-01-01.
- **gushanliang (谷山梁)** — 谷山梁500千伏变电站电源侧独立储能电站子项目1、2--接入系统（收口）/: 子项目1(500MW) 0.1km串入子项目2升压站, 再约4km接入谷山梁变220kV侧. **裕昭沙子坝接入谷山梁变已经用户确认 (2026-09-01)**; 地图(Dec-2025)未标注(投运晚于图面). 运营数据自 2026-01-06.
- **bameng (巴盟)** — 25.041.…巴彦淖尔河套新型储能专项行动项目接入系统设计的评审意见(1).pdf + 巴彦淖尔河套…设计报告（3.17收口）.pdf: 约3km接入河套500kV变220kV侧. 运营数据自 2026-01-05.
- **wulate (乌拉特中旗)** — 乌拉特中旗德岭山新型储能电站项目接入系统报告0425.pdf: 100MW, 约1.5km接入德岭山500kV变220kV侧, 打捆国电投乌中旗钠离子储能. plant_name=远景乌拉特储能电站 — **已确认**: 2025-12-22 主接线图上 "德岭山变·远景乌拉特·巴能峰塔" 同组出现. 运营数据自 2025-01-22.
- **alashan (阿拉善)** — 阿拉善盟阿拉腾敖包新型储能专项行动项目（电网侧）接入系统报告2410.pdf: 约5km接入阿拉腾敖包500kV变220kV侧. 投产后以 cleared data plant_name 补全.
- **wuchuan (武川)** — 25.022.…评审意见.pdf + 关于转发…的通知.pdf + 呼和浩特武川…报告（修编收口稿）.pdf: 约10km接入武川500kV变220kV侧.
- **xixier (锡西二)** — 锡林郭勒盟苏尼特右旗锡西二站…接入系统报告1016.pdf + 24.263.…评审意见.pdf: 约5km接入锡西二500kV变220kV侧. **锡西二站在建/未建成 (用户确认 2026-09-01)** — 站与BESS均为新建; 投运后与suyou(景蓝乌尔图)同处二连/苏尼特价格区.

## Co-node / congestion-zone relationships

- **谷山梁 220kV bus zone**: hangjinqi + gushanliang (各自升压站计量, 相距数km-30km — 平日同价, 阻塞日可分叉, 2026-05-20 实证; 裕昭沙子坝接入谷山梁变已经用户确认) + 库布其凝光400MW光储 / 库布其洁能800MW光储 / 库布其云恒800MW光储 / 明阳(亚什图,把栅).
- **二连/苏尼特 zone**: suyou 在运于苏尼特变 (地图清单确认, 100MW) + xixier (upcoming, 锡西二站在建) — 投运后同区.
- 节点表中变电站多表记 (.1M–.4M) 对应不同母线/线路计量点, 阻塞时同站不同表价可分叉.

## Substation neighborhoods (from 内蒙古电网主系统图 2024-08-01, data/nodal/网架图/内蒙/)

Positional text extraction (130pt radius) — proximity on the diagram ⇒ probable electrical adjacency, but individual connections not yet visually verified. Vintage 2024-08: 锡西二/阿拉腾敖包 not yet on the map (在建/规划); BESS assets themselves also absent (all energized later).

| Substation | Neighboring plants/stations on map (curated) |
|---|---|
| 谷山梁变 | 库布其凝光/洁能/云恒(光储), 沙日召光伏, 亿利治沙, 朔方, 卓越, 度光, 河日恒, 沙拉告里, 门肯, 先导, 泰泽一站, 蒙泰韩家渠 |
| 杜尔伯特变 | 港建乌兰花风电, 席边河风光, 杜尔伯特(中广核)风电, 夏日(三峡)风电, 四子王, 清泉, 锡拉木伦, 布力格, 中光, 明杰汗乌拉 |
| 苏尼特变/锡西二 zone | 京东方楚鲁图, 乌日希勒, 深能那仁, 巴音塔拉, 伊林一场/二场, 二连协合, 环昕满都拉图, 二连恩和, 蒙科敖都, 天宏阳光, 乌日根, 中海油二连风电 |
| 河套站 | 京能伊力更, 国华川井, 获各琦, 乌后旗开闭站, 曙光变, 杭后, 布拉格, 厂汉 |
| 德岭山站 | 隆兴昌, 文更, 临河, 国合, 金风达茂, 兴顺西, 巴中, 新安, 金泉 |
| 武川站 | 东山永光伏, 南卜子, 天能久远, 国龙白山风电, 东方新能源西南壕, 风盛北疆, 恒润, 呼市抽水蓄能(4×300), 三圣太 |
| 可镇站 | 李汉梁风光, 红山, 黑沙兔, 三圣太, 义和美, 元山, 上秃亥, 北梁, 风后柜 |

Cross-validation: 港建乌兰花/席边河/李汉梁/国龙白山/东山永 all appeared in 景通四益堂's price cluster; 中海油二连/苏尼特站/玉龙站/涌泉站 in 景蓝乌尔图's — map, price analysis, and interconnection reports triangulate consistently.

## BESS clusters by zone (from 2025-12-22 主接线图 — has BESS plants by name)

The Dec-2025 map groups BESS stations with their parent substations. Direct answer to "what assets share a zone with each BESS":

| Zone | BESS on map (✓ = own fengxing node exists) |
|---|---|
| 谷山梁变 | 熠能沙壕✓, 熠储新荣✓, **悦杭独贵**✓, 博梁海纳✓, 星辰科创✓, 明阳亚什图✓ (+综能万成功✓ adjacent). PV siblings: 凝光/洁能/云恒/沙日召/正利光伏/亿利治沙/朔方/卓越/河日恒/夜鸣沙/波特尔/蓝晖#1,2/中节能众新 |
| 杜尔伯特变 | **景通四益堂** (map line: "杜尔伯特变…准兴纳兰·景通四益堂") — 第三重确认. 景通红丰 elsewhere (瑞升/化德 area — explains why 红丰≠四益堂 price zone) |
| 德岭山变 | **远景乌拉特**✓, 巴能峰塔✓ ("德岭山变·远景乌拉特·巴能峰塔" 同组) |
| 二连/苏尼特 — 苏尼特变 | **景蓝乌尔图**✓100MW, 蒙能吉博尔#1,2 (500+500), 蒙能卓拉#1,2 (500+500), 蒙能呼其 (500), 蒙能百利格✓ (450), 京能那木斯, 陶勒盖 + wind/PV: 京东方楚鲁图, 乌日希勒, 深能那仁, 巴音塔拉, 伊林一场/二场, 二连恩和, 蒙科敖都, 天宏阳光, 乌日根, 二连协合, 中海油二连, 宏晖二连微网 |
| 二连/苏尼特 — 努如变 | 蒙能亿和1号/2号 (500+500), 蒙能亿力齐1站/2站, 大航都林✓, 星能乌勒吉✓, 明阳浩来图, 环昕满都拉图, 沪能呼博, 嘉泽图和木, 塔林贡 |
| 可镇变 | 综能可镇✓ ("北梁·综能可镇·青城得胜沟") |
| 阿拉善 region (for upcoming alashan) | 蒙能奈伦✓, 蒙能敖伦✓, 蒙能百泉1/2号, 蒙能百湖#1,2, 蒙能冬青#1-4, 旭恒 |

Not on the Dec-2025 map (too new): 锡西二站, 阿拉腾敖包站, 裕昭沙子坝 (COD 2026-01), 景怡查干哈达 (COD 2026-01), 锡西二 BESS.
