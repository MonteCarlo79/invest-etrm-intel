# Market Fundamentals Tab 重做 — 设计文档

**日期:** 2026-11-02 · **状态:** 待用户评审
**背景会话:** strategist(spot-market)

## 意图与为什么

单省调研的基本面纵深视图(安徽 pack 模式的常态化):选定一个省,一眼看到该省供需基本盘的**时间演变**(2025 年度 → 2026 逐月;进入 2027 自动切 2026 年度 + 2027 逐月)。

更进一步:基本面层是后续电价/政策分析的解释底座 — "为什么某省价差大、为何容量电价高"这类问题,需要 agent 能把基本面与价格/政策数据 JOIN 起来。所以数据模型必须 agent 可消费,不只是给人看图。

## 目标

1. Tab 四段,每段带时间演变:
   - **S1 分技术装机**(万千瓦):火电/水电/核电/风电/光伏/储能,年度 2025 + 逐月 2026
   - **S2 分电源发电量**(亿千瓦时):同口径
   - **S3 分类型利用小时**:2025 逐月(交接表)+ 2026 逐月(月报);年度行带同比
   - **S4 用电需求演变**:逐月用电量 + 同比;年度产业拆分(一产/二产/三产/居民)
2. 省份覆盖:全部 32 省月报目录;机器可读(xlsx)才进,PDF-only/缺月份标注"数据缺失"
3. 全国基准条:该省 vs 全国(装机增速/用电增速),只做窄条对比,不做主画布
4. 时间轴自动滚动:按当前日历自动选"上年度+今年逐月",无需手工维护

## 非目标 / 反目标

- **不解析 PDF**(本期):PDF-only 的省/月显示缺失标记;PDF 抽取是后续独立项目
- 不做全国对比主视图(现有 ranking/tightness 表保留原位即可)
- 不为本期新建 RAG/向量检索 — 走结构化表
- 不破坏现有 tab 的系统裕度(system tightness)等已有段落 — 保留,新内容在其上方

## 数据源实勘(2026-11-02,已读原文件验证)

| 源 | 内容 | 形态 |
|---|---|---|
| `data/market-fundamentals/【！飔合资料交接】/交易数据/2025年1-12月各省分类型利用小时.xlsx` | `2025各省年度利用小时数`(省×类型 年度+同比)、`全社会用电量`(省×产业)、`全国发电量`(省×电源 发电量+装机+年度小时)、`2025各省分类型年度利用小时数`;分省月度 sheet **大多空白**(待查询省份 tracker) | 单文件多 sheet,整洁 |
| `data/market-fundamentals/2025年全国电力版图（CN）（更新版）2609.xlsx` | 用电量+GDP+度电产值、装机前十排名 | 年度快照 |
| `data/exchange-monthly-reports/<省>月报/*月报数据*.xlsx`(如安徽) | 装机及发电情况:月份×(总/火/水/风/光/储装机 + 累计发电量),2025-01 起连续;另有交易/结算/用电侧 sheets | 分省 xlsx,格式 A(多省类似) |
| 同目录 `N-<省>装机-<月>.xlsx`(如安徽 7 月) | 发电生产汇总:年月×指标(总计/水电/抽蓄/火电/燃煤/风电/光伏)×期末容量+本月发电量+同比 | 分省 xlsx,格式 B |
| `data/market-fundamentals/各省现货价格及边界数据/` | 分省边界 xlsx(现货边界条件) | 备查 |
| DB `marketdata.province_installed_monthly` | 已有 BESS/风/光月度装机(auto-ETL) | 存量,保留不动 |

**结论:** S1/S2/S4 的 2025→2026 逐月由各省"月报数据"类 xlsx 供给(格式 A 为主);S3 由交接表(2025)+ 各省"装机-N月" xlsx(格式 B,2026)供给。

## 数据模型(新建一张表)

```sql
marketdata.province_fundamentals_monthly (
    id SERIAL PRIMARY KEY,
    province TEXT NOT NULL,
    month DATE NOT NULL,             -- 月初;年度行 = YYYY-01-01
    period TEXT NOT NULL DEFAULT 'month',   -- month | year
    section TEXT NOT NULL,           -- capacity | generation | util_hours | demand
    metric TEXT NOT NULL,            -- 见词表
    value NUMERIC,
    unit TEXT NOT NULL,              -- 万千瓦 | 亿千瓦时 | 小时 | %
    source TEXT NOT NULL,            -- 文件名或 hermes-record-fact-YYYYMMDD
    status TEXT NOT NULL DEFAULT 'confirmed',   -- confirmed | draft | conflict
    created_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE UNIQUE INDEX ... ON marketdata.province_fundamentals_monthly
    (province, month, period, section, metric, COALESCE(source,''));   -- 独立表达式索引
```

**metric 词表(节选):**
- capacity: `cap_total_mw10`… 统一存万千瓦: `cap_thermal / cap_hydro / cap_nuclear / cap_wind / cap_solar / cap_storage / cap_total`
- generation: `gen_thermal / gen_hydro / gen_nuclear / gen_wind / gen_solar / gen_total`(亿千瓦时,本月值;源给累计则当月差分)
- util_hours: `hours_thermal / hours_hydro / hours_nuclear / hours_wind / hours_solar / hours_total`(小时)+ `_yoy` 后缀存同比 %
- demand: `demand_total`(亿千瓦时)+ `demand_yoy`(%)+ `demand_p1 / demand_p2 / demand_p3 / demand_residential`(产业拆分,亿千瓦时)

**年度行的存法(builder-check 定案):** 加 `period TEXT NOT NULL DEFAULT 'month'` 列,年度行用 `period='year'` + `month=YYYY-01-01`(与月度行不撞键;唯一键含 period)。年度值来源:交接表/版图(2025);2026 年度卡到年底由月度行加总派生(覆盖率不足时标"推算"),不预存。
**S3 缺月兜底(定案):** 该月无小时数源时,由 发电量÷平均装机 推算,`source` 标注 "(推算)";无可推算则缺失灰标。
**全国基准条(用户定案 2026-11-02):** 只用 2025 年度全国数(交接表+版图);省 vs 全国年度同比,月度全国基准待有源再补。
**唯一键语法(定案):** COALESCE 表达式键用独立 `CREATE UNIQUE INDEX`(照抄 intake_routes 模式),不写在表约束里。

长表(与 `province_storage_pipeline` 同构,该模式已被 intake/巡逻/agent 验证)。宽表被否:各省可报指标不齐,长表稀疏友好。与存量 `marketdata.province_fundamentals`(年度宽表,两 app 在用)不合并 — 新表是月度正典,宽表维持现状。

## 写入路径(三条)

1. **一次性回填(本地脚本):** 解析交接表 + 版图 + 各省月报 xlsx → 上表。RDS 从 Mac 被墙时走 Fargate run-task(既定模式)。**提取策略(用户定案 2026-11-02):确定性解析器为主** — 各省月报版式互不相同(builder-check 实证:山东 21 sheet、蒙西/河南/辽宁各一套、江苏装机独立文件),但每省格式逐月稳定 → 每省一次性写注册式解析器(`services/province_fundamentals/`,结算单 parser_guangxi 同款套路,真实文件的打印合计为测试 oracle),写一次终身复用,后续月度文件同格式自动吃;未知/一次性格式走 vision 兜底。首批按调研优先级排序,节奏 3–5 省/天。
2. **Intel Intake 新路由** `fundamentals_stat`:月报/披露文件走 spot-market Intel Intake。**注册表优先:**上传文件命中已知省份格式时调确定性解析器(数值零误读),未命中才走 vision 提取 → review → 写入(draft→确认)。复用 intake_routes 的 skip-guard 模式。
3. **RECORD_FACT 新 fact_type** `fundamentals_stat`:"记一下:安徽6月全社会用电量同比+8%" → 写 demand 段 draft → 「确认」翻 confirmed。

## 读取规则与冲突(builder-check 定案)

- 读取:`DISTINCT ON (province, month, section, metric) ORDER BY created_at DESC`,仅 `status='confirmed'`(与 bess-map 约定一致)
- 同键新值与已确认值偏差 >0.5% 时,写入侧标 `conflict`(不覆盖),UI/巡逻可见
- 回填直接写 `confirmed`(来源为原始文件,人工校验过解析器)

## Tab UI(spot-market 现有 Market Fundamentals tab 原地重做)

省份选择器(**tab 内 selectbox**,不动共享侧边栏;默认安徽或最近调研省)→ 四段:
- 每段:年度卡(2025 值 + 同比)+ 逐月堆叠柱/面积图(2026 至今)+ 缺失月灰标
- 全国基准窄条:该省装机增速/用电增速 vs 全国(**2025 年度口径**)
- 时间轴说明:当前显示"2025 年度 + 2026 逐月";2027 起自动滚动
- 现有 ranking/tightness 段落原样保留在四段之后

## Agent 消费

- **Strategist** 新工具 `get_province_fundamentals(province, section, months)` → 读上表(本期)
- **Quant(bess-map)同名工具推迟到二期**(builder-check 指出部署清单遗漏 bess-map;先 Strategist 支撑调研用例,Quant 随 bess-map 下次部署窗口带上)
- 价差/容量电价的归因分析("为什么山东价差大")由 agent 侧组装: fundamentals + capture + 政策

## 测试

- 解析器:对实勘过的真实文件行做 fixture 断言(安徽 2025-01 装机 6831.49 万千瓦等)
- 写入:draft/confirmed/skip-guard 复用 intake 测试模式
- Tab:AppTest 冒烟(模块级执行顺序类崩溃防护,沿用 v60 教训)
- 表 DDL:幂等迁移

## 部署

1. DDL 上 RDS(需用户确认)
2. 回填脚本(Fargate 或本地视 RDS 连通性)
3. spot-market 新镜像(tab + intake 路由)
4. hermes 新镜像(RECORD_FACT fact_type)— 可与下次 hermes 变更合并

## 我自己拍板的点(评审时可驳回)

- 长表而非宽表(稀疏友好 + 与 storage_pipeline 同构)
- 原地重做而非新 tab(用户说"complete the tab")
- 月度值优先存"当月值";源只给累计时由解析器差分并标注 `source` 含 "(差分)"
- PDF 不解析,缺口标注而非硬抠
- `province_installed_monthly` 不动(存量 auto-ETL 继续喂它;新表是全燃料口径的正典)
