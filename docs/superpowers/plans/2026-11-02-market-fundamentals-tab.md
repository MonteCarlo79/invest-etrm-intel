# Market Fundamentals Tab 重做 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** spot-market 的 Market Fundamentals tab 原地重做:省份纵深四段(分技术装机/分电源发电量/分类型利用小时/用电需求)带 2025 年度→2026 逐月时间演变,数据落新表 `marketdata.province_fundamentals_monthly`,写入走确定性解析器注册表 + Intake/RECORD_FACT 路由。

**Architecture:** 长表(同构 province_storage_pipeline 已验证模式)+ 每省注册式解析器(结算单 parser 套路,真实文件打印值为 oracle)+ tab 读 DB(双环境)。Spec 已定案:确定性解析为主、vision 兜底;全国基准仅 2025 年度;Quant 工具二期。

**Tech Stack:** Python 3.13, openpyxl, pandas, psycopg2/SQLAlchemy, Streamlit + Plotly, pytest。

**Spec:** `docs/superpowers/specs/2026-11-02-market-fundamentals-tab-design.md`

## Global Constraints

- 新表 DDL: 独立 `CREATE UNIQUE INDEX`(COALESCE 表达式不进表约束 — 照抄 `services/knowledge_pool/intake_routes.py` 模式)
- 读取规则: 仅 `status='confirmed'`,`DISTINCT ON (province, month, period, section, metric) ORDER BY created_at DESC`;同键偏差 >0.5% 标 `conflict` 不覆盖
- 单位固定: 装机=万千瓦, 电量=亿千瓦时, 利用小时=小时, 同比=%;源是万元/万千万时换算在解析器内完成
- **口径分离:** 省调口径与全口径用不同 metric 名(`cap_total` vs `cap_total_full`),不混存同 metric(安徽 2025-01 两源实测: 6,831 vs 12,636 万千瓦)
- 累计值差分成当月值时 `source` 加 "(差分)";1 月累计值=当月值,不得减上年 12 月
- spot-market app 双语: 所有新 UI 文案走 `_t()` 字典(EN/ZH)
- 提交纪律(共享 OneDrive 树): 每个 task 完成即提交+推送;`git status` 前查暂存外来内容;首选对象级 plumbing 或 ~/bess-push 克隆(本地 .git 状态不稳定时)
- hermes/spot-market 部署均需用户当 session 明确 yes

## Review Focus

1. **累计差分跨年错减:** 2026-01 累计值差分时若误减 2025-12,1 月会变负/扭曲 — 期望: 1 月当月值=累计原值(测试钉死, T2/T3)
2. **口径混存:** 同省同月两个总装机(省调 6831 vs 全口径 12636)若存同 metric,读取端随机取行 — 期望: 不同 metric 名,永不冲突(store 测试, T1)
3. **单位错配:** 某省表用"万千瓦",另一省用"MW"或"亿千瓦时 vs 万kWh" — 期望: 解析器显式断言单位,不归一就报错不猜(T2/T3/T4)
4. **重申值(restated):** 同 (省,月,metric) 后到的月度文件修正前值 — 期望: 偏差 ≤0.5% 覆盖更新,>0.5% 标 conflict 且保留旧行(store 测试, T1)
5. **空单元格/NaT:** 月报表里空 A2、空日期行(_sheet_date NaT 前科)— 期望: 跳过不崩,计入 skipped 计数(T2/T3)

---

### Task 1: 数据表 DDL + store 读写层

**Files:**
- Create: `db/ddl/marketdata/province_fundamentals_monthly.sql`
- Create: `services/province_fundamentals/__init__.py`(空)、`services/province_fundamentals/store.py`
- Test: `tests/province_fundamentals/test_store.py`

**Interfaces:**
- Produces:
  - `write_rows(rows: list[FundRow], *, pg_url: str, default_status: str = "confirmed") -> dict`(返回 `{"written": n, "conflicts": n, "skipped": n}`;>0.5% 偏差不覆盖、写 `status='conflict'` 新行)
  - `read_series(province: str, section: str, metrics: list[str] | None = None, *, pg_url: str, months: int = 24) -> list[dict]`(confirmed-only、DISTINCT-ON 最新、含 period 字段)
  - `FundRow` dataclass: `province, month(date), period("month"|"year"), section, metric, value(float|None), unit, source`(`services/province_fundamentals/models.py`)

- [ ] **Step 1: 写失败测试 — 口径分离与冲突规则**

```python
def test_same_key_different_scope_metrics_coexist():
    # 安徽 2025-01: 省调 cap_total=6831.49 与全口径 cap_total_full=12635.64 必须共存
def test_restatement_within_half_pct_overwrites():
    # 同键同 source 族: 6831.49 → 6831.52 (0.001%) → written+1, 无 conflict
def test_restatement_beyond_half_pct_marks_conflict():
    # 6831.49 → 6900.0 (0.97%) → 旧行保留, 新行 status='conflict', conflicts+1
```

- [ ] **Step 2: 跑测试确认失败**(`pytest tests/province_fundamentals/test_store.py -v` → FAIL,模块不存在)

- [ ] **Step 3: 实现**

`store.py`:`write_rows` 用 `psycopg2.extras.execute_values`(批量,ERRORS.md 教训)+ 逐行预查 confirmed 现值做 0.5% 判定;`read_series` 用 DISTINCT ON。DDL 文件照 spec 的列清单 + 独立唯一索引;`init.sql` 风格与 `db/ddl/marketdata/province_fr_market.sql` 一致。

- [ ] **Step 4: 跑测试确认通过**

- [ ] **Step 5: 提交**

```bash
git commit -m "Add province_fundamentals_monthly store layer + DDL" -- db/ddl/marketdata/province_fundamentals_monthly.sql services/province_fundamentals/ tests/province_fundamentals/
```

---

### Task 2: 解析器注册表 + 格式 A(月报数据累计工作簿)解析器

**Files:**
- Create: `services/province_fundamentals/registry.py`、`services/province_fundamentals/parsers/__init__.py`、`services/province_fundamentals/parsers/format_a.py`
- Test: `tests/province_fundamentals/test_format_a.py`、`tests/province_fundamentals/test_registry.py`

**Interfaces:**
- Consumes: Task 1 的 `FundRow`
- Produces:
  - `registry.parse_file(path: Path) -> list[FundRow]`(按文件名/省份 sheet 结构嗅探分派;未知格式返回 [] 并 log)
  - `registry.register(province: str, parser: Callable)`;`registry.known_formats() -> list[str]`
  - 格式 A 识别特征: sheet 名含 "装机及发电情况",首行表头含 "月份" 与 "省调口径装机总容量"

**实测真值(安徽 `安徽电力市场信息披露月报数据-截至2026年5月.xlsx`, fixture 断言用):**
`2025-01 | 总 6831.49 | 火电 4321.6 | 水电 55.41 | 风电 905.31 | 光伏 1307.29 | 储能 241.88 | 累计发电量 239.04`

- [ ] **Step 1: 写失败测试**

```python
def test_format_a_anhui_2025_01_capacity_rows():
    rows = parse_format_a(Path("data/exchange-monthly-reports/安徽月报/安徽电力市场信息披露月报数据-截至2026年5月.xlsx"), "安徽")
    by = {(r.month, r.metric): r.value for r in rows}
    assert by[(date(2025,1,1), "cap_total")] == pytest.approx(6831.49)
    assert by[(date(2025,1,1), "cap_thermal")] == pytest.approx(4321.6)
    assert by[(date(2025,1,1), "cap_storage")] == pytest.approx(241.88)

def test_format_a_january_generation_is_cumulative_value_not_diff():
    # 2025-01 gen_total 必须 == 239.04(累计原值), 不得减上年12月
```

- [ ] **Step 2: 验证失败** → **Step 3: 实现**(openpyxl read_only;表头按名称匹配列,不按下标;累计发电量差分为当月值,1 月原值直取,`source` 加 "(差分)";空行/NaT 跳过并计数)→ **Step 4: 验证通过** → **Step 5: 提交** `git commit -m "Add province fundamentals parser registry + format A parser" -- <paths>`

---

### Task 3: 格式 B(N-省装机-月)解析器 — 利用小时月度

**Files:**
- Create: `services/province_fundamentals/parsers/format_b.py`
- Test: `tests/province_fundamentals/test_format_b.py`

**Interfaces:**
- Consumes: Task 1 `FundRow`
- Produces: `parse_format_b(path: Path, province: str) -> list[FundRow]`(识别特征: sheet 名含 "利用小时数数据",表头含 "年月" + "指标名称" + "期末发电设备容量";利用小时列在右半部分,按表头名匹配,缺则用 发电量÷期末容量 推算并标 `(推算)`)

**实测真值(安徽 `2-安徽装机-7月.xlsx`):**
`2025-01 总计: 期末容量 12635.64 万千瓦, 本月发电量 299.53 亿千瓦时, 同比 -14.39%`;`火电: 6298.21 / 242.21 / -20.69%`

- [ ] **Step 1: 失败测试** — 总计行 `cap_total_full`(全口径!)、`gen_total`(当月值 299.53,非差分 — 格式 B 本身给当月)、`demand_yoy` 类推同比列、指标名映射(总计/水电/其中：抽水蓄能/火电/燃煤机组/风电/光伏 → 词表;抽水蓄能并入 `cap_hydro`?— **否: 抽蓄单列 `cap_pumped`**,词表加一条)
- [ ] **Step 2-5:** 同 Task 1 节奏;提交 `git commit -m "Add format B parser (monthly utilization hours)" -- <paths>`

---

### Task 4: 交接表 2025 年度解析器(利用小时+用电量+全国发电量 sheet)

**Files:**
- Create: `services/province_fundamentals/parsers/jiaojie_2025.py`
- Test: `tests/province_fundamentals/test_jiaojie_2025.py`

**Interfaces:**
- Consumes: Task 1 `FundRow`;`period="year"` 行,`month=YYYY-01-01`
- Produces: `parse_jiaojie(path: Path) -> list[FundRow]`(三个 sheet:`2025各省年度利用小时数` → hours_* + hours_*_yoy;`全社会用电量` → demand_total + demand_p1/p2/p3/residential;`全国发电量` → gen_* 年度 + cap_*_full 年度 + hours_total)

**实测真值:** `华北|山东|火电|3997.03`;`华东|安徽|总计|2724|-389`;`广东 全社会用电量合计 9589.73 亿千瓦时`;全国发电量 sheet `山西省 4405.8(累计发电量 亿千瓦时)…`

- [ ] **Step 1: 失败测试** — 山东火电年度 3997.03(period='year', month=date(2025,1,1));安徽总计 2724 + yoy -389(两行: hours_total=2724, hours_total_yoy=-389);广东 demand_total=9589.73
- [ ] **Step 2-5:** 同节奏;提交 `git commit -m "Add jiaojie 2025 annual parser" -- <paths>`

---

### Task 5: 回填 runner

**Files:**
- Create: `services/province_fundamentals/backfill.py`
- Test: `tests/province_fundamentals/test_backfill.py`

**Interfaces:**
- Consumes: T1 `write_rows`,T2–T4 解析器
- Produces: CLI `python -m services.province_fundamentals.backfill --dir data/exchange-monthly-reports [--province 安徽] [--dry-run]`;`--dry-run` 只打印每文件判定(格式/行数/跳过原因)不写库;写库经 `write_rows(default_status="confirmed")`

- [ ] **Step 1: 失败测试** — dry-run 不触碰 `write_rows`(mock 断言未调用);未知格式文件计入 `unknown` 不崩
- [ ] **Step 2-5:** 实现(遍历目录 → registry 分派 → 汇总计数打印;RDS 不通时的 Fargate 包装放进部署任务,不在此 task);提交 `git commit -m "Add province fundamentals backfill runner" -- <paths>`

---

### Task 6: Tab UI 重做(apps/spot-market/app.py)

**Files:**
- Modify: `apps/spot-market/app.py`(fundamentals tab 段)
- Test: `apps/spot-market/tests/test_fundamentals_tab.py`(新建目录如不存在)

**Interfaces:**
- Consumes: T1 `read_series`(经 `@st.cache_data(ttl=300)` 包一层 loader)
- Produces: tab 结构供部署验证 — 见下

**布局(tab 内,不动共享侧边栏;沿用 `_t()` 双语):**
1. tab 内 selectbox 选省(默认"安徽",候选=表内有数据的省 + 全国)
2. S1 分技术装机: 年度卡(2025 各 tech 值)+ 2026 逐月堆叠面积图 + 全国年度增速基准窄条
3. S2 分电源发电量: 同上结构
4. S3 分类型利用小时: 2025 逐月(交接表)+ 2026 逐月(格式 B);推算值虚线样式区分
5. S4 用电需求: 逐月柱 + 同比折线双轴;年度产业拆分横向条
6. 缺失月/省: 灰标 "数据缺失";既有 ranking/tightness 段原样保留在四段之后

- [ ] **Step 1: 失败测试** — loader 函数对空表返回空结构不崩;`AppTest` 冒烟(模块级执行顺序,沿用 v60 教训的 def-order 守卫思路:tab 代码不得在任何 def 之前调用它)
- [ ] **Step 2-5:** 实现 → 本地 `streamlit run apps/spot-market/app.py --server.port 8505` 目视验证(用户来做)→ 提交 `git commit -m "Rebuild market fundamentals tab with monthly evolution" -- apps/spot-market/`

---

### Task 7: Intake 路由 `fundamentals_stat`(注册表优先)

**Files:**
- Modify: `services/knowledge_pool/intake_extract.py`(ROUTE_TYPES + prompt 路由说明)、`services/knowledge_pool/intake_routes.py`(`write_fundamentals_draft`)
- Test: `tests/knowledge_pool/test_intake_fundamentals.py`

**Interfaces:**
- Consumes: T1 `write_rows`,T5 `registry.parse_file`
- Produces: `write_fundamentals_draft(*, province, rows: list[dict], source) -> str`(draft 状态写入;skip-guard 复用 `_write_rate_draft` 的 confirmed/superseded 保护);intake 流程中文件命中注册格式时优先调解析器,未命中才走 vision

- [ ] **Step 1: 失败测试** — vision 提案含 `fundamentals_stat` 路由时写入 draft;confirmed 行不被 draft 覆盖(skip-guard)
- [ ] **Step 2-5:** 实现 → 提交 `git commit -m "Add fundamentals_stat intake route (parser-registry first)" -- <paths>`

---

### Task 8: RECORD_FACT 新 fact_type `fundamentals_stat`

**Files:**
- Modify: `services/hermes/record_fact.py`(FACT_TYPES、schema enum、prompt、write_fact_drafts 分支、pending_writes_for)
- Test: `tests/hermes/test_record_fact.py`(追加)

**Interfaces:**
- Consumes: T7 `write_fundamentals_draft`
- Produces: `fact_type="fundamentals_stat"` 时调用 `write_fundamentals_draft`;pending key = `(province, month, section, metric, source)`

- [ ] **Step 1: 失败测试** — `记一下:安徽6月全社会用电量同比+8%` → demand/demand_yoy draft 路由;`other` 仍跳过
- [ ] **Step 2-5:** 实现 → 提交 `git commit -m "Add fundamentals_stat fact_type to RECORD_FACT" -- <paths>`

---

### Task 9: Strategist 工具 `get_province_fundamentals`

**Files:**
- Modify: `services/spot_mcp/tools.py`、`apps/spot-market/app.py`(agent 工具注册与 prompt 框架行)
- Test: `tests/spot_mcp/test_tools.py` 追加(目录如已有)

**Interfaces:**
- Consumes: T1 `read_series`
- Produces: `get_province_fundamentals(province: str, section: str | None = None, months: int = 24) -> dict`(grounding 规则不变 — 只回表内数据,无数据直说)

- [ ] **Step 1: 失败测试** — 空省返回 `{"rows": [], "note": "无数据"}` 而非编造
- [ ] **Step 2-5:** 实现 → 提交 `git commit -m "Add get_province_fundamentals tool to Strategist" -- <paths>`

---

### Task 10: 部署(每一步都要用户当 session yes)

- [ ] **Step 1: RDS DDL 应用** — `db/ddl/marketdata/province_fundamentals_monthly.sql`(幂等,可重复执行验证)
- [ ] **Step 2: 回填** — RDS 连通则本地 `backfill --dir data/exchange-monthly-reports`(先 `--dry-run` 给用户看判定表);不通则 Fargate run-task(脚本 base64 模式)
- [ ] **Step 3: spot-market 镜像** — v50(ditto 暂存、mirror Dockerfile、amd64、prove、push verify、jq-swap 自当前 td、manifest 追加)
- [ ] **Step 4: hermes 镜像** — 与下次 hermes 变更合并发(RECORD_FACT fact_type);单独发亦可,用户定
- [ ] **Step 5: 验证** — 安徽 tab 四段实数据渲染 + Strategist 问"安徽装机结构"看工具调用;LATEST-IMAGES.md 行+历史更新

---

## Self-Review 记录

- Spec 覆盖: 四段(T6)、三条写入路径(T5/T7/T8)、agent 消费(T9)、DDL(T1/T10)、解析器策略(T2–T4)、全国年度基准(T6 基准条,数据源=交接表/版图年度行 — 交接表解析在 T4)、时间轴自动滚动(T6 按日历取上年度+今年)
- 版图 2609 解析器:**v1 不做**(我的拍板) — 其装机/发电与交接表重复,GDP/度电产值不在四段内;spec 数据源表保留备查
- 类型一致性: `FundRow`/`write_rows`/`read_series`/`parse_*` 签名跨 T1–T9 一致
- 比例: 计划约为 spec 的 1.6 倍,代码块只含钉真值的测试断言
