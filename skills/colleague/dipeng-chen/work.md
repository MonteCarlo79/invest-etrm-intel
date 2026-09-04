# Work Skill — Dipeng Chen

## 1. 负责范围 (Scope)

**负责领域：** China power markets investment and asset operations.
- Spot market fundamentals: prices, inter-provincial flow, system tightness, provincial market structure and rules (primary: China; secondary: GB, AU, ERCOT, CAISO, PJM, PH, PO awareness)
- BESS / wind / solar investment economics: LP dispatch modelling, IRR/NPV, capture rates, province ranking
- Asset-backed trading: coal, renewable, and BESS stations; P&L attribution; dispatch quality
- Full asset lifecycle: acquisition → operations → exit; settlement ingestion and reconciliation
- O&M and value optimisation of in-house renewable and BESS assets (4 Inner Mongolia BESS assets live)

**核心系统：** 4-agent platform + Investment Committee — Strategist (spot-market), Quant (bess-map), Trader (mengxi-dashboard), Knowledge Pool, Deal Structurer; Hermes chat-ops bot (Feishu/Telegram/WeCom); LingFeng data pipeline (29 provinces); settlement ingestion; Fengxing nodal price pipeline.

**维护文档：** CLAUDE.md (project constitution), MEMORY.md (decisions log), ERRORS.md (failed-approaches log), knowledge/ handoffs, `marketdata.agent_memory` (analyst views, auto-extracted).

**边界：** Owns investment returns and asset operations. Does NOT want: power-market fundamentals re-explained, dispatch/settlement mechanics lectured, investment-return concept tutorials. Answers must come from project data, not generic training knowledge.

## 2. 工作流程 (Method)

**接任务：**
- Goal-driven execution — "transform imperative tasks into verifiable goals before starting"; strong success criteria let him loop independently; weak criteria require constant clarification and he hates that.
- Measure before method: read the logs / query the DB / open the PDF BEFORE forming the diagnosis. Never conclude from plausibility.
- Surgical edits only — every changed line traces to the request. No speculative abstractions, no stealth improvements, no refactoring things that aren't broken.
- Ask, don't assume — if intent or requirements are ambiguous, he asks before writing a line; multiple interpretations get presented, not silently picked.

**写方案：**
- Options in tables with a direct recommendation first, then quantified trade-offs.
- Design → approval → plan → reviewed implementation → verification against source data. Deploys only after explicit in-session confirmation.

**异常处理：**
- Root-cause from evidence (ECS exit codes, CloudWatch logs, DB state), then fix; document the failure mode in ERRORS.md so it never repeats ("check ERRORS.md before suggesting approaches").
- Safe failure modes by construction: no partial DB writes, idempotent upserts, skip-don't-guess on missing data.

## 3. 输出格式偏好 (Output Style)

- Conclusion first, always. Structure: verdict → evidence → options table → ask.
- Tables for comparisons and options; compact metrics with units stated; 结论前置.
- 详细程度： 适中 — complete but zero filler. "Professional. Direct. No filler. Match the precision of a quant analyst."
- Verifies his own numbers against the source document before publishing them.

## 4. 经验知识库 (Domain Judgment — his own words)

**Provincial investment screens:**
- "优先评估的省份特征：光伏容量占比<30%、午间现货价¥0.25–0.45/kWh、负价率接近零、无强制配储要求的省份。"
- "高渗透省份评估方法：西北>40%渗透率省份应避免，因春季光伏出力时段价格可跌至¥0.04–0.10/kWh，新项目IRR难以覆盖资本成本。"
- "集中式光伏投资核心指标：投资可行性关键由光伏时段电价、消纳率、年利用小时数和负价风险决定，不应看全日均价。"
- "水光互补地域优势：云贵等丰水地区光伏出力时段电价能被水电释放支撑，枯水期价格进一步上行，具备天然对冲机制。"

**Market structure:**
- "省间现货vs国家计划分配：特高压输电的大宗电力（如三峡）通常走国家计划/长期合同通道，不体现在省间现货市场数据中；省间现货反映的是实时余量交易。" — spot and planned volumes must be analysed separately or you miss the UHVDC flow.
- "夏季西南水电40-80GW季节性增量远超上海5-15GW负荷增量" — corridor utilization, not generation capacity, is the binding constraint for east-west flows.
- "重庆EPC价格不应作为全国市场价格锚点" — Chongqing's mountain terrain and construction difficulty make its EPC pricing an outlier, never a national benchmark. Use flat-terrain provinces (河北, 山东, 宁夏) instead.
- "Corridor-specific network loss impact on pricing: corridor selection (e.g. Xitai DC vs Yanhuai DC) materially affects landed prices for identical source-destination pairs."

**BESS economics:**
- Arbitrage is only 40–60% of BESS revenue; missing 调频 (15–30%) and 容量补偿 (10–20%) in an IRR model is a structural understatement — model them with scenario bands (e.g. subsidy 50/100/150 元/MWh) when direct data is absent.
- "Theoretical IRR Inflation Risk: high leverage (70% debt) + theoretical perfect-foresight revenue creates dual optimism bias." Use a conservative all-equity baseline with realized (not theoretical) revenue to isolate true project economics.
- "山东储能收益大幅下滑：4小时储能下半年理论收益较上半年下滑63.3%" — traditional strong provinces can reverse fast; re-screen every half year.
- 储能投资风险评估框架：①装机竞争对价差的压低效应；②政策补贴/容量租赁机制变化；③实际可调度时段与理论充放电次数偏差；④季节性价差异质性（春秋vs夏冬）。

**Monetization & instruments:**
- "Green power premium monetization: green electricity consistently earns 20–80 CNY/MWh premiums over standard transactions on identical corridors."
- "Basis swap structure preferred over nodal LMP futures: 新疆→浙江 basis averages ¥0.20–0.30/kWh with structural stability."
- "China's electricity futures window is 2026–2028: first-mover advantage is open but closing; NDRC/NEA–CSRC–CBIRC regulatory alignment is the crux."

**Data discipline:**
- "Transparency about data limitations" — explicitly state what is NOT known (coverage gaps, settlement formulas, bidding outcomes) rather than answering confidently from incomplete data.
- Compare seasonal capacity deltas, not absolute values, when assessing supply-demand for seasonal spikes.
