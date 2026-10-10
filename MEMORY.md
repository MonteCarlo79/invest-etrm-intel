# MEMORY.md — bess-platform

Read this at the start of every session before doing anything.

---

## 2026-05-06, Spot Market App Architecture

**What was decided:** `apps/spot-market/app.py` is the canonical Pillar 1 (Market Map) app. The old `apps/spot-agent/` china-spot app is retired and deleted. The new app runs on ECS at `/spot-markets` (port 8505, ECR repo `bess-spot-markets`).

**Why:** The new app is a 10-tab cockpit with agent, MCP, geo maps, inter-provincial flow, market fundamentals, load factor, system tightness, and bilingual (EN/ZH) support.

**What was rejected:** Keeping the old app alongside the new one — unnecessary duplication.

---

## 2026-05-06, Bilingual Support Architecture

**What was decided:** Language toggle (English / 中文) in the left sidebar drives all UI label translations via a `_t()` dict lookup. AI-generated summaries (Province Deep-Dive) are translated lazily on demand — per-summary `🌐 翻译` button inside each expander, result cached in `st.session_state["translated_summaries"]`. Agent responds in Chinese when Chinese mode is active.

**Why:** Auto-translating all summaries on language switch caused 30–60s page freeze (sequential API calls before first render). Lazy per-summary translation is instant for the page and ~2s per summary on demand.

**What was rejected:** Batch auto-translate on language switch (caused full-page freeze); `@st.cache_data` spinner approach (caused rerun loops).

---

## 2026-05-06, Geo Map Animation Anti-Pattern Fix

**What was decided:** Use `_anim_loop_rerun` session state flag to control the geo map animation loop. Animation only continues if the rerun was explicitly triggered by the animation itself. Any other user interaction stops the animation.

**Why:** `time.sleep() + st.rerun()` inside tab code runs on every Streamlit rerender (all tab code always executes), causing an infinite rerun loop that greys out the entire app when `anim_playing = True`.

**What was rejected:** Any approach that calls `st.rerun()` unconditionally inside tab render code.

---

## 2026-05-06, Market Fundamentals Tab

**What was decided:** New tab in spot-market app parsing `data/market-fundamentals/2023-2025 全国各省电力市场基础信息汇总2026-03-30.xlsx`. Displays: installed capacity (donut/stacked bar), generation mix, renewables share, peak load ranking table (both years), load factor by fuel type (%), and system tightness ranking.

**Why:** Agent needs structured access to market fundamentals to form complete investment picture. Visual display supports province comparison and ranking.

**What was rejected:** Embedding raw Excel data in the agent prompt — too large and unstructured.

---

## 2026-05-06, System Tightness Definition

**What was decided:** Effective capacity = Σ(Installed capacity 万kW × 10 MW × Standard EOH / 8760). Standard EOH: Wind 2000h, Solar 1100h, Thermal 5500h, Hydro 3500h, Nuclear 7500h. Storage excluded. Tightness = Effective capacity minus avg demand (= total generation / 8760) and minus summer/winter peak. Sorted tightest-first (ascending).

**Why:** Reflects the Chinese power system planning convention. Blended thermal EOH (5500h) used since 火电 data combines coal and gas.

**What was rejected:** Using pandas Styler for colour coding — version-sensitive (`applymap` → `map` in pandas 2.1+); replaced with plain `+`/`−` prefixed string formatting.

---

## 2026-05-06, Spot Market App — Full Reference

### Current deployed version
`bess-spot-markets:v13` on ECS service `bess-platform-spot-markets-svc`, task definition `bess-platform-spot-markets:13`

### Tab structure (10 tabs in order)
| # | Key | Title (EN) | What it shows |
|---|-----|-----------|---------------|
| 1 | `tab_overview` | Overview | DA/RT price time series, KPI strip, latest prices table |
| 2 | `tab_spread` | DA–RT Spread | Spread analysis by province and period |
| 3 | `tab_heatmap` | Heatmap | Province × time heatmap of DA or RT prices |
| 4 | `tab_province` | Province Deep-Dive | Province-level detail + AI market summaries (translatable) |
| 5 | `tab_dist` | Distributions | Price distribution histograms/KDE by province |
| 6 | `tab_geo` | Geo Map | Choropleth map of China provinces + animated monthly playback + period comparison |
| 7 | `tab_interprov` | Inter-Provincial Flow | 省间现货交易 — export/import volumes and prices by province |
| 8 | `tab_fundamentals` | Market Fundamentals | Installed capacity, generation mix, renewables share, peak load, load factor, system tightness |
| 9 | `tab_agent` | Agent | Claude-powered analyst agent with tool use |
| 10 | `tab_mgmt` | Data Management | S3 PDF upload, PDF inventory vs DB coverage gap analysis, pipeline trigger |

### Data sources
| Table/Source | Schema | Content |
|---|---|---|
| `public.spot_daily` | report_date, province_en, province_cn, da_avg/max/min, rt_avg/max/min | Daily DA/RT clearing prices (¥/kWh) |
| `staging.spot_interprov_flow` | report_date, direction, metric_type, province_cn, price, vol | Inter-provincial spot trading |
| `staging.spot_report_summaries` | report_date, summary_text, model, source_pdf | AI-generated daily market narratives |
| `data/market-fundamentals/*.xlsx` | Excel file, one sheet per province | Installed capacity (万kW), generation (亿kWh), peak load (MW) by fuel type, 2024/2025 |
| S3 `bess-uploader-data-chen-singp-2026/spot-reports/<year>/` | PDF files | Source daily market reports |

### Agent tools (defined in `services/spot_mcp/tools.py` + `tab_agent` in app.py)
- `get_spot_prices(start_date, end_date, provinces)` — queries `public.spot_daily`
- `get_interprov_flow(start_date, end_date)` — queries `staging.spot_interprov_flow`
- `get_market_summaries(start_date, end_date)` — queries `staging.spot_report_summaries`
- `get_market_fundamentals(provinces, year)` — reads Excel via `services/market_fundamentals/loader.py`
- `run_pipeline(pdf_path, dry_run)` — triggers full ingestion pipeline via `apps/spot-watcher/pipeline.py`

### Market Fundamentals Excel loader (`services/market_fundamentals/loader.py`)
- `load_province_data()` — `@lru_cache(maxsize=1)`, reads latest `*.xlsx` from `data/market-fundamentals/`
- Data structure per province: `{capacity: {year: {fuel_cn: {value, share}}}, generation: {...}, peak_load: {year: {summer, winter, other}}}`
- Anchor-column parsing: finds 需求13 (capacity), 需求14 (generation), 需求11 (peak load) in each sheet
- `stop_before_row` guards prevent row overlap between sections
- Units: capacity in 万kW, generation in 亿kWh, peak load in MW

### Key session state keys
- `lang_radio` — "English" or "中文"
- `anim_playing`, `anim_frame_idx`, `_anim_loop_rerun` — geo map animation control
- `translated_summaries` — `{report_date_str: chinese_text}` cache for province deep-dive

### Standard EOH constants (in `tab_fundamentals` code block)
Wind 2000h · Solar 1100h · Thermal 5500h · Hydro 3500h · Nuclear 7500h · Storage excluded

---

## Session Summary, 2026-05-06

**Worked on:** `apps/spot-market` — multiple feature additions and bug fixes; ECS deployment troubleshooting.

**Completed:**
- Market Fundamentals tab (capacity, generation, renewables share, peak load, load factor, system tightness)
- Bilingual support (EN/ZH) across all tabs including lazy summary translation
- Geo map animation loop fix (`_anim_loop_rerun` flag)
- CJK font fix in Docker (layer order + `FontManager()` cache rebuild)
- File upload table refresh fix (clear widget state before rerun)
- Peak load table refactored to ranking table with both years
- Load factor table with equivalent hours columns
- Pandas Styler removed in favour of `+`/`−` string formatting
- `--server.fileWatcherType=none` added to Dockerfile CMD
- Deployed to ECS as `bess-spot-markets:v13`

**In progress:** Nothing — all changes deployed.

**Decisions made:** See entries above.

**Next session:** The 5-pillar system is Pillar 1 (Market Map / spot-market) now largely complete. Pillar 2 (Asset Map) is the logical next focus — similar framework to spot-market but modelling asset value by type and region. Pillar 3 (Asset Operations) has a foundation in Inner Mongolia ops (`services/ops_ingestion/`). Consider which pillar to prioritise based on immediate business need.

---

## Session Summary, 2026-09-22 (Nodal Trading Agents — shipped to main)

**Worked on:** Nodal Trading Agents plan (8 tasks, SDD): L1 grid forecast contract, asset registry extraction, grid node map (vision), L2 nodal price formation, L3 per-asset agent engine, promote loop, Nodal Trading tab, final review + merge.

**Completed:** All 7 build tasks + final review; 19 commits merged to main (merge 327a49d, pushed); 172/172 tests green. Final review caught a real Critical (UTC metric_time::date bucketing priced overnight charge slots from the FOLLOWING CST day in the evaluation path — fixed f07f22b). L2 backtest gate on real data: FAIL all 5 zones — day-level nodal−grid delta is non-stationary (train −67..−10 vs holdout −19..+14 CNY/MWh) → **L2 (price_formation) ships UNWIRED per user decision; writer uses grid level × per-slot shape**.

**Key lessons:** (1) literal −∞ zone masks churn under RTE<1 — no-discharge = price 0.0; (2) simultaneous best-response oscillates under binding shared caps — Gauss-Seidel; (3) _load_shapes 1-based slot reindex bug (ffb96c0); (4) run-task probes: NAT subnets (hermes config) + sqlalchemy (no psycopg in images); public service subnets fail ECR pull without public IP.

**Decisions made:** merge with L2 unwired; 蒙西 renewable_d1_mw empty → proxy = load−bidding_space.

**Next session:** (a) user reviews substation_capacity.md + bess_asset_registry_draft.md → seed registries; (b) prod DDL apply + dry-run writer (needs confirmation); (c) deploy mengxi-dashboard v23 (needs confirmation); (d) 蒙西 fundamentals ingest gap 09-14→09-16 (load ~25GW vs 43GW normal, grid price 0.0 on 09-16) — data patrol; (e) L2 model iteration (rolling window/substation-local features) before any wiring.

## 2026-09-23 — v23 deployed + LingFeng recovery
**Deployed:** mengxi-dashboard **v23 (td:37)** live with Nodal Trading tab. Built from `git archive main` (parallel session's dirty app.py kept out), targeted terraform apply (their uncommitted lifecycle-guard edits kept out), update-service --force-new-deployment (their new `ignore_changes=[task_definition]` guard on mengxi service makes the force step mandatory now). Stable, clean startup. tfvars edited but git-ignored (never committed).
**LingFeng:** password rotated → td:8 (jq-swap; family NOT in terraform). Backfill 09-16→09-22 done （冀北/广州 follow-up needed). 蒙西 verified real prices; dry-run re-run OK (real economics). ols_fundamentals_v1 capture runs ~15h in background.
**Open:** (a) Mac launchd LingFeng fallback dead since Aug 20 (OneDrive permission) — not a fallback; (b) 0.0-junk-row writer guard + '运行数据披露' phantom province parser bug; (c) 冀北/广州 backfill pass; (d) recursion non-convergence at fleet scale (iterations=3, delta ~10GWh) — model-owner decision; (e) two data reviews (substation_capacity.md, bess_asset_registry_draft.md) → full registry seed; (f) L2 model iteration before wiring.


## 2026-09-24 — v24 deployed (damping live)
**Deployed:** mengxi-dashboard **v24 (td:38)** — damped best-response recursion (damping=0.5) + lazy strategy_experiments import. Built from git archive main; targeted apply; force-new-deployment; stable, clean startup.
**State:** Nodal Trading tab live with 23-plant fleet (6 reviewed + 17 provisional flat-but-honest); registry 23 active / 21 parked; Mac LingFeng fallback restored (~/bess-platform-lingfeng, 29/29 logins); ECS primary td:8 (new password); full backfill 09-16→09-22 done.
**Open:** user data reviews (substation_capacity.md, bess_asset_registry_draft.md) → mapped fleet; L2 model iteration before wiring; recursion tightening optional (damping 0.3 / max_iter 4).


## 2026-09-25 — v25 deployed (tab S1/S3 fixes)
**Deployed:** mengxi-dashboard **v25 (td:39)** — Nodal Trading tab: S1 resolves registry name → fengxing_node_name before price queries (was silently empty on every node); S3 bridges attribution asset_code → plant_name via zones.py (could never overlap by name). Stable, clean startup.
**Remaining big item:** no scheduled daily run_day writer — S2/S3 live only on days a probe/manual run has produced strategies (09-12, 09-21 currently). EventBridge Fargate schedule ~17:00 CST proposed, user decision pending.


## 2026-09-25 — Daily nodal writer scheduled (v27)
**Deployed:** mengxi-dashboard **v27 (td:41)** + **EventBridge rule bess-platform-nodal-writer-daily (16:00 CST)** running services/nodal_agents/daily_job.py on the bess-platform-nodal-writer family (1vCPU/4GB, image tracks app image). Smoke run exit 0: RUN_DAY 23 plants D+1, damped loop CONVERGED (iterations=2, delta 0.0); REGISTER skips cleanly until parallel workstream commits strategy_experiments.py. First real cron run: today 16:00 CST. Terraform: infra/terraform/nodal_writer.tf.

## 2026-09-26 — Ingest image v7 with zero-day guard (td:9)
**Deployed:** lingfeng-ingest **v7 / td:9** — placeholder-zero-day guard live in prod ingestion (all-zero rt_price days dropped for active markets; kept for no-disclosure provinces). Incremental build FROM v6 (full playwright base rebuild stalls from this network). Smoke: 蒙西 09-25 collected + ingested clean on v7. Guard committed afc2c4c; 09-24 蒙西 zeros overwrite by tonight run.


## 2026-09-27 — Writer schedule fixed (family-ARN pin) + first green cron-path run
**Incident:** first two scheduled firings (09-26, 09-27 08:00 UTC) FAILED — EventBridge target pinned nodal-writer:1, deregistered by the v27 terraform apply → RunTask 'TaskDefinition is inactive' (FailedInvocations=1 both days, found via CloudTrail). **Fix:** target now uses the td FAMILY arn (arn_without_revision), resolves latest-active at run time (7c82c67). Manual run on the fixed path: exit 0, RUN_DAY 2026-09-28 23 plants (iterations=3, delta 2414 MWh; shape_misses=17). Tomorrow 08:00 UTC is the first true cron run on the fixed target.
**Flagged sibling breakage:** rule bess-platform-nodal-pf-daily pins bess-platform-mengxi-dashboard:33 — same 'inactive' failure mode since the v23 deploy (09-23); its owner needs the same family-ARN repoint (feeds reports.nodal_pf_node_daily → T6 theoretical + Nodal Maps tab).

## Session Summary, 2026-10-02 → 2026-10-06
**Worked on:** Two new workstreams from scratch: (1) Power Academy — bilingual power-markets quant curriculum built from the owner's Western-market library + SEE practice folders; (2) 西洋镜看中国电力市场 column (xiyangjing) — evidence-pack publishing pipeline. Both live in new `power-academy/` package, merged to main (96888b0) and pushed.

**Completed:**
- Power Academy Phase 0-1: 126 sources indexed (LibreOffice conversion for legacy .ppt/.doc, local macOS Vision OCR for scanned PDFs), 8-track syllabus with 100 stubs, zero coverage gaps (owner-approved). Cloud LLM stages run on Fargate via `power-academy/scripts/run_cloud.sh` (Anthropic geo-blocked from Mac; owner runs script manually — `aws ecs run-task` is classifier-blocked for Claude sessions).
- Power Academy Phase 2: 24 concepts EN+ZH (asset_valuation ×12, hedging_trading ×12), each with a tested runnable lab; 3 gate-enforced CLI tools (pack/set-status/translate); suite 196; fresh-context review with Critical/Important fixes (hour-weighted implied offpeak, published lab gate, relative cross-lab imports).
- Xiyangjing column: evidence-pack pipeline (read-only SQL/KB, sha256 manifest, [[E:id]] claim tracing), 7 gates incl. hard-coded owner_signoff render gate, 公众号 HTML renderer, editor agent `column propose-topics` (hermes briefings + KB + weekly anomaly scan). Suite 87, reviewed and fixed.
- RDS-from-Mac solved: Astrill excludes AWS → direct PGURL works with VPN on (no AWS changes needed).

**In progress:** nothing active. ZH translations complete for all 24 concepts.

**Decisions made:**
- Mixed labs (worked-example per concept + 2-3 anchor model reproductions per track); owner reviews per 6-concept batch; pen-name first for the column (byline is a field); bilingual EN authoring + locked-glossary ZH with staleness hashes; no live web search anywhere (hermes is the only news intake).
- Anchors assert internal consistency (formula = ground truth); practice xlsm models out of approved scope.
- Plan corrections made honestly during execution: rolling-intrinsic tracking is curve-vol-driven (not rebalance frequency); dynamic delta rebalancing fails through jumps; linear beats benchmark hedging under mean reversion.

**Next session:**
- Candidates: xiyangjing pilot article (pick topic from `columns/xiyangjing/topics/backlog.yaml`; workflow in its README), Phase 3 simulation games, or hermes international-feeds workstream (Timera/Modo/Montel/Cornwall/EnAppSys — needs hermes deploy confirmation).
- Carry-forward: `/tmp/bess-pa` worktree kept (holds git-ignored `cache/` extracted source text reused by future phases). Incident lessons logged in ledgers: pull-results EN-overwrite (guard added — zh-only pull), forward-curve tautology, translate JSON fragility (now plain-markdown output).
- Project memories written: `project_power_academy.md`, `project_xiyangjing_column.md`.

## Session Summary, 2026-10-06 → 2026-10-10
**Worked on:** 西洋镜看中国电力市场 (公众号 **胖橘花丈量电价**, 作者 **PJH**) — articles 001 and 002 end-to-end through the evidence-pack pipeline.

**Completed:**
- **001《输电权来了：云霄直流第一课》** — full cycle: evidence pack (8 entries incl. NDRC 734号文 + 方案全文), JR Fujian data rescue (LingFeng 福建 selector exports SHANXI content — broken at source), draft, three rounds of owner corrections (drop 2025 data, remove company name, Fujian disclosure nuance→access-scope), GPT 9-point revision (three-paradigm framing, 85/101/16 framework, net-spread formula), final owner version adopted. Identity locked: 胖橘花丈量电价/PJH. UTF-8 charset fix (mojibake), generic source footer (no internal identifiers), claim-map evidence mode invented (clean manuscript + auditable claims). Published to GitHub main.
- **002《广西9家售电公司联名求救》** — all policy numbers verified against primary docs (桂电交易函〔2026〕3号 4.224→8.448 cap; 广西3.0版 第十六/三十六条; 广东细则 1分×系数6=6分/200万, ≤20%履约保险, 红色预警3日; 桂电交易函〔2026〕134号 May-13 suspension of all three sharing mechanisms + 平价套餐 4.224×0.5 uplift). Province conflation fixed (10→5 is Guangxi May, Guangdong is 3 days natively). Draft committed (e99e951), gates green, awaiting owner review + signoff for render.

**In progress:** Article 002 final review + render + publish; 公众号 itself not yet launched publicly.

**Decisions made:**
- Claim-map mode (evidence/claim_map.yaml) is the default for public articles — manuscript stays clean, audit lives beside it; owner waives inline [[E:]] tags.
- Footer shows only generic source categories (公开政策文件/作者自有经营数据/授权市场数据/公开报道/作者注记); no table names, doc ids, file paths, company names in public HTML.
- Unverified numbers get cut, not softened ("由10缩短", "覆盖6个月" examples); owner domain notes registered as owner_note evidence class, honestly labeled.
- Worktree lives at ~/bess-pa-wt (NEVER /tmp — macOS wipes on reboot, cost one article scaffold).

**Next session:**
- 002: owner signoff → render → push → publish 001 and 002 to 公众号.
- LingFeng 福建 selector bug: decide whether to fix collector mapping or report to vendor; add zero-price monitoring rule to hermes data patrol (silent 2026 福建 zeros across prices+fundamentals).
- 003 candidates: first 云霄 auction results (25.6/100 vs measured 85/101/16); retail-risk P2 ingestion (山东 PDF + contract schemas) still open from earlier queue.
- Watch: parallel session's stale worktree keeps deleting newer files from main (3 clobber incidents this week) — restore via merge-tree when it happens.
