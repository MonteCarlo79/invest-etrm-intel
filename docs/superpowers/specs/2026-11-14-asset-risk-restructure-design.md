# Asset-Risk / Retail-Risk Restructure — Design Spec

**Date:** 2026-11-14
**Status:** Draft for user approval
**Apps touched:** `apps/asset_risk` (primary), `apps/retail_risk` (position tabs removed), `libs/trading_book` (registry, waterfall delegation)
**Companion spec:** `2026-11-14-market-maker-venue-design.md` (receives the position layer)

---

## 1. Intent

Asset-risk was specified in July 2026 as an ETRM risk cockpit (positions, MtM, VaR, Greeks, forward curves). What was actually built and validated over 97 deploys is a **settlement ledger and dispatch-attribution tool for owned 蒙西 BESS stations and the 零碳46 wind farm**. The architecture never caught up: business logic lives in Streamlit tab files, parser routing is a nested ladder in the UI, asset identity is mapped six times, and two placeholder tabs read tables nothing populates.

This restructure makes the code match the product:

- asset-risk becomes the **realised layer for generation-side books** (bills, dispatch chain, attribution waterfall, merchant share).
- retail-risk stays the **realised layer for load-side books** and loses its position/MtM/VaR tabs.
- The **open/forward layer** (positions, exposure, MtM, VaR, Greeks, curves) moves out of both apps into the new market-maker app (companion spec).

No number shown to the user changes in steps 1–3. Step 4 restores a currently broken attribution column.

## 2. Goals

1. Every domain computation in asset-risk is importable without Streamlit and covered by the existing 95 tests plus new router tests.
2. One settlement parser router shared by the upload tab and the folder scanner.
3. One asset identity map, derived from the trading-book registry, replacing PLANT_MAP / AUTO_NOM_CODE / CAPCOMP_RATE / folder_mapper / settlement-bridge name map / attribution alias.
4. The dispatch chain reads WeCom actuals, so the Δ执行与费用 attribution rows stop showing 无数据 for months after 2026-08.
5. Position, MtM and VaR tabs removed from both apps; their tables and agent tools hand over to market-maker. "Hand over" means market-maker becomes the sole UI and the owner of future schema evolution for `rm_positions`, `rm_forward_curves`, `rm_var_snapshots`; evolution is additive only (new columns, no renames, no repopulation), so today's readers in retail-risk reconciliation, `services/retail_risk/pnl_bridge.py` and `services/deal_committee/orchestrator.py` keep working untouched.
6. Agent tools cover what the app actually does (waterfall, diagnostics, merchant share, settlement).
7. Benchmark and forecast LP legs are persisted per asset-month instead of solved on page render.

## 3. Non-goals

- No new attribution methodology. The 4-stage 投资标准→申报→出清→实际 waterfall and its 偏差归因 rows are moved, not redefined.
- No parser rewrites. Provincial parsers stay as they are; only the routing moves.
- No change to `rm_settlements` / `rm_settlement_items` schema.
- No retail-risk logic changes beyond removing three tabs and their agent tools.

## 4. Anti-goals (what would make this a failure)

- Any settlement total, waterfall segment, or diagnostics figure differs after the move. The regression check is: same inputs, byte-identical outputs on the existing fixtures.
- A parser incident class that is already guarded (voucher supersede, 统推 canonical preference, hash dedup, scanned-charge refuse) regresses because the guard was left behind in the tab.
- The trading book is modified in a way that breaks mengxi-dashboard's Book tab.

## 5. Constraints

- CLAUDE.md coding rules: surgical edits, no stealth improvements, confirmation before deploy or schema change.
- Deploy via the existing jq-swap pattern from the service's current tdArn; amd64 build; `.dockerignore` exception list must gain any new `libs/` package (v93 incident).
- Parallel sessions edit `LATEST-IMAGES.md` and `MEMORY.md`; commit with explicit paths.
- DB writes from the Mac are unreliable; migrations run via ECS run-task.

## 6. Target structure

```
libs/asset_risk/                      pure, no streamlit
  __init__.py
  registry.py        asset identity (see §7)
  settlement/
    router.py        ordered detector→parser table (see §8)
    ledger.py        insert / overwrite / voucher-supersede / hash-dedup (from tab_settlement)
    analytics.py     monthly pivot, YTD subtotals, 口径校验, prefer_canonical_settlements,
                     compute_merchant_shares (from tab_settlement + tab_pnl)
  chain.py           typed asset-day chain frame (see §9)
  diagnostics.py     exec_gap_monthly, bid_fail_monthly, capacity_loss_monthly,
                     restriction_monthly, find_defect_events, _attach_prices (from tab_diagnostics)
  waterfall.py       stage_arbitrage, stage_capacity, guard_cleared_leg, benchmark_leg,
                     forecast_optimal_leg, build_waterfall, attribution_rows (from tab_waterfall)
  legs_store.py      persisted benchmark/forecast legs (see §11)
  agent_tools.py     tool specs + execute_tool, no streamlit (from tab_agent)
services/asset_risk/
  headless_agent.py  imports from libs/asset_risk/agent_tools (unchanged interface)
apps/asset_risk/
  app.py             lazy tab render (only the selected tab computes)
  tab_asset_config.py, tab_settlement.py, tab_pnl.py, tab_diagnostics.py,
  tab_waterfall.py, tab_wind_replication.py, tab_agent.py   widgets + charts only
```

Tabs removed: `tab_positions.py`, `tab_var.py` from asset-risk; `tab_positions.py`, `tab_var.py` from retail-risk (retail keeps `tab_pnl.py`, which reads `rm_pnl_snapshots` for realised P&L only — the "Latest Unrealised MtM" metric is removed from it).

`libs/risk/` (mtm, var, greeks, pnl) stays where it is; market-maker owns its evolution.

## 7. Registry unification

`rm_assets` gains `asset_code TEXT UNIQUE NULL` referencing `libs.trading_book.registry` codes: `hangjinqi`, `bameng`, `suyou`, `gushanliang`, `wulate`, `siziwangqi`, **`wuchuan`, `alashan`** (user decision 2026-11-14: 武川 and 阿拉善 are first-class BESS assets with the same data as the others; the registry already holds both with `data_path='full'`, exchange account names and WeCom nomination codes, and `rm_assets` rows 武川 / 阿拉善储能 exist). Migration: `ALTER TABLE ... ADD COLUMN IF NOT EXISTS`, then an UPDATE seeded from today's `AUTO_NOM_CODE` map plus the two new codes. Wind: 零碳46 gets code `lingt46` (already a registry code).

`libs/asset_risk/registry.py` exposes one function, `asset_identity(engine) -> DataFrame` with columns: `asset_id, name, asset_code, asset_type, plant_names (tuple, md_id_cleared_energy account names), capcomp_rate, invoice_folder, capacity_mw, bess_duration_h, commission_date`. `plant_names` = `StationRef.md_plant_names` (武川 has two accounts, 旭纯哈乐储能电站 / 旭纯哈乐储能电站二, summed per slot); `capcomp_rate` = the existing `StationRef.subsidy_cny_per_mwh` (no new registry field; 350 default, 280 for alashan/wuchuan). Every former map is a derived dict from this frame.

Consumers switched: tab_diagnostics, tab_waterfall, services/mengxi_nodal/settlement_bridge.py `_RM_ASSET_NAME`, libs/trading_book/waterfall.py `_ATTR_CODE_ALIAS` (bameng→hetao alias stays, now in the registry). Wind constants in `tab_wind_replication.py` (BOOK_ID=6, PLANT, ASSET) resolve from the registry row for `lingt46`.

**What the registry does not take over (my calls, builder check 2026-11-14):**

- `invoice_folder` stays DB-sourced on `rm_assets` (it is UI-editable in Asset Config). `folder_mapper.py` keeps its explicit-skip entries (`B-【外】乌海储能 → None`) and its non-registry fallbacks (乌兰察布); it only stops carrying names that the identity frame now provides. 乌海 scans do not start ingesting as a side effect.
- Books with no registry code (灵山, 滨州, 定远, 融水, 乌兰察布) get `asset_code NULL`, `plant_name NULL`, `capcomp_rate 350`. They have no dispatch chain, so nothing downstream changes.
- `capcomp_rate` moves to a table keyed by `asset_code` in `libs/trading_book/registry.py`: 280 for `alashan`, `wuchuan`, and 锡西二 if it ever gets a code; 350 otherwise. Today `CAPCOMP_RATE["阿拉善"]` never matches the rm_assets name `阿拉善储能`, so 阿拉善 is silently billed at 350 in the waterfall; keying by code fixes it to 280 (user decision, §15).
- Step 3 verification is therefore "identity frame equals the six old maps except the listed exceptions (阿拉善 rate, NULL rows for non-registry books)", with the exception list printed by the probe.

## 8. Settlement router

`libs/asset_risk/settlement/router.py`:

```python
@dataclass(frozen=True)
class Route:
    label: str                       # user-facing, e.g. "蒙西上网电费结算单 (成分明细)"
    kind: str                        # "discharge" | "charge" | "voucher" | "skip"
    detect: Callable[[BillText], bool]
    parse: Callable[[BillText], list[dict]]
    needs_text_layer: bool = True

ROUTES: tuple[Route, ...] = (...)    # today's ladder, in today's priority order

def route(bill: BillText, kind_hint: str) -> Route | None
```

`BillText` carries `path, filename, is_scanned, full_text, page1_text`. The order of `ROUTES` reproduces `_process_pdf` exactly, including the skip rules (广西 grid duplicate, wb2 voucher, SDTC 售电侧 duplicate check which needs `_has_category` — that check stays in `ledger.py` and the route returns both sides tagged so the ledger decides). Vision parsers are the last routes for each kind.

**Caller modes (my call, 2026-11-14 builder check).** `route(bill, kind_hint, mode)` with `mode in ('upload','scanner')`. The 发电侧凭证 fallback (voucher as sole discharge source) is an `upload`-only route: the scanner is unattended and the 乌海 2026-08/09 double count came from exactly this fallback running without a human in the loop. In `scanner` mode every voucher route resolves to `skip`, which is the scanner's behaviour today. The scanner keeps its own writer (`_write_settlement`, `_has_discharge_items`); only its detection switches to `route()`. Unifying the two write paths is not in scope.

Tests: one fixture text per existing parser detector (text snippets already exist in `tests/services/settlement_ingest`), asserting the route label chosen. Plus an ordering test: the tuple's labels equal a frozen list, so reordering is a visible diff.

## 9. Chain frame

`libs/asset_risk/chain.py::load_chain(engine, asset_ids, start, end) -> DataFrame` with the same columns the tabs use today (`asset_id, asset, capacity_mw, ts, nominated_mw, da_cleared_mw, rt_cleared_mw, actual_mw, restriction, soc_pct`), built from:

| Column | Source, in priority | Unit contract |
|---|---|---|
| nominated_mw | `bess_nomination_96` (auto) → `rm_dispatch_chain` (legacy ≤2026-08-30) | MW, +discharge |
| da_cleared_mw | `rm_dispatch_chain` only (no live source) | MW |
| rt_cleared_mw | `rm_dispatch_chain` → `md_id_cleared_energy.cleared_energy_mwh` summed over the asset's `plant_names` per slot (interval-average MW, **no ×4**; exact names + `data_date` bound, the `AssetBook._CLEARED_SQL` pattern, never LIKE) | MW |
| actual_mw | `rm_dispatch_chain` → **`bess_actual_96`** (WeCom actuals, new) → `ops_bess_dispatch_15min` | MW |
| restriction, soc_pct | `rm_dispatch_chain` only | — |

`ts` is CST-naive wall clock; the `AT TIME ZONE 'Asia/Shanghai'` expression stays for the legacy table, named zone only. The merge rule is today's: auto wins on overlap, auto-only slots extend. A unit assertion guards `rt_cleared_mw`: monthly Σ|rt|×0.25 within 3× of billed discharge, else raise with the month named (replaces silent downgrade for the loader; `guard_cleared_leg` stays as the display-level tripwire).

**Actuals sign and coverage (my call).** `bess_actual_96.actual_mw` comes from column F (实际执行功率) of the same WeCom 调度计划表 workbook that supplies `nominated_mw`, so the two share a sign convention by construction (+ = discharge). The loader asserts it anyway: per asset-month, `sign(Σ actual_mw × nominated_mw) > 0` where both are non-null, else raise naming the month. Coverage is whatever the WeCom cache holds (the collector only started landing 2026-11-09 and WeCom caches only opened chats), so the step 4 acceptance criterion is: Δ执行 rows are non-null for every month where `bess_actual_96` has ≥ 20 days for that asset, and the coverage table on the tab shows per-month `with_actual` counts so gaps are visible. No historical backfill of actuals is in scope.

## 10. Agent tools

Keep: `get_asset_list`, `get_book_pnl`, `get_settlement_summary`, `get_deviation_analysis` (now over `load_chain`).
Add: `get_waterfall(asset, months)` returning stage totals + attribution rows; `get_diagnostics(asset, start, end)` returning exec-gap, bid-fail, restriction, defect summaries; `get_merchant_share(book_id)`.
Remove: `get_position_mtm`, `get_var` (move to market-maker).
Both the tab and the headless agent share one grounding system prompt constant; the headless prompt gains the grounding rule it currently lacks.

## 11. Persisted legs

Table `marketdata.rm_waterfall_legs (asset_id, month, leg TEXT CHECK IN ('benchmark','forecast'), arb_cny, dis_mwh, chg_mwh, cap_cny, days_solved, days_skipped, age, note, price_source, computed_at, PRIMARY KEY (asset_id, month, leg))`. `legs_store.get_or_compute()` returns the stored row if present and the month is complete (month_end < today − 3 days), else computes and upserts. The waterfall tab calls `get_or_compute`; `st.cache_data` is dropped for these two functions. A "重算" button on the tab forces recompute for the selected asset-months.

## 12. Build order (each step deployable alone)

| Step | Change | Verify |
|---|---|---|
| 1 | Move pure functions to `libs/asset_risk/*`; tabs import from there; `headless_agent` imports `agent_tools` | 95 tests pass unchanged; `python -c "import libs.asset_risk.waterfall"` has no streamlit in `sys.modules` |
| 2 | Router table + scanner switch + router tests | router tests; existing settlement_ingest suite |
| 3 | `rm_assets.asset_code` migration (ECS run-task) + registry module + consumers switched; 武川 and 阿拉善 enter Diagnostics and Waterfall | identity frame equals the six old maps on prod data except the printed exception list (one-off probe script); 武川/阿拉善 rows appear in the coverage table with their nomination, cleared and price counts |
| 4 | `chain.py` with actuals | Δ执行 rows non-null for 2026-09..11 on prod; existing diagnostics tests |
| 5 | Agent tools reshaped; position/VaR tabs removed from both apps; retail `tab_pnl` MtM metric removed | headless agent answers a waterfall question via Hermes bridge |
| 6 | `rm_waterfall_legs` + lazy tabs | cold load of 全部资产 × 3 months < 5 s after first compute |

Steps 1–2 ship as one asset-risk deploy; 3 and 4 as one; 5 is one asset-risk + one retail-risk deploy; 6 is one.

**Hermes dependency (open for the user, §15).** `services/asset_risk/headless_agent.py` runs inside the hermes image via `services/hermes/market_agent_bridge.py`. After step 1 it imports `libs/asset_risk`, which the hermes `.dockerignore` allowlist does not include, so the next hermes rebuild would break the asset-risk bridge unless `!libs/asset_risk/` is added to the hermes allowlist in the same commit as step 1 (it will be). The running hermes task is unaffected until rebuilt. Step 5's Hermes verification only holds after a hermes rebuild; until then the check is `run_asset_risk_query()` invoked directly in the asset-risk image.

## 15. Decisions taken by the user (2026-11-14)

1. 阿拉善 capacity-compensation rate: corrected to 280 ¥/MWh in step 3. This is the one intentional number change in the restructure; the step 3 probe prints it as a named exception.
2. Hermes: a hermes rebuild is bundled with step 5 (delta build via `scripts/cloud_build.sh`, jq-swap from the service's current tdArn with the env-var count verified before and after). Step 5's Hermes-path verification stands as written.

## 13. Left to the builder

- Exact split of `tab_settlement.py` helper functions between `ledger.py` and `analytics.py` (rule: anything that writes is ledger).
- Whether `agent_tools.execute_tool` takes an engine or a connection (match `headless_agent` today: engine).
- Lazy-tab mechanism: `st.radio` navigation or `st.tabs` with per-tab `if` guards keyed on a `selected_tab` session value. Either is acceptable; must not compute inactive tabs.
- Test fixture for the chain loader (sqlite as in `tests/asset_risk/test_dispatch_diagnostics.py`).

## 16. Open items recorded, not in scope

- 武川 / 阿拉善 are in scope (supersedes the 2026-11-11 deferral). Known data caveats, surfaced by the coverage table rather than hidden: neither has legacy `rm_dispatch_chain` rows, so their chain is auto-nomination + exchange-cleared + WeCom-actual only, from the dates those feeds cover; 阿拉善's registry price node is still a candidate (the station had not appeared in the price table as of 2026-09), so its 投资标准 leg may report "no prices" until the node is confirmed; 阿拉善 and 武川 settlement bills are ingested through the normal Settlement tab like any other book.
- `da_cleared_mw` has no live source since the manual chain stopped; the 日前未中标 row will keep showing 无数据 until one exists.
- The trading-book waterfall (`libs/trading_book/waterfall.py`) delegating to `libs/asset_risk/waterfall.py` is P2 of the book roadmap and a separate change after step 4.
