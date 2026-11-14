# Asset-Risk / Retail-Risk Restructure Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move asset-risk's domain logic out of Streamlit tab files into `libs/asset_risk`, unify the settlement parser router and the asset identity map, read WeCom actuals into the dispatch chain, retire the position/VaR tabs from asset-risk and retail-risk, and persist the LP legs — with no displayed number changing except the one the user approved (阿拉善 capacity rate 280).

**Architecture:** Pure functions move first with re-exports so the 95 existing tests pass unchanged; UI files keep only widgets and charts. New code enters as `libs/asset_risk/*` modules with sqlite-ATTACH tests in the existing harness style. Six deployable steps; each deploy and the one schema migration stop for explicit user confirmation.

**Tech Stack:** Python 3.11/3.13, pandas, SQLAlchemy (Postgres prod, sqlite-ATTACH in tests), Streamlit, pdfplumber, pulp (LP), pytest. Deploys: amd64 docker build (or CodeBuild delta build for hermes), ECR, jq-swap task definitions.

**Spec:** `docs/superpowers/specs/2026-11-14-asset-risk-restructure-design.md`

## Global Constraints

- CLAUDE.md coding rules: surgical edits only; no stealth improvements; match existing style.
- Every deploy and the `rm_assets` migration require explicit "yes" from the user in the current message before running. The plan marks these as **STOP** steps.
- Commit and push only from `~/bess-platform-local` (OneDrive `.git` is dataless; see memory). Always `git add` / `git commit -- <explicit paths>`; never stage `infra/terraform/terraform.tfvars`.
- Image builds: `--platform linux/amd64`; verify with `docker buildx imagetools inspect <uri>:<tag> | grep Platform`. Before tagging read `LATEST-IMAGES.md` and pick max+1. Register new task definitions from the service's CURRENT tdArn (never the family name). Hermes stays on the latest hand-injected revision (31 env vars); verify env count before and after.
- `libs/asset_risk/**` must not import `streamlit` (asserted by test in Task 4).
- Named time zones only (`AT TIME ZONE 'Asia/Shanghai'`); never the `'+08:00'` literal.
- `md_id_cleared_energy` queries filter on exact `plant_name IN (...)` and `data_date` bounds; never `LIKE`.
- `md_id_cleared_energy.cleared_energy_mwh` is interval-average MW: no ×4 anywhere.
- Unit of `bess_actual_96.actual_mw` and `bess_nomination_96.nominated_mw`: MW, + = discharge.
- No behaviour change in steps 1–2 (Tasks 1–8): the regression check is the existing suite passing unmodified plus the parity test in Task 7.

## Review Focus

1. A 下网 (charge) filename whose text layer matches a discharge detector must still be routed as a charge bill, as today (filename kind wins the branch); a wrong guess here re-creates the 1000× kWh incident. Test in Task 6.
2. The scanner must never obtain the voucher-fallback route even for a month with no discharge rows; that path double-counted 乌海 2026-08/09. Test in Task 6.
3. `load_chain` for an asset with no registry code (wind, 灵山-type books) must return the legacy chain rows only and never raise. Test in Task 11.
4. 武川 has two exchange accounts; a slot where only one account has a row must sum to that row, not NaN, and a slot with both must sum both. Test in Task 11.
5. The legs store must never persist a leg for an incomplete month (bills and prices still arrive), and `重算` must overwrite a stored leg. Tests in Task 16.

---

## File map

| Path | Responsibility | Step |
|---|---|---|
| `libs/asset_risk/__init__.py` | package marker | 1 |
| `libs/asset_risk/standard_params.py` | moved from `apps/asset_risk/standard_params.py` (re-export left behind) | 1 |
| `libs/asset_risk/chain.py` | chain + price loaders and merge (`_load_chain`, `_load_auto_nominations`, `_merge_auto_nominations`, `_load_prices`, `_attach_prices`, `_month_token_bounds`, `_ts_expr`); step 4 rewrites loaders | 1, 4 |
| `libs/asset_risk/diagnostics.py` | pure metrics from `tab_diagnostics.py` + the `PLANT_MAP`/`AUTO_NOM_CODE`/`CAPCOMP_RATE*` constants (until step 3 derives them) | 1 |
| `libs/asset_risk/waterfall.py` | stage math, guard, legs, `build_waterfall`, `attribution_rows`, `_load_settlement_actuals` from `tab_waterfall.py` | 1 |
| `libs/asset_risk/settlement/__init__.py` | package marker | 1 |
| `libs/asset_risk/settlement/analytics.py` | `prefer_canonical_settlements`, `compute_merchant_shares`, `_bucket_of`, `_year_subtotal_row`, `_insert_year_subtotals`, category label maps, `book_matches_classes`, `_portfolio_matrix`, `_asset_summary`, `_asset_month_metric`, `_load_settlement_items`, `_days_in_months` | 1 |
| `libs/asset_risk/settlement/ledger.py` | `_content_sha256`, `_already_ingested`, `_has_category`, `_delete_book_month_file`, `_delete_voucher_settlements`, plus `insert_settlement(conn, book_id, month, file_name, file_type, file_hash, items)` extracted from the two INSERT blocks | 1 |
| `libs/asset_risk/settlement/router.py` | `BillText`, `Route`, `ROUTES`, `route()` | 2 |
| `libs/asset_risk/agent_tools.py` | `tools`, `execute_tool`, `AGENT_BASE_SYSTEM` | 1, 5 |
| `libs/asset_risk/registry.py` | `asset_identity(engine)` + derived maps | 3 |
| `libs/asset_risk/legs_store.py` | `rm_waterfall_legs` DDL + `get_or_compute` | 6 |
| `scripts/asset_risk/migrate_asset_code.py` | ALTER + seed UPDATE, run via ECS run-task | 3 |
| `scripts/asset_risk/probe_identity.py` | prints identity frame vs old maps, with exception list | 3 |
| `apps/asset_risk/tab_*.py` | thinned; imports from libs; re-exports for tests | 1–6 |
| `apps/asset_risk/app.py` | lazy tabs; two tabs removed | 5, 6 |
| `apps/retail_risk/app.py`, `tab_pnl.py` | two tabs removed; MtM series dropped from chart | 5 |
| `services/settlement_ingest/scanner.py:252-376` | detection replaced by `route(..., mode="scanner")` | 2 |
| `services/mengxi_nodal/settlement_bridge.py:69-80` | `_RM_ASSET_NAME` derived from registry | 3 |
| `libs/trading_book/registry.py` | `attribution_code` field on `StationRef` | 3 |
| `libs/trading_book/waterfall.py:66-75` | `_ATTR_CODE_ALIAS` derived from registry | 3 |
| `services/asset_risk/headless_agent.py:57` | import from `libs.asset_risk.agent_tools` | 1, 5 |
| `apps/hermes-service/Dockerfile:52` | `COPY libs/ /app/libs/` | 5 |
| `.dockerignore:146-152` | `!libs/asset_risk/` | 1 |
| `tests/asset_risk/test_chain_loader.py`, `test_router.py`, `test_registry.py`, `test_legs_store.py`, `test_no_streamlit.py` | new tests | 1–6 |

---

## Step 1 — Extract pure functions (no behaviour change)

### Task 1: Scaffold `libs/asset_risk`, move `standard_params`, `chain`, `diagnostics`

**Files:**
- Create: `libs/asset_risk/__init__.py`, `libs/asset_risk/standard_params.py`, `libs/asset_risk/chain.py`, `libs/asset_risk/diagnostics.py`
- Modify: `apps/asset_risk/standard_params.py` (becomes re-export), `apps/asset_risk/tab_diagnostics.py:21-264` (constants + loaders + pure helpers move out; keep chart helpers and `render_diagnostics`), `.dockerignore:146-152`
- Test: existing `tests/asset_risk/test_dispatch_diagnostics.py`, `tests/asset_risk/test_asset_config.py` (unchanged)

**Interfaces:**
- Produces `libs.asset_risk.chain`: `_ts_expr(engine) -> str`, `_month_token_bounds(start, end) -> tuple[str|None, str|None]`, `_load_chain(engine, asset_ids, start, end) -> pd.DataFrame`, `_load_auto_nominations(engine, asset_ids, start, end) -> pd.DataFrame`, `_merge_auto_nominations(chain, auto) -> pd.DataFrame`, `_load_prices(engine, plants, start, end) -> pd.DataFrame`, `_attach_prices(chain, prices, plant_map) -> pd.DataFrame` — identical bodies to today's `tab_diagnostics.py`.
- Produces `libs.asset_risk.diagnostics`: `PLANT_MAP`, `AUTO_NOM_CODE`, `CAPCOMP_RATE`, `CAPCOMP_RATE_DEFAULT`, `RESTRICTION_CN`, `RESTRICTION_COLOR`, `exec_gap_monthly`, `bid_fail_monthly`, `capacity_loss_monthly`, `restriction_monthly`, `find_defect_events`, `_coverage`, `_month_asset_matrix` — identical bodies.
- Produces `libs.asset_risk.standard_params`: `STANDARD_DOD`, `STANDARD_SOH`, `STANDARD_RTE`, `age_years`, `params_for_asset`.

- [ ] **Step 1: Run the baseline suite and record the count**

Run: `~/.venvs/bess-platform/bin/python -m pytest tests/asset_risk tests/services/asset_risk -q -p no:cacheprovider`
Expected: `95 passed`

- [ ] **Step 2: Create the three libs modules by moving code**

Move the named functions and constants verbatim. `chain.py` imports `PLANT_MAP`/`AUTO_NOM_CODE` from `libs.asset_risk.diagnostics` (same direction as today's single file; no cycle because diagnostics does not import chain). `chain.py` keeps the lazy `from services.mengxi_nodal.nomination_curves import ensure_table` inside `_load_auto_nominations`.

- [ ] **Step 3: Turn the apps files into re-exports**

`apps/asset_risk/standard_params.py` becomes `from libs.asset_risk.standard_params import *  # noqa` plus explicit names `STANDARD_DOD, STANDARD_SOH, STANDARD_RTE, age_years, params_for_asset`. `tab_diagnostics.py` top becomes `from libs.asset_risk.chain import (...)` and `from libs.asset_risk.diagnostics import (...)` listing every name the tab and `tab_waterfall.py`/`tab_agent.py`/tests import (`PLANT_MAP, AUTO_NOM_CODE, CAPCOMP_RATE, CAPCOMP_RATE_DEFAULT, RESTRICTION_CN, RESTRICTION_COLOR, _ts_expr, _month_token_bounds, _load_chain, _load_auto_nominations, _merge_auto_nominations, _load_prices, _attach_prices, exec_gap_monthly, bid_fail_monthly, capacity_loss_monthly, restriction_monthly, find_defect_events, _coverage, _month_asset_matrix`). Chart helpers (`_grouped_bars`, `_pct_heatmap`, `_hourly_exec_gap_profile`) and `_load_assets` stay in the tab.

- [ ] **Step 4: Add `!libs/asset_risk/` to `.dockerignore` under the asset-risk exceptions block (after `!libs/trading_book/`)**

- [ ] **Step 5: Run the suite**

Run: `~/.venvs/bess-platform/bin/python -m pytest tests/asset_risk tests/services/asset_risk -q -p no:cacheprovider`
Expected: `95 passed`

- [ ] **Step 6: Commit**

```bash
git add libs/asset_risk/__init__.py libs/asset_risk/standard_params.py libs/asset_risk/chain.py libs/asset_risk/diagnostics.py apps/asset_risk/standard_params.py apps/asset_risk/tab_diagnostics.py .dockerignore
git commit -m "asset_risk: move chain/diagnostics/standard_params into libs (re-exports kept)" -- libs/asset_risk apps/asset_risk/standard_params.py apps/asset_risk/tab_diagnostics.py .dockerignore
```

### Task 2: Move waterfall logic

**Files:**
- Create: `libs/asset_risk/waterfall.py`
- Modify: `apps/asset_risk/tab_waterfall.py:24-542` (everything above `_DEFINITIONS_MD` except `_load_assets` and the two figure builders moves out)
- Test: existing `tests/asset_risk/test_waterfall.py`

**Interfaces:**
- Produces `libs.asset_risk.waterfall`: `CLEARED_VOLUME_TOLERANCE`, `_STAGES`, `_COMPONENT_CN`, `_COMPONENT_COLORS`, `_load_settlement_actuals(engine, asset_ids, start, end) -> pd.DataFrame`, `stage_arbitrage(df, mw_col) -> pd.DataFrame`, `stage_capacity(dis_mwh, assets) -> pd.Series`, `guard_cleared_leg(cleared, actual, tolerance=CLEARED_VOLUME_TOLERANCE) -> tuple[pd.DataFrame, list]`, `_load_nodal_prices_filled(engine, asset_name, start, end) -> pd.Series`, `benchmark_leg(engine, asset_id, month, capacity_mw, duration_h, commission_iso, capcomp_rate, plant, asset_name="") -> dict` (**no** `st.cache_data` decorator; first parameter renamed from `_engine` to `engine`), `forecast_optimal_leg(engine, asset_id, month, capacity_mw, duration_h, commission_iso, plant) -> dict` (same), `build_waterfall(bench, nominated, cleared, actual) -> pd.DataFrame`, `attribution_rows(df, wf, bench, nominated, cleared, actual, forecast_rows) -> pd.DataFrame`.
- Consumes Task 1 names from `libs.asset_risk.chain` / `.diagnostics` / `.standard_params`.

- [ ] **Step 1: Move the functions verbatim; drop the two `@st.cache_data` decorators in libs**

- [ ] **Step 2: In `tab_waterfall.py`, re-create the cached wrappers and re-exports**

```python
from libs.asset_risk import waterfall as _wf
from libs.asset_risk.waterfall import (CLEARED_VOLUME_TOLERANCE, _load_settlement_actuals, stage_arbitrage,
                                       stage_capacity, guard_cleared_leg, build_waterfall, attribution_rows)
benchmark_leg = st.cache_data(show_spinner=False)(_wf.benchmark_leg)        # same cache semantics as before
forecast_optimal_leg = st.cache_data(show_spinner=False)(_wf.forecast_optimal_leg)
```

`st.cache_data` hashes positional args; the engine is unhashable, so the wrappers must keep the leading-underscore convention: define `def benchmark_leg(_engine, *a, **k): return _wf.benchmark_leg(_engine, *a, **k)` decorated, rather than wrapping the libs function directly. `render_waterfall` keeps calling `benchmark_leg(engine, ...)` positionally as today.

- [ ] **Step 3: Run the suite**

Run: `~/.venvs/bess-platform/bin/python -m pytest tests/asset_risk/test_waterfall.py -q -p no:cacheprovider`
Expected: all tests in the file pass (the file patches `benchmark_leg` internals; if a test patches `apps.asset_risk.tab_waterfall._load_prices`, re-point the patch target to `libs.asset_risk.waterfall._load_prices` — that is the only permitted test edit).

- [ ] **Step 4: Commit**

```bash
git commit -m "asset_risk: move waterfall stage math and legs into libs" -- libs/asset_risk/waterfall.py apps/asset_risk/tab_waterfall.py tests/asset_risk/test_waterfall.py
```

### Task 3: Move settlement analytics and ledger

**Files:**
- Create: `libs/asset_risk/settlement/__init__.py`, `libs/asset_risk/settlement/analytics.py`, `libs/asset_risk/settlement/ledger.py`
- Modify: `apps/asset_risk/tab_settlement.py:12-43, 226-269, 499-529, 563-591, 594-669, 994-1044`, `apps/asset_risk/tab_pnl.py:11-135, 260-348`
- Test: existing `tests/asset_risk/test_settlement_ytd.py`, `test_merchant_share.py`, `test_upload_dedup.py`, `test_pnl_filters.py`, `test_pnl_portfolio.py`

**Interfaces:**
- Produces `analytics`: `CATEGORY_CN` (the dict today duplicated as `_CATEGORY_CN` in tab_pnl and `CATEGORY_CN` inside `_render_analytics`; one copy, both tabs import it), `_REVENUE_CATS`, `ASSET_CLASS_OPTIONS`, `book_matches_classes`, `_portfolio_matrix`, `_days_in_months`, `_load_settlement_items`, `_asset_summary`, `_asset_month_metric`, `_EXCHANGE_SELL_NOTE`, `_EXCHANGE_BUY_NOTE`, `prefer_canonical_settlements`, `_year_subtotal_row`, `_insert_year_subtotals`, `_MERCHANT_CATS`, `_CONTRACT_CATS`, `_SERVICE_CATS`, `_COST_CATS`, `_bucket_of`, `compute_merchant_shares`.
- Produces `ledger`: `_content_sha256(data: bytes) -> str`, `_already_ingested(engine, book_id, settlement_month, file_hash) -> bool`, `_has_category(engine, book_id, settlement_month, category) -> bool`, `_delete_book_month_file(conn, book_id, settlement_month, file_name)`, `_delete_voucher_settlements(conn, book_id, settlement_month) -> int` (the unreachable lines after its `return` are dropped), `insert_settlement(conn, book_id, settlement_month, file_name, file_type, file_hash, items) -> int` returning the new `rm_settlements.id`; it writes `delivery_date / amount_receivable_cny / amount_settled_cny / amount_diff_cny / counterparty` when present in an item and `peak_period` when present, so one INSERT serves both the PDF and Excel paths.

- [ ] **Step 1: Write the ledger parity test**

In `tests/asset_risk/test_upload_dedup.py` add:

```python
def test_insert_settlement_pdf_and_excel_items(sqlite_engine):
    from libs.asset_risk.settlement.ledger import insert_settlement
    pdf_items = [{"category": "discharge_energy", "amount_cny": 100.0, "peak_period": "peak", "notes": "n"}]
    xl_items = [{"category": "capacity_compensation", "amount_cny": 50.0, "delivery_date": "2026-03-01",
                 "amount_receivable_cny": 60.0, "amount_settled_cny": 50.0, "amount_diff_cny": 10.0,
                 "counterparty": "exchange"}]
    with sqlite_engine.begin() as conn:
        sid1 = insert_settlement(conn, 7, "2026-03-01", "a.pdf", "pdf", "h1", pdf_items)
        sid2 = insert_settlement(conn, 7, "2026-03-01", "b.xlsx", "excel", "h2", xl_items)
    with sqlite_engine.connect() as conn:
        rows = conn.execute(text("SELECT settlement_id, category, amount_cny, peak_period, amount_diff_cny "
                                 "FROM marketdata.rm_settlement_items ORDER BY settlement_id")).fetchall()
    assert rows[0][:4] == (sid1, "discharge_energy", 100.0, "peak") and rows[0][4] is None
    assert rows[1][:3] == (sid2, "capacity_compensation", 50.0) and rows[1][4] == 10.0
```

Extend that file's sqlite fixture so `rm_settlement_items` has all columns the INSERT names and `rm_settlements` has `file_name, file_type, status, raw_data`.

- [ ] **Step 2: Run it to verify it fails**

Run: `~/.venvs/bess-platform/bin/python -m pytest tests/asset_risk/test_upload_dedup.py::test_insert_settlement_pdf_and_excel_items -q`
Expected: FAIL, `ModuleNotFoundError: libs.asset_risk.settlement`

- [ ] **Step 3: Create `analytics.py` and `ledger.py` by moving code; implement `insert_settlement`**

- [ ] **Step 4: Thin the tabs**

`tab_settlement.py`: `_process_pdf` and `_process_excel` call `insert_settlement` instead of their inline INSERT loops; `_render_analytics` and `_render_merchant_share` import from `analytics`; module-level re-exports for every name a test imports (`_bucket_of, compute_merchant_shares, _content_sha256, _already_ingested, _insert_year_subtotals, prefer_canonical_settlements`). `tab_pnl.py`: imports `book_matches_classes, ASSET_CLASS_OPTIONS, _portfolio_matrix, _asset_summary, _asset_month_metric, _load_settlement_items, CATEGORY_CN` from `analytics` and re-exports them.

- [ ] **Step 5: Run the suite**

Run: `~/.venvs/bess-platform/bin/python -m pytest tests/asset_risk tests/services/asset_risk -q -p no:cacheprovider`
Expected: `96 passed`

- [ ] **Step 6: Commit**

```bash
git commit -m "asset_risk: move settlement analytics and ledger writes into libs" -- libs/asset_risk/settlement apps/asset_risk/tab_settlement.py apps/asset_risk/tab_pnl.py tests/asset_risk/test_upload_dedup.py
```

### Task 4: Agent tools without Streamlit

**Files:**
- Create: `libs/asset_risk/agent_tools.py`, `tests/asset_risk/test_no_streamlit.py`
- Modify: `apps/asset_risk/tab_agent.py:509-605, 808-920`, `services/asset_risk/headless_agent.py:57`
- Test: existing `tests/asset_risk/test_agent_tools.py`, `tests/services/asset_risk/test_headless_agent.py`

**Interfaces:**
- Produces `libs.asset_risk.agent_tools`: `AGENT_BASE_SYSTEM: str` (today's `_AGENT_BASE_SYSTEM`), `tools: list[dict]`, `execute_tool(name: str, inputs: dict, engine) -> dict` (today's `_execute_tool`).
- `tab_agent.py` re-exports `_AGENT_BASE_SYSTEM = AGENT_BASE_SYSTEM`, `tools`, `_execute_tool = execute_tool`.

- [ ] **Step 1: Write the no-streamlit test**

```python
# tests/asset_risk/test_no_streamlit.py
import importlib, subprocess, sys

MODULES = ["libs.asset_risk.chain", "libs.asset_risk.diagnostics", "libs.asset_risk.waterfall",
           "libs.asset_risk.settlement.analytics", "libs.asset_risk.settlement.ledger",
           "libs.asset_risk.agent_tools", "libs.asset_risk.standard_params"]

def test_libs_asset_risk_imports_without_streamlit():
    code = "import sys; " + "; ".join(f"import {m}" for m in MODULES) + "; sys.exit(1 if 'streamlit' in sys.modules else 0)"
    proc = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True)
    assert proc.returncode == 0, proc.stderr
```

- [ ] **Step 2: Run it to verify it fails**

Run: `~/.venvs/bess-platform/bin/python -m pytest tests/asset_risk/test_no_streamlit.py -q`
Expected: FAIL, `No module named 'libs.asset_risk.agent_tools'`

- [ ] **Step 3: Create `agent_tools.py` by moving `_AGENT_BASE_SYSTEM`, `tools`, `_execute_tool`; re-export from `tab_agent.py`; change `headless_agent.py:57` to `from libs.asset_risk.agent_tools import tools as _tools, execute_tool as _execute_tool`**

- [ ] **Step 4: Run the suite**

Run: `~/.venvs/bess-platform/bin/python -m pytest tests/asset_risk tests/services/asset_risk -q -p no:cacheprovider`
Expected: `97 passed`

- [ ] **Step 5: Commit**

```bash
git commit -m "asset_risk: agent tools importable without streamlit; headless agent uses libs" -- libs/asset_risk/agent_tools.py apps/asset_risk/tab_agent.py services/asset_risk/headless_agent.py tests/asset_risk/test_no_streamlit.py
```

---

## Step 2 — One settlement router

### Task 5: Router fixtures

**Files:**
- Create: `tests/asset_risk/fixtures/router/` with one text file per detector not already covered by `tests/settlement_ingest/fixtures/`: `gansu_bill.txt`, `gansu_charge.txt`, `mengxi_charge_regex.txt` (a text whose only hit is `parse_charging_cost_pdf`'s layout — copy the smallest snippet from `tests/settlement_ingest/test_parser_charge.py`'s fixture), `unknown.txt` ("no detector matches")
- Test: none (fixtures only; Task 6 consumes)

- [ ] **Step 1: Copy the minimal detector-triggering text for each file from the existing parser tests' fixtures or inline strings (read `tests/settlement_ingest/test_parser_gansu.py` and `test_parser_charge.py` to find them)**

- [ ] **Step 2: Verify each fixture triggers exactly its detector**

Run: `~/.venvs/bess-platform/bin/python -c "from pathlib import Path; from services.settlement_ingest.parser_gansu import is_gansu_bill, is_gansu_charge_bill; t=Path('tests/asset_risk/fixtures/router/gansu_charge.txt').read_text(); print(is_gansu_charge_bill(t), is_gansu_bill(t))"`
Expected: `True ...` for its own detector; repeat per fixture.

- [ ] **Step 3: Commit**

```bash
git commit -m "asset_risk: router fixtures" -- tests/asset_risk/fixtures/router
```

### Task 6: `router.py`

**Files:**
- Create: `libs/asset_risk/settlement/router.py`, `tests/asset_risk/test_router.py`
- Test: `tests/asset_risk/test_router.py`

**Interfaces:**
- Produces:

```python
@dataclass(frozen=True)
class BillText:
    filename: str
    path: str                 # temp or real path (parsers that need the PDF take this)
    is_scanned: bool
    full_text: str            # all pages, "\n"-joined ("" when scanned)
    page1_text: str

@dataclass(frozen=True)
class Route:
    label: str                # exact st.info text used today, e.g. "蒙西上网电费结算单 (成分明细) detected — parsed deterministically (no vision)."
    kind: str                 # "discharge" | "charge" | "voucher" | "skip"
    detect: Callable[[BillText], bool]
    parse: Callable[[BillText], list[dict] | dict]   # dict only for the two SDTC routes: {"售电侧": [...], "购电侧": [...]}
    needs_text_layer: bool = True
    upload_only: bool = False # voucher fallback

ROUTES: tuple[Route, ...]
ROUTE_LABELS: tuple[str, ...]   # frozen order, asserted by test

def read_bill(path: str, filename: str) -> BillText
def route(bill: BillText, kind_hint: str, mode: str) -> Route | None
    # kind_hint: classify_pdf(filename) result; mode: "upload" | "scanner"
    # Returns the first Route in ROUTES whose kind matches the branch classify_pdf would take today
    # (discharge | charge, with "unknown" → scanned→discharge-vision, text→charge branch as today),
    # whose needs_text_layer is compatible with bill.is_scanned, whose detect(bill) is True,
    # and (mode == "upload" or not upload_only). None when nothing matches (caller reports as today).
```

Route order (the ladder, top to bottom, exactly as `_process_pdf` evaluates it): discharge branch — voucher fallback (`upload_only`, `kind == "voucher"` hint, parse = `parse_generation_voucher`), guangxi grid duplicate (`kind="skip"`), wb2 voucher (`skip`), wb2 discharge, guangxi discharge, stategrid discharge (page1), SDTC settlement (returns sides dict), AH SDTC (sides dict), AH discharge (full text), local discharge (page1), gansu bill (`parse_gansu_discharge_text`), discharge vision (`needs_text_layer=False`, detect = always). Charge branch — scanned charge vision-verified (`needs_text_layer=False`, detect = `bill.is_scanned`), wb2 charge, guangxi charge, stategrid charge (page1), gansu charge, mengxi charge regex (`parse_charging_cost_pdf(path)`; detect = returns non-empty, so detect runs the parser once and caches on the BillText via a module-level `lru_cache` keyed on path), charge vision generic (`needs_text_layer` irrelevant, detect = always).

- [ ] **Step 1: Write the tests**

```python
# tests/asset_risk/test_router.py
import pathlib, pytest
from libs.asset_risk.settlement.router import BillText, ROUTES, ROUTE_LABELS, route

FIX = pathlib.Path(__file__).parent.parent / "settlement_ingest" / "fixtures"
FIX2 = pathlib.Path(__file__).parent / "fixtures" / "router"

def _bill(text, scanned=False, name="x.pdf"):
    return BillText(filename=name, path="/nonexistent.pdf", is_scanned=scanned, full_text=text, page1_text=text.split("\f")[0])

CASES = [  # (fixture, kind_hint, expected label prefix)
    ("wb2_discharge_202503.txt", "discharge", "蒙西上网电费结算单"),
    ("guangxi_discharge_202504.txt", "discharge", "广西电力交易结算依据"),
    ("guangxi_grid_dup_202504.txt", "discharge", "电网版上网结算单"),
    ("stategrid_discharge_202505.txt", "discharge", "网上国网发电侧电费账单"),
    ("sdtc_202504.txt", "discharge", "山东电力交易中心交易结算单"),
    ("ah_sdtc_202602.txt", "discharge", "安徽电力交易中心(统推)结算单"),
    ("ah_discharge_202609.txt", "discharge", "国网安徽发电侧电费账单"),
    ("stategrid_discharge_local_202501.txt", "discharge", "山东省地方电厂市场化结算单"),
    ("wb2_charge_202503.txt", "charge", "蒙西下网电费核查票"),
    ("guangxi_charge_202504.txt", "charge", "广西电网电费通知单"),
    ("stategrid_charge_202505.txt", "charge", "网上国网用电侧电费账单"),
]

@pytest.mark.parametrize("fixture,hint,label", CASES)
def test_route_picks_expected_parser(fixture, hint, label):
    r = route(_bill((FIX / fixture).read_text(encoding="utf-8")), hint, "upload")
    assert r is not None and r.label.startswith(label)

def test_route_order_is_frozen():
    assert tuple(r.label for r in ROUTES) == ROUTE_LABELS

def test_charge_filename_never_routes_to_discharge_parser():
    # Review Focus 1: a 下网 filename with discharge-looking text stays on the charge branch
    r = route(_bill((FIX / "wb2_discharge_202503.txt").read_text(encoding="utf-8")), "charge", "upload")
    assert r is not None and r.kind == "charge"

def test_scanner_mode_never_gets_voucher_fallback():
    # Review Focus 2
    b = _bill("", scanned=True, name="2026-08 发电侧结算凭证.pdf")
    r = route(b, "voucher", "scanner")
    assert r is None or r.kind == "skip"
    r2 = route(b, "voucher", "upload")
    assert r2 is not None and r2.upload_only

def test_scanned_discharge_goes_to_vision():
    r = route(_bill("", scanned=True), "discharge", "upload")
    assert r is not None and r.needs_text_layer is False and r.kind == "discharge"

def test_unknown_text_charge_falls_through_to_generic_vision():
    r = route(_bill((FIX2 / "unknown.txt").read_text(encoding="utf-8")), "charge", "upload")
    assert r is not None and "Vision" in r.label
```

- [ ] **Step 2: Run to verify failure**

Run: `~/.venvs/bess-platform/bin/python -m pytest tests/asset_risk/test_router.py -q`
Expected: FAIL, `ModuleNotFoundError`

- [ ] **Step 3: Implement `router.py`**

`detect` callables wrap today's `is_*` functions on `bill.full_text` (or `page1_text` where today's code uses page 1). The label strings are copied verbatim from the `st.info(...)` calls in `tab_settlement.py` lines 350–474 so the UI text does not change. The gansu discharge/charge detectors keep the asymmetry present today (`is_gansu_bill` on the discharge branch, `is_gansu_charge_bill` on the charge branch).

- [ ] **Step 4: Run tests**

Run: `~/.venvs/bess-platform/bin/python -m pytest tests/asset_risk/test_router.py -q`
Expected: all pass

- [ ] **Step 5: Commit**

```bash
git commit -m "asset_risk: settlement parser router table with ordered routes and tests" -- libs/asset_risk/settlement/router.py tests/asset_risk/test_router.py
```

### Task 7: Tab and scanner use the router

**Files:**
- Modify: `apps/asset_risk/tab_settlement.py` `_process_pdf` (lines 272–530 today), `services/settlement_ingest/scanner.py:252-376`
- Test: `tests/asset_risk/test_router.py` (add parity test), existing `tests/settlement_ingest/*`

- [ ] **Step 1: Write the parity test**

```python
def test_tab_and_scanner_share_route_table():
    # Both callers go through route(); the scanner must see every non-upload-only route the tab sees.
    from libs.asset_risk.settlement.router import ROUTES
    tab_labels = [r.label for r in ROUTES]
    scanner_labels = [r.label for r in ROUTES if not r.upload_only]
    assert len(tab_labels) - len(scanner_labels) == 1   # exactly the voucher fallback
```

- [ ] **Step 2: Rewrite `_process_pdf`**

Structure: classify → voucher pre-check as today (the `voucher_fallback` decision needs `_has_category`, stays in the tab) → `bill = read_bill(tmp_path, uploaded.name)` → `r = route(bill, kind, "upload")` → if `r is None`: today's "No settlement items extracted" warning → if `r.kind == "skip"`: `st.info(r.label)` and return → `items = r.parse(bill)`; if `items` is a sides dict, apply today's SDTC/AH-SDTC rules (`售电侧` skipped when `_has_category(discharge_energy)` for SDTC; both kept for AH) → `st.info(r.label)` → `split_insertable` → write via `ledger` as in Task 3. The `is_scanned` info message for vision stays.

- [ ] **Step 3: Rewrite the scanner's detection block (lines 252–376) to `bill = read_bill(str(pdf_path), pdf_path.name); r = route(bill, pdf_type, "scanner")`, keeping its `voucher`/`capcomp_table`/unknown handling, its `_has_discharge_items` SDTC rule, and `_write_settlement` untouched**

- [ ] **Step 4: Run all suites**

Run: `~/.venvs/bess-platform/bin/python -m pytest tests/asset_risk tests/services/asset_risk tests/settlement_ingest tests/settlement -q -p no:cacheprovider`
Expected: all pass (asset_risk count = Task 6 count + 1 for the parity test; no failures, no skips)

- [ ] **Step 5: Commit**

```bash
git commit -m "asset_risk: upload tab and folder scanner route through one parser table" -- apps/asset_risk/tab_settlement.py services/settlement_ingest/scanner.py tests/asset_risk/test_router.py
```

### Task 8: Deploy A (steps 1–2) — **STOP for confirmation**

**Files:**
- Modify: `LATEST-IMAGES.md` (asset_risk row + history line), `infra/terraform/terraform.tfvars` (image tag only; never staged)

- [ ] **Step 1: Pre-deploy checks** — read `LATEST-IMAGES.md`; next tag = max+1 (currently v98); `git status` clean on `~/bess-platform-local`; build from `git archive HEAD` context.

- [ ] **Step 2: STOP. Ask the user: "Deploy asset-risk vN (steps 1–2, no behaviour change) to ECS now?" Proceed only on an explicit yes in that message.**

- [ ] **Step 3: Build, verify, push**

```bash
docker build --platform linux/amd64 -f apps/asset_risk/Dockerfile -t bess-asset-risk:vN .
docker run --rm bess-asset-risk:vN python -c "import libs.asset_risk.settlement.router, libs.asset_risk.waterfall; import sqlalchemy; print('ok')"
docker run --rm bess-asset-risk:vN ls libs/asset_risk/settlement          # router.py present
# tag/push both names; docker buildx imagetools inspect ... | grep Platform → linux/amd64; describe-images digest check
```

- [ ] **Step 4: Register td from the service's current tdArn (jq-swap image), update-service, wait for rollout COMPLETED, ALB 200 on `/asset-risk/_stcore/health`, then open the Settlement tab and upload one known 蒙西 bill with Overwrite on: item count equals the previous ingest for that file**

- [ ] **Step 5: Append the deploy line to `LATEST-IMAGES.md`; commit**

```bash
git commit -m "Log asset-risk vN deploy (libs extraction + router)" -- LATEST-IMAGES.md
```

---

## Step 3 — Registry unification

### Task 9: `StationRef.attribution_code`, `asset_identity`, migration + probe scripts

**Files:**
- Modify: `libs/trading_book/registry.py:30-43` (add `attribution_code: str | None = None`; set `"hetao"` on `bameng`), `libs/trading_book/waterfall.py:66-75` (`_ATTR_CODE_ALIAS` built from registry: `{s.asset_code: s.attribution_code for s in get_stations() if s.attribution_code}`)
- Create: `libs/asset_risk/registry.py`, `scripts/asset_risk/migrate_asset_code.py`, `scripts/asset_risk/probe_identity.py`, `tests/asset_risk/test_registry.py`
- Test: `tests/asset_risk/test_registry.py`, existing `tests/trading_book/*`

**Interfaces:**
- Produces `libs.asset_risk.registry`:

```python
IDENTITY_COLUMNS = ["asset_id", "name", "asset_code", "asset_type", "plant_names", "capcomp_rate",
                    "invoice_folder", "capacity_mw", "bess_duration_h", "commission_date"]
SEED_CODES: dict[str, str] = {   # rm_assets.name -> registry asset_code, used by the migration UPDATE
    "悦杭独贵": "hangjinqi", "景怡查干哈达": "bameng", "景蓝乌尔图": "suyou", "裕昭沙子坝": "gushanliang",
    "远景乌拉特": "wulate", "四子王旗": "siziwangqi", "武川": "wuchuan", "阿拉善储能": "alashan",
    "零碳46风电": "lingt46", "乌海康富": "wuhai"}
CAPCOMP_DEFAULT = 350.0

def asset_identity(engine) -> pd.DataFrame          # one row per rm_assets row; registry fields NULL/default when asset_code is NULL
def plant_map(identity: pd.DataFrame) -> dict[str, tuple[str, ...]]      # name -> plant_names (only rows with a code)
def nom_code_map(identity: pd.DataFrame) -> dict[str, str]              # name -> asset_code
def capcomp_rate_map(identity: pd.DataFrame) -> dict[str, float]        # name -> rate (all rows)
def rm_name_map(identity: pd.DataFrame) -> dict[str, str]               # asset_code -> name (replaces _RM_ASSET_NAME)
```

`capcomp_rate` = `StationRef.subsidy_cny_per_mwh` if not None else 350.0. `plant_names` = `StationRef.cleared_names`.

- [ ] **Step 1: Write tests**

```python
# tests/asset_risk/test_registry.py  (sqlite-ATTACH fixture with rm_assets incl. asset_code column)
def test_identity_derives_registry_fields(sqlite_engine):
    df = asset_identity(sqlite_engine)
    wc = df[df.name == "武川"].iloc[0]
    assert wc.asset_code == "wuchuan" and wc.plant_names == ("旭纯哈乐储能电站", "旭纯哈乐储能电站二") and wc.capcomp_rate == 280.0
    al = df[df.name == "阿拉善储能"].iloc[0]
    assert al.capcomp_rate == 280.0
    ls = df[df.name == "灵山"].iloc[0]
    assert ls.asset_code is None and ls.plant_names == () and ls.capcomp_rate == 350.0

def test_maps_match_legacy_for_six_stations(sqlite_engine):
    from libs.asset_risk.diagnostics import PLANT_MAP, AUTO_NOM_CODE
    df = asset_identity(sqlite_engine)
    assert {k: v[0] for k, v in plant_map(df).items() if k in PLANT_MAP} == PLANT_MAP
    assert {k: v for k, v in nom_code_map(df).items() if k in AUTO_NOM_CODE} == AUTO_NOM_CODE

def test_attribution_alias_from_registry():
    from libs.trading_book.waterfall import _ATTR_CODE_ALIAS
    assert _ATTR_CODE_ALIAS == {"bameng": "hetao"}
```

Fixture rows: the six legacy stations, 武川, 阿拉善储能, 零碳46风电, 灵山 (code NULL).

- [ ] **Step 2: Run to verify failure** — `pytest tests/asset_risk/test_registry.py -q` → `ModuleNotFoundError`

- [ ] **Step 3: Implement `registry.py`; add `attribution_code` to `StationRef`; derive `_ATTR_CODE_ALIAS`**

- [ ] **Step 4: Write `scripts/asset_risk/migrate_asset_code.py`**

Reads `PGURL`; runs `ALTER TABLE marketdata.rm_assets ADD COLUMN IF NOT EXISTS asset_code TEXT UNIQUE`; then for each `(name, code)` in `SEED_CODES`: `UPDATE marketdata.rm_assets SET asset_code = :code WHERE name = :name AND asset_code IS NULL`; prints rows updated per name and the final `SELECT name, asset_code FROM rm_assets ORDER BY id`. Idempotent. Takes `--dry-run` (prints the plan, no writes).

- [ ] **Step 5: Write `scripts/asset_risk/probe_identity.py`**

Prints `asset_identity(engine)` and a diff against the legacy `PLANT_MAP`, `AUTO_NOM_CODE`, `CAPCOMP_RATE`, `_RM_ASSET_NAME`, `folder_mapper._FALLBACK_MAP`, `_ATTR_CODE_ALIAS`; expected printed exception list is exactly: `阿拉善储能 capcomp 350→280`, `武川 (new)`, `阿拉善储能 (new)`, and the NULL-code books. Any other line is a defect.

- [ ] **Step 6: Run tests** — `pytest tests/asset_risk tests/trading_book -q` → all pass

- [ ] **Step 7: Commit**

```bash
git commit -m "asset_risk: registry-derived asset identity, attribution_code on StationRef, migration + probe scripts" -- libs/asset_risk/registry.py libs/trading_book/registry.py libs/trading_book/waterfall.py scripts/asset_risk tests/asset_risk/test_registry.py
```

### Task 10: Switch consumers to the identity frame

**Files:**
- Modify: `libs/asset_risk/diagnostics.py` (constants become thin: `PLANT_MAP`, `AUTO_NOM_CODE`, `CAPCOMP_RATE` kept as names but populated from `asset_identity` at call time via `load_maps(engine)`), `libs/asset_risk/chain.py` (`_load_auto_nominations` and `_attach_prices` callers pass maps from the frame; multi-name plant tuples handled in Task 11), `apps/asset_risk/tab_diagnostics.py`, `apps/asset_risk/tab_waterfall.py` (`_load_assets` filters on `asset_code IS NOT NULL AND asset_type='bess'` instead of `name in PLANT_MAP`), `apps/asset_risk/tab_wind_replication.py:22-24` (constants resolved from the `lingt46` row: `BOOK_ID` via `rm_books.asset_id`, `PLANT` = `plant_names[0]`), `services/mengxi_nodal/settlement_bridge.py:69-80` (`_RM_ASSET_NAME` → `rm_name_map(asset_identity(engine))` at call time; keep the dict name as a module-level fallback used only when the DB has no `asset_code` column yet — removed in Task 12 after migration)
- Test: existing suites; `tests/asset_risk/test_dispatch_diagnostics.py` fixture gains an `asset_code` column

**Interfaces:**
- Produces `libs.asset_risk.diagnostics.load_maps(engine) -> tuple[dict, dict, dict]` returning `(plant_map_first_name, nom_code_map, capcomp_rate_map)`; `capacity_loss_monthly`, `restriction_monthly`, `stage_capacity` take `rate_map` from it (they already accept `rate_map` / default-rate parameters; `stage_capacity` gains `rate_map: dict[str, float]` as a required argument).

- [ ] **Step 1: Update `test_dispatch_diagnostics.py` and `test_waterfall.py` fixtures to include `asset_code` and pass explicit `rate_map` where `stage_capacity` is called; run to see failures on the new signature**

- [ ] **Step 2: Implement the switch; `PLANT_MAP`/`AUTO_NOM_CODE`/`CAPCOMP_RATE` module constants remain as the legacy literal dicts for test parity but are no longer read by any render path**

- [ ] **Step 3: Run suites** — `pytest tests/asset_risk tests/mengxi_nodal tests/trading_book -q` → all pass

- [ ] **Step 4: Commit**

```bash
git commit -m "asset_risk: diagnostics/waterfall/wind/settlement-bridge read identity from registry frame" -- libs/asset_risk apps/asset_risk services/mengxi_nodal/settlement_bridge.py tests/asset_risk
```

---

## Step 4 — Chain frame with actuals

### Task 11: `load_chain` over nominations, multi-account cleared, WeCom actuals

**Files:**
- Modify: `libs/asset_risk/chain.py` (replace `_load_auto_nominations` + `_merge_auto_nominations` with the loader below; keep `_load_chain` name as the public entry so tabs/tests do not change)
- Create: `tests/asset_risk/test_chain_loader.py`
- Test: new file + existing `test_dispatch_diagnostics.py`

**Interfaces:**
- `_load_chain(engine, asset_ids, start, end) -> pd.DataFrame` unchanged signature; columns unchanged (`asset_id, asset, capacity_mw, ts, nominated_mw, da_cleared_mw, rt_cleared_mw, actual_mw, restriction, soc_pct`).
- Sources and priority per spec §9: legacy `rm_dispatch_chain` first; `bess_nomination_96` overrides `nominated_mw` on overlap and extends; `md_id_cleared_energy` summed over `plant_names` fills `rt_cleared_mw` where NaN (exact names, `data_date BETWEEN start::date AND end::date` when bounds are given); `bess_actual_96` fills `actual_mw` where NaN, then `ops_bess_dispatch_15min.actual_dispatch_mw` (keyed by `asset_code`, `interval_start`) where still NaN.
- Guards: per (asset, month) `sign(Σ actual_mw·nominated_mw) > 0` over slots where both non-null, else `raise ValueError(f"actual/nominated sign disagreement {asset} {month}")`; per (asset, month) with billed discharge available (caller passes none here — the loader only checks the sign; the 3× cleared-vs-billed assertion lives in `tab_waterfall` next to `guard_cleared_leg` and raises instead of downgrading when ratio > 3).

- [ ] **Step 1: Write tests (sqlite-ATTACH; tables `rm_assets(+asset_code)`, `rm_dispatch_chain`, `bess_nomination_96`, `bess_actual_96`, `md_id_cleared_energy(+data_date)`, `ops_bess_dispatch_15min`)**

```python
def test_actuals_fill_from_bess_actual_96_then_ops(sqlite_engine):
    df = _load_chain(sqlite_engine, [1], "2026-09-01", "2026-09-02")
    # slot 00:00 has legacy actual → kept; 00:15 only in bess_actual_96 → 12.0; 00:30 only in ops → 7.5
    assert df.set_index("ts").loc["2026-09-01 00:00:00", "actual_mw"] == 5.0
    assert df.set_index("ts").loc["2026-09-01 00:15:00", "actual_mw"] == 12.0
    assert df.set_index("ts").loc["2026-09-01 00:30:00", "actual_mw"] == 7.5

def test_multi_account_cleared_sums_present_rows(sqlite_engine):
    # Review Focus 4: 武川 two accounts; slot A has both (10 + 15), slot B has one (−4)
    df = _load_chain(sqlite_engine, [3], "2026-09-01", "2026-09-02").set_index("ts")
    assert df.loc["2026-09-01 01:00:00", "rt_cleared_mw"] == 25.0
    assert df.loc["2026-09-01 01:15:00", "rt_cleared_mw"] == -4.0

def test_asset_without_code_returns_legacy_only(sqlite_engine):
    # Review Focus 3
    df = _load_chain(sqlite_engine, [9], None, None)
    assert len(df) == 2 and df["nominated_mw"].notna().all()

def test_sign_disagreement_raises(sqlite_engine_bad_sign):
    with pytest.raises(ValueError, match="sign disagreement"):
        _load_chain(sqlite_engine_bad_sign, [1], None, None)

def test_no_times_four_on_cleared(sqlite_engine):
    df = _load_chain(sqlite_engine, [1], "2026-09-01", "2026-09-02").set_index("ts")
    assert df.loc["2026-09-01 02:00:00", "rt_cleared_mw"] == 3.0   # stored 3.0 → 3.0
```

- [ ] **Step 2: Run to verify failure** — new tests fail on missing fixture tables / wrong values

- [ ] **Step 3: Implement the loader**

Build four frames keyed on `(asset_id, ts)` and `combine_first` in priority order; `ts` for the 96-slot tables = `data_date + (slot−1)·15min`; for `ops_bess_dispatch_15min` = `interval_start` (CST-naive). The sqlite dialect branch keeps TEXT timestamps as today.

- [ ] **Step 4: Add the 3× cleared-vs-billed assertion in `tab_waterfall.render_waterfall` directly before `guard_cleared_leg`: if any (asset, month) has `cleared.dis_mwh / actual.dis_mwh > 3`, `st.error` naming them and `st.stop()`**

- [ ] **Step 5: Run suites** — `pytest tests/asset_risk -q` → all pass

- [ ] **Step 6: Commit**

```bash
git commit -m "asset_risk: chain loader reads WeCom actuals, sums multi-account cleared, asserts sign and units" -- libs/asset_risk/chain.py apps/asset_risk/tab_waterfall.py tests/asset_risk/test_chain_loader.py tests/asset_risk/test_dispatch_diagnostics.py
```

### Task 12: Migration + Deploy B (steps 3–4) — **STOP for confirmation twice**

- [ ] **Step 1: STOP. Ask: "Run the rm_assets.asset_code migration on RDS via ECS run-task now (ALTER + seed UPDATE, idempotent, dry-run output attached)?" Attach the `--dry-run` output first. Proceed only on an explicit yes.**

- [ ] **Step 2: Run the migration via the one-off Fargate pattern (asset-risk td, NAT network config, base64 script in env `S`); read the CloudWatch stream `asset-risk/asset-risk/<task-id>`; expected: 10 names updated, final listing shows every seeded code**

- [ ] **Step 3: Run `probe_identity.py` the same way; expected exception list exactly as Task 9 Step 5; anything else → fix before deploying**

- [ ] **Step 4: Remove the `_RM_ASSET_NAME` module fallback from `settlement_bridge.py`; run `tests/mengxi_nodal`; commit**

- [ ] **Step 5: STOP. Ask: "Deploy asset-risk vN+1 (registry + actuals; 阿拉善 rate becomes 280, 武川/阿拉善 appear) now?"**

- [ ] **Step 6: Build/verify/push/jq-swap as Task 8; post-deploy: Dispatch Diagnostics coverage table shows 武川 and 阿拉善 rows; Waterfall 偏差归因 for 2026-11 shows ¥ values (not 无数据) on the Δ执行 rows for any asset with ≥20 actual days; log to `LATEST-IMAGES.md`; commit**

---

## Step 5 — Agent tools, tab retirement, Hermes

### Task 13: Reshape agent tools

**Files:**
- Modify: `libs/asset_risk/agent_tools.py`, `services/asset_risk/headless_agent.py:28-40` (`_SYSTEM` replaced by `AGENT_BASE_SYSTEM` + date prefix)
- Test: `tests/asset_risk/test_agent_tools.py`

**Interfaces:**
- `tools` names after this task: `get_asset_list`, `get_book_pnl`, `get_settlement_summary`, `get_deviation_analysis`, `get_waterfall`, `get_diagnostics`, `get_merchant_share`. Removed: `get_position_mtm`, `get_var`.
- `get_waterfall(asset: str, months: list[str])` → `{"stage_totals": {stage: cny}, "attribution": [rows of attribution_rows without the _fmt columns]}`; computed with `libs.asset_risk.waterfall` functions and `benchmark_leg`/`forecast_optimal_leg` (uncached in libs; Task 16 swaps in the legs store).
- `get_diagnostics(asset: str, start_date: str, end_date: str)` → `{"exec_gap": exec_gap_monthly rows, "bid_fail": ..., "restriction": ..., "defect_events": top 20}`.
- `get_merchant_share(book_id: int)` → `{"monthly": compute_merchant_shares monthly rows, "latest": latest row or None}`.
- `AGENT_BASE_SYSTEM` analytical framework paragraph gains one line per new tool and loses the MtM/VaR lines.

- [ ] **Step 1: Update `test_agent_tools.py`: the tool-inventory test asserts the exact seven names; add `test_get_merchant_share_tool` (two months of items → `latest["merchant_share"]` equals the hand-computed ratio) and `test_get_diagnostics_tool_shape` (keys present, rows list)** — run, see failures

- [ ] **Step 2: Implement; delete the two removed branches from `execute_tool`**

- [ ] **Step 3: `headless_agent.py`: build `system = date_prefix + AGENT_BASE_SYSTEM` (+ expert-memory block as today); delete the local `_SYSTEM`**

- [ ] **Step 4: Run** — `pytest tests/asset_risk tests/services/asset_risk -q` → pass

- [ ] **Step 5: Commit**

```bash
git commit -m "asset_risk agent: waterfall/diagnostics/merchant-share tools; drop mtm/var; shared grounding prompt" -- libs/asset_risk/agent_tools.py services/asset_risk/headless_agent.py tests/asset_risk/test_agent_tools.py
```

### Task 14: Retire position and VaR tabs in both apps

**Files:**
- Delete: `apps/asset_risk/tab_positions.py`, `apps/asset_risk/tab_var.py`, `apps/retail_risk/tab_positions.py`, `apps/retail_risk/tab_var.py`
- Modify: `apps/asset_risk/app.py:52-90` (6 tabs: Asset Config, Settlement, Realised P&L, Agent, Dispatch Diagnostics, 收益瀑布), `apps/retail_risk/app.py:54-88` (6 tabs: CRM, Settlement, Realised P&L, Reconciliation, Data Upload, Agent), `apps/retail_risk/tab_pnl.py:99-106` (SELECT and chart drop `unrealized_mtm_cny`)
- Test: `tests/asset_risk/test_app_tabs.py` (new, small)

- [ ] **Step 1: Write a test that parses each `app.py` with `ast` and asserts the `st.tabs([...])` literal equals the six expected labels, and that neither file references `tab_positions` or `tab_var`**

- [ ] **Step 2: Apply the deletions and edits; run `grep -rn "tab_positions\|tab_var" apps/asset_risk apps/retail_risk` → no hits; run `pytest tests/asset_risk tests/retail_risk -q` → pass**

- [ ] **Step 3: Commit**

```bash
git commit -m "Retire Positions & MtM / VaR & Greeks tabs from asset-risk and retail-risk (moving to market-maker)" -- apps/asset_risk/app.py apps/asset_risk/tab_positions.py apps/asset_risk/tab_var.py apps/retail_risk/app.py apps/retail_risk/tab_positions.py apps/retail_risk/tab_var.py apps/retail_risk/tab_pnl.py tests/asset_risk/test_app_tabs.py
```

### Task 15: Deploy C — asset-risk, retail-risk, hermes — **STOP for confirmation**

**Files:**
- Modify: `apps/hermes-service/Dockerfile:52` (add `COPY libs/ /app/libs/` after `COPY services/`), `LATEST-IMAGES.md`

- [ ] **Step 1: Confirm the current hermes image lacks `libs/` and `apps/` (`aws ecs describe-task-definition` → image; `docker run --rm <image> ls /app` is not possible from the Mac — instead inspect via one-off run-task `ls /app`); record the finding in the deploy note: the asset-risk and retail-risk Hermes bridges have been importing `apps.*` that the image does not contain**

- [ ] **Step 2: Add the `COPY libs/` line; local check that the root `.dockerignore` lets through `libs/asset_risk`, `libs/trading_book`, `libs/risk`, `libs/settlement`: `git archive HEAD | tar -t | grep '^libs/asset_risk/'`**

- [ ] **Step 3: STOP. Ask: "Deploy asset-risk vN+2, retail-risk vM, and a hermes rebuild (adds libs/ to the image; 31 env vars preserved) now?"**

- [ ] **Step 4: asset-risk and retail-risk: build/verify/push/jq-swap as Task 8. Hermes: CodeBuild delta build via `scripts/cloud_build.sh <ctx> bess-platform-hermes <tag> --smoke "python -c 'import libs.asset_risk.agent_tools, services.asset_risk.headless_agent'"`; jq-swap from hermes's current tdArn; `aws ecs describe-task-definition --task-definition <new> --query 'length(taskDefinition.containerDefinitions[0].environment)'` → 31 before and after**

- [ ] **Step 5: Verify via Hermes: send the bridge a question routed to `asset-risk` ("查干哈达 2026-10 收益瀑布?") and confirm the answer cites stage totals; log all three deploys in `LATEST-IMAGES.md`; commit**

---

## Step 6 — Persisted legs and lazy tabs

### Task 16: `rm_waterfall_legs` + `legs_store`

**Files:**
- Create: `libs/asset_risk/legs_store.py`, `tests/asset_risk/test_legs_store.py`
- Modify: `apps/asset_risk/tab_waterfall.py` (`_solve` calls `get_or_compute`; the `st.cache_data` wrappers from Task 2 are removed; a `重算所选月份` button sets `force=True`), `libs/asset_risk/agent_tools.py` (`get_waterfall` uses `get_or_compute`)
- Test: `tests/asset_risk/test_legs_store.py`

**Interfaces:**

```python
DDL = """CREATE TABLE IF NOT EXISTS marketdata.rm_waterfall_legs (
  asset_id INT NOT NULL, month TEXT NOT NULL, leg TEXT NOT NULL CHECK (leg IN ('benchmark','forecast')),
  arb_cny DOUBLE PRECISION, dis_mwh DOUBLE PRECISION, chg_mwh DOUBLE PRECISION, cap_cny DOUBLE PRECISION,
  days_solved INT, days_skipped INT, age INT, note TEXT, price_source TEXT,
  computed_at TIMESTAMPTZ DEFAULT NOW(), PRIMARY KEY (asset_id, month, leg))"""
def ensure_table(engine) -> None
def is_month_complete(month: str, today: date) -> bool        # month_end + 3 days < today
def get_or_compute(engine, leg: str, asset_id: int, month: str, compute: Callable[[], dict],
                   force: bool = False, today: date | None = None) -> dict
```

`get_or_compute`: if not `force` and a row exists → return it as a dict (same keys as `benchmark_leg` output); else call `compute()`; if `is_month_complete(month, today)` upsert the result; return it. `price_source` stores `note` ("节点填充电价" / "").

- [ ] **Step 1: Write tests**

```python
def test_incomplete_month_is_computed_but_not_stored(sqlite_engine):
    calls = []
    out = get_or_compute(sqlite_engine, "benchmark", 1, "2026-11", lambda: calls.append(1) or {"arb_cny": 1.0, "dis_mwh": 2.0, "chg_mwh": 2.1, "cap_cny": 0.7, "days_solved": 10, "days_skipped": 20, "age": 1, "note": ""},
                         today=date(2026, 11, 14))
    assert out["arb_cny"] == 1.0 and calls == [1]
    assert _row_count(sqlite_engine) == 0

def test_complete_month_is_stored_then_served_without_recompute(sqlite_engine):
    calls = []
    f = lambda: calls.append(1) or {"arb_cny": 5.0, "dis_mwh": 1, "chg_mwh": 1, "cap_cny": 1, "days_solved": 30, "days_skipped": 0, "age": 0, "note": "节点填充电价"}
    get_or_compute(sqlite_engine, "benchmark", 1, "2026-09", f, today=date(2026, 11, 14))
    out = get_or_compute(sqlite_engine, "benchmark", 1, "2026-09", f, today=date(2026, 11, 14))
    assert calls == [1] and out["note"] == "节点填充电价" and _row_count(sqlite_engine) == 1

def test_force_recomputes_and_overwrites(sqlite_engine):
    get_or_compute(sqlite_engine, "forecast", 1, "2026-09", lambda: {"arb_cny": 1.0}, today=date(2026, 11, 14))
    out = get_or_compute(sqlite_engine, "forecast", 1, "2026-09", lambda: {"arb_cny": 9.0}, force=True, today=date(2026, 11, 14))
    assert out["arb_cny"] == 9.0 and _stored(sqlite_engine, 1, "2026-09", "forecast")["arb_cny"] == 9.0
```

- [ ] **Step 2: Run to verify failure; implement; run to pass**

- [ ] **Step 3: Wire the tab and the agent tool; `ensure_table(engine)` is called once in `render_waterfall` behind `st.cache_resource`**

- [ ] **Step 4: Run** — `pytest tests/asset_risk -q` → pass

- [ ] **Step 5: Commit**

```bash
git commit -m "asset_risk: persist benchmark/forecast legs per asset-month; 重算 button" -- libs/asset_risk/legs_store.py apps/asset_risk/tab_waterfall.py libs/asset_risk/agent_tools.py tests/asset_risk/test_legs_store.py
```

### Task 17: Lazy tabs

**Files:**
- Modify: `apps/asset_risk/app.py`
- Test: `tests/asset_risk/test_app_tabs.py` (extend)

- [ ] **Step 1: Extend the ast test: `app.py` must contain exactly one `st.radio(` or `st.tabs(` navigation call and no `with tabN:` blocks that import render functions unconditionally (assert every `render_*` import is inside an `if` whose test compares the selected label)**

- [ ] **Step 2: Implement: `page = st.radio("", LABELS, horizontal=True, key="asset_risk_page", label_visibility="collapsed")` then one `if page == label:` block per tab importing and calling its renderer. Only the selected block runs.**

- [ ] **Step 3: Run locally `streamlit run apps/asset_risk/app.py --server.port 8512` with `PGURL` set; switching pages must not trigger the waterfall LP unless the waterfall page is selected (observe the status widget); run tests; commit**

```bash
git commit -m "asset_risk: lazy page rendering (only the selected tab computes)" -- apps/asset_risk/app.py tests/asset_risk/test_app_tabs.py
```

### Task 18: Deploy D and close-out — **STOP for confirmation**

- [ ] **Step 1: STOP. Ask: "Deploy asset-risk vN+3 (persisted legs + lazy tabs; creates marketdata.rm_waterfall_legs on first render) now?"**

- [ ] **Step 2: Build/verify/push/jq-swap as Task 8; post-deploy: open 收益瀑布 for 全部资产 × 3 complete months twice; second load < 5 s; `SELECT COUNT(*) FROM marketdata.rm_waterfall_legs` via run-task ≥ 2 × assets × months**

- [ ] **Step 3: Update `LATEST-IMAGES.md`; update `CLAUDE.md` service table image row for asset-risk and retail-risk; append MEMORY.md decisions: 武川/阿拉善 in scope (supersedes 2026-11-11 deferral), 阿拉善 280, position tabs moved to market-maker; commit**

```bash
git commit -m "Log asset-risk restructure deploys; memory: 武川/阿拉善 in scope, tabs moved to market-maker" -- LATEST-IMAGES.md CLAUDE.md MEMORY.md
```

---

## Self-review notes

- Spec §6 target tree: every module has a task (chain/diagnostics/standard_params T1, waterfall T2, analytics/ledger T3, agent_tools T4/T13, router T6, registry T9, legs_store T16). `apps/asset_risk/app.py` lazy render T17. Retail removals T14. Hermes T15. Migration T12.
- Spec §7 registry: `attribution_code` on `StationRef` replaces the "alias stays, now in the registry" sentence; `folder_mapper` untouched per the my-call paragraph.
- Spec §9 unit/sign guards: T11 (sign) and T11 Step 4 (3× in tab). `da_cleared_mw` has no new source — documented in spec §16.
- Spec §10 tool list: T13 matches exactly.
- Spec §11 legs: T16 `is_month_complete` = month_end + 3 days < today.
- Type consistency: `_load_chain` keeps its name throughout (spec calls it `load_chain`; the public alias `load_chain = _load_chain` is added in T11 so both names resolve). `execute_tool`/`_execute_tool` and `AGENT_BASE_SYSTEM`/`_AGENT_BASE_SYSTEM` pairs are re-exports defined in T4.
- Review Focus 1–5 each have a named test (T6, T6, T11, T11, T16).
