# Market-Maker Phase A+B — Central Risk Book Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** One position register, one reference curve and one risk engine across asset books and retail books, shown in a new `apps/market_maker` Streamlit app (Positions, Exposure & MtM, VaR & Greeks, Curves, Risk Agent).

**Architecture:** Three pure libraries (`libs/positions`, `libs/curves`, extended `libs/risk`) that take frames in and return frames out, with thin DB stores beside them. A refresh job regenerates physical positions and reference curves; the app only reads. The simulated venue (spec phases C–F) is a separate plan and reuses these libraries over a `venue.*` store.

**Tech Stack:** Python 3.11, pandas, SQLAlchemy (Postgres prod, sqlite-ATTACH tests), Streamlit, Plotly, `libs/options` (Black-76), `libs/decision_models/bess_spread_call_strip.py` (Margrabe strip), ECS Fargate + ALB + Cognito, Terraform.

**Spec:** `docs/superpowers/specs/2026-11-14-market-maker-venue-design.md` (§1–8, §11, §13 phases A–B, §16)

## Global Constraints

- `direction` is the risk side: `buy` = long power, `sell` = short. Delta = +volume for buy, −volume for sell. Assets are long (physical rows `buy`); load is short (`sell`).
- Volume is MWh over the delivery period; prices and curve points are ¥/MWh. `price_cny_kwh` columns convert ×1000 at the store boundary only.
- Physical rows are generated, never user-edited: `source='physical'`.
- BESS optionality is not a volume row; it enters Greeks only.
- `libs/positions`, `libs/curves`, `libs/risk` contain no DB access except in files named `store.py`; none imports streamlit or `apps.*`.
- Schema evolution on `rm_positions` / `rm_forward_curves` is additive only (new nullable columns, widened CHECKs); existing readers (`services/retail_risk`, `services/deal_committee`) must keep working.
- A session/venue never writes to `rm_positions` (out of scope here; do not add `session_id` to it).
- Agent pattern per CLAUDE.md: prompt opens with the grounding rule; memory app key `market_maker`; Haiku auto-extract; tools return JSON-serialisable dicts; tool errors returned as data.
- Every deploy, DB migration and Terraform apply requires the user's explicit "yes" in the current message (**STOP** steps). Commit/push only from `~/bess-platform-local`, explicit paths, never `terraform.tfvars`.
- Builds via `scripts/cloud_build.sh` (CodeBuild), amd64 verified; new services get a full (non-delta) first build.

## Review Focus

1. A book whose province has no curve point for a delivery month: exposure must show the MWh and a blank mark/MtM for that month, never ¥0 and never a crash. Test in Task 6.
2. A retail book with only indexed (unpriced) customer contracts: physical `sell` rows with NULL price must give exposure but contribute no MtM. Test in Task 6.
3. An asset with under 30 days of history (武川, 阿拉善): expected-volume generation must return rows flagged `basis='insufficient_history'` with what data exists scaled to the month, not silently zero. Test in Task 4.
4. Province names differ across tables (`inner_mongolia_mengxi`, `蒙西`, `内蒙古`): every join goes through one canonical mapper; an unmapped name raises with the name in the message. Test in Task 1.
5. Refresh run twice in a row must leave the same physical rows (idempotent replace per book), and must not touch `source='bilateral'`/`'manual'` rows. Test in Task 5.

---

## File map

| Path | Responsibility |
|---|---|
| `libs/positions/conventions.py` | sign rule, TOU blocks, province canonicalisation, `Position` dataclass |
| `libs/positions/physical.py` | pure generators of physical rows (wind/solar, BESS, load) |
| `libs/positions/store.py` | read/replace rows in a positions table (`rm_positions` here) |
| `libs/curves/curve.py` | `Curve` frame contract, `spot_seasonal`, `traded_points`, `reference_curve` |
| `libs/curves/store.py` | read/write `rm_forward_curves` in the curve frame shape |
| `libs/risk/exposure.py` | `exposure()` and curve-aware MtM |
| `libs/risk/var.py` | add `portfolio_var()` over monthly deltas (existing functions kept) |
| `libs/risk/greeks.py` | add `option_greeks()`; existing `compute_book_greeks` kept |
| `libs/risk/bess_greeks.py` | per-month BESS delta/vega from the Margrabe strip |
| `services/market_maker/refresh.py` | job: regenerate physical rows + reference curves |
| `services/market_maker/agent_tools.py`, `headless_agent.py` | risk agent tools (no streamlit) |
| `apps/market_maker/app.py`, `page_*.py`, `Dockerfile`, `requirements.txt` | Streamlit app, lazy pages |
| `db/ddl/marketdata/market_maker_migration.sql`, `scripts/market_maker/migrate.py` | additive migration |
| `infra/terraform/market_maker.tf`, `variables.tf` | ECS service, target group, listener rule, ECR |
| `tests/positions/`, `tests/curves/`, `tests/risk/`, `tests/market_maker/` | tests |

---

### Task 1: Conventions, TOU blocks, province mapper

**Files:**
- Create: `libs/positions/__init__.py`, `libs/positions/conventions.py`, `tests/positions/__init__.py`, `tests/positions/test_conventions.py`

**Interfaces — Produces:**

```python
TOU_BLOCKS = ("base", "peak", "offpeak")
PEAK_HOURS = frozenset(range(17, 22))        # 17:00–22:00, v1 default all provinces (my call)
OFFPEAK_HOURS = frozenset(range(10, 16))     # 10:00–16:00
def tou_of_hour(hour: int) -> str            # "peak" | "offpeak" | "flat"
PROVINCE_ALIASES: dict[str, tuple[str, ...]] = {
    "蒙西": ("蒙西", "inner_mongolia_mengxi", "内蒙古", "Inner Mongolia (Mengxi)"),
    "冀南": ("冀南", "河北南网", "hebei_south"), "安徽": ("安徽", "anhui"),
    "浙江": ("浙江", "zhejiang"), "山东": ("山东", "shandong")}
def canonical_province(name: str) -> str     # raises ValueError(f"unknown province: {name!r}")
def signed_mwh(direction: str, volume_mwh: float) -> float   # buy +, sell −; else ValueError
@dataclass(frozen=True)
class Position:
    book_id: int; province: str; direction: str; volume_mwh: float
    delivery_month: date            # first of month
    tou_block: str = "base"; price_cny_mwh: float | None = None
    source: str = "manual"; instrument_type: str = "forward"; channel: str = "annual"
    basis: str = ""                 # how a physical volume was estimated
POSITION_COLUMNS: list[str]         # frame column order used by stores and risk functions
def positions_frame(rows: list[Position]) -> pd.DataFrame
```

- [ ] **Step 1: Write tests** — `signed_mwh("buy", 5) == 5`, `("sell", 5) == -5`, `("hold", 1)` raises; `canonical_province("inner_mongolia_mengxi") == "蒙西"`, `canonical_province("火星")` raises with `"火星"` in the message; `tou_of_hour(18) == "peak"`, `(12) == "offpeak"`, `(3) == "flat"`; `positions_frame([])` returns an empty frame with `POSITION_COLUMNS`.
- [ ] **Step 2: Run** `~/.venvs/bess-platform/bin/python -m pytest tests/positions -q` → FAIL (module missing).
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run** → PASS.
- [ ] **Step 5: Probe (read-only, via ECS run-task on the asset-risk td):** print `SELECT DISTINCT province` from `rm_assets`, `rm_positions`, `rm_customers`, `rm_forward_curves`, `spot_prices_hourly`; add any name not yet in `PROVINCE_ALIASES` for the five provinces above; leave other provinces unmapped (they raise). Record the output in the commit message body.
- [ ] **Step 6: Commit** `libs/positions tests/positions` — "Add position conventions, TOU blocks, province mapper".

### Task 2: Curve frame and spot-seasonal source

**Files:**
- Create: `libs/curves/__init__.py`, `libs/curves/curve.py`, `tests/curves/__init__.py`, `tests/curves/test_curve.py`

**Interfaces — Produces:**

```python
CURVE_COLUMNS = ["province", "delivery_month", "tou_block", "price_cny_mwh", "source"]
def spot_seasonal(hourly: pd.DataFrame, province: str, months: list[date],
                  lookback_years: int = 2) -> pd.DataFrame
    # hourly: columns datetime, price (¥/MWh), one province. For each target month m and block b:
    # mean price over the same calendar month in the lookback window, hours filtered by block
    # (base = all hours). source = "spot_seasonal". Month with no history → no row.
def traded_points(trades: pd.DataFrame) -> pd.DataFrame
    # trades: columns province, delivery_month, tou_block, volume_mwh, price_cny_mwh
    # → volume-weighted price per (province, month, block); source = "traded"; zero/NaN volume rows ignored.
def reference_curve(*sources: pd.DataFrame) -> pd.DataFrame
    # one row per (province, delivery_month, tou_block); priority manual > traded > spot_seasonal;
    # keeps the winning row's source.
def curve_lookup(curve: pd.DataFrame, province: str, month: date, tou_block: str = "base") -> float | None
    # exact block, else falls back to "base" for that month, else None
```

- [ ] **Step 1: Tests** — spot_seasonal on a synthetic 2-year hourly series with price = hour (0..23): base = 11.5, peak = mean(17..21) = 19.0, offpeak = mean(10..15) = 12.5 for a covered month, no row for an uncovered month; traded_points weights `(100 MWh @ 300, 300 MWh @ 340)` → 330.0; reference_curve picks manual over traded over spot_seasonal and reports the source; curve_lookup falls back peak→base→None.
- [ ] **Step 2: Run → FAIL. Step 3: Implement. Step 4: Run → PASS.**
- [ ] **Step 5: Commit** — "Add curve frame, spot-seasonal and traded sources, reference curve".

### Task 3: Curve inputs from the two trade datasets + curve store

**Files:**
- Create: `libs/curves/inputs.py`, `libs/curves/store.py`, `tests/curves/test_inputs_store.py`

**Interfaces — Produces:**

```python
# inputs.py (pure)
def trades_from_position_volumes(pv: pd.DataFrame, province: str) -> pd.DataFrame
    # pv: rm_position_volumes rows (delivery_date, hour, *_price_cny_mwh, *_volume_mwh) for load books of one province.
    # Uses the four mid/long channels (annual, monthly_auction, monthly_listed, intramonth_match);
    # emits one row per (month, block in base/peak/offpeak via tou_of_hour, channel) in the traded_points input shape.
def trades_from_wind_trades(wt: pd.DataFrame, province: str = "蒙西") -> pd.DataFrame
    # wt: marketdata.wind_trades rows. Keeps energy_kind == '正常', channel == '省内', volume_mwh > 0,
    # price = energy_price; tou_block = "base". 汇总 / 置换 rows excluded.
# store.py
def load_curve(engine, province: str | None = None, as_of: date | None = None) -> pd.DataFrame   # CURVE_COLUMNS
def save_curve(engine, curve: pd.DataFrame, curve_date: date) -> int
    # upsert into marketdata.rm_forward_curves: product = "reference", delivery_date = delivery_month,
    # delivery_hour NULL, tou_block column, price_cny_kwh = price/1000, source as given.
def load_spot_hourly(engine, province: str, start: date, end: date, price_col: str = "rt_price") -> pd.DataFrame
```

- [ ] **Step 1: Tests** (sqlite-ATTACH harness as in `tests/asset_risk/test_agent_tools.py`): wind trades with one 正常 and one 汇总 row → only the 正常 row survives; position volumes with annual 10 MWh @ 350 at hour 18 → a peak row and a base row; save_curve then load_curve round-trips ¥/MWh exactly (kWh conversion inside the store only); saving the same curve twice leaves one row per key.
- [ ] **Step 2–4:** RED, implement, GREEN.
- [ ] **Step 5: Commit** — "Curve inputs from retail channel prices and 零碳46 trades; curve store".

### Task 4: Physical position generators

**Files:**
- Create: `libs/positions/physical.py`, `tests/positions/test_physical.py`

**Interfaces — Produces (all pure; `months` = list of first-of-month dates):**

```python
MIN_HISTORY_DAYS = 30
def expected_monthly_mwh(daily_mwh: pd.Series, months: list[date]) -> pd.DataFrame
    # daily_mwh: date-indexed realised MWh/day. For each target month: mean daily MWh of the same calendar
    # month in history × days in month, basis="same_month"; else trailing-90-day mean × days, basis="trailing_90d";
    # if total history < MIN_HISTORY_DAYS: mean of what exists × days, basis="insufficient_history";
    # empty history → no rows. Columns: delivery_month, volume_mwh, basis.
def generation_positions(book_id, province, daily_gen_mwh, months) -> list[Position]        # direction "buy", base block
def bess_positions(book_id, province, daily_net_discharge_mwh, months) -> list[Position]    # direction "buy", base block
def load_positions(book_id, province, contracts: pd.DataFrame, months, as_of: date) -> list[Position]
    # contracts: rm_customer_contracts rows (contract_type, price_cny_mwh, monthly_forecast JSON {"1".."12": mwh},
    # annual_forecast_mwh, start_date, end_date). Per month: volume from monthly_forecast[month] else annual/12,
    # only months within [start_date, end_date]; direction "sell"; price = price_cny_mwh for contract_type in
    # ("fixed", "peak_offpeak"), None otherwise; rows aggregated per (month, priced?) with volume-weighted price.
```
All set `source="physical"`.

- [ ] **Step 1: Tests** — same-month basis picked when last year's month exists; trailing_90d otherwise; 12 days of history → `basis == "insufficient_history"` and volume = mean × days (Review Focus 3); empty → `[]`; load_positions: one fixed contract (120 MWh/yr @ 400) and one indexed (240 MWh/yr) → per month a priced sell row of 10 MWh @ 400 and an unpriced sell row of 20 MWh; contract ending mid-year stops producing rows.
- [ ] **Step 2–4:** RED, implement, GREEN.
- [ ] **Step 5: Commit** — "Physical position generators for generation, BESS and load books".

### Task 5: Migration + positions store

**Files:**
- Create: `db/ddl/marketdata/market_maker_migration.sql`, `scripts/market_maker/migrate.py`, `libs/positions/store.py`, `tests/positions/test_store.py`

**Migration (additive, idempotent, one transaction; `--dry-run` default prints it):**

```sql
ALTER TABLE marketdata.rm_positions ADD COLUMN IF NOT EXISTS source TEXT;
ALTER TABLE marketdata.rm_positions ADD COLUMN IF NOT EXISTS tou_block TEXT;
ALTER TABLE marketdata.rm_positions ADD COLUMN IF NOT EXISTS basis TEXT;
UPDATE marketdata.rm_positions SET source = 'bilateral' WHERE source IS NULL;
ALTER TABLE marketdata.rm_forward_curves ADD COLUMN IF NOT EXISTS tou_block TEXT;
ALTER TABLE marketdata.rm_forward_curves DROP CONSTRAINT IF EXISTS rm_forward_curves_source_check;
ALTER TABLE marketdata.rm_forward_curves ADD CONSTRAINT rm_forward_curves_source_check
  CHECK (source IN ('lingfeng','manual','exchange','spot_seasonal','traded','venue'));
CREATE UNIQUE INDEX IF NOT EXISTS uq_rm_fc_reference ON marketdata.rm_forward_curves
  (province, product, curve_date, delivery_date, COALESCE(tou_block,'base'), source) WHERE delivery_hour IS NULL;
```
The migration script first prints the actual name of the existing source CHECK constraint (query `pg_constraint`) and uses that name in the DROP.

**Interfaces — Produces:**

```python
def load_positions(engine, book_ids: list[int] | None = None, status: str = "open") -> pd.DataFrame
    # POSITION_COLUMNS; delivery_month = first of start_date's month; rows spanning several months are split
    # evenly by calendar days per month; province canonicalised; source NULL → "bilateral"; tou_block NULL → "base".
def replace_physical(engine, book_id: int, rows: list[Position]) -> int
    # one transaction: DELETE FROM rm_positions WHERE book_id=:b AND source='physical'; INSERT rows with
    # instrument_type='profile', channel='annual', status='open', start_date=month start, end_date=month end.
def load_books(engine) -> pd.DataFrame   # book_id, book_name, book_type, asset_id, asset_type, asset_code, province (canonical), capacity_mw, bess_duration_h
```

- [ ] **Step 1: Tests** — replace_physical twice → same row count, and a pre-existing `bilateral` row is untouched (Review Focus 5); a position 2026-01-01..2026-03-31 of 90 MWh loads as three month rows 31/28/31 MWh; unknown province in a row raises naming it.
- [ ] **Step 2–4:** RED, implement, GREEN.
- [ ] **Step 5: Commit** — "Positions store, additive migration script".

### Task 6: Exposure and curve-aware MtM

**Files:**
- Create: `libs/risk/exposure.py`, `tests/risk/__init__.py`, `tests/risk/test_exposure.py`

**Interfaces — Produces:**

```python
def exposure(positions: pd.DataFrame, curve: pd.DataFrame) -> pd.DataFrame
    # per (book_id, province, delivery_month, tou_block): physical_mwh, financial_mwh, net_mwh (all signed),
    # mark_cny_mwh (curve_lookup; NaN when missing), mtm_cny, hedge_ratio.
    # mtm_cny = Σ over PRICED rows of signed_mwh × (mark − price); unpriced rows add exposure, no MtM;
    # mark missing → mtm_cny NaN. hedge_ratio = −financial/physical when physical != 0 else NaN.
def portfolio_exposure(exp: pd.DataFrame) -> pd.DataFrame   # summed over books per (province, month, block)
```

- [ ] **Step 1: Tests** — wind book: physical buy 1000 MWh + forward sell 600 @ 350, mark 330 → net 400, hedge_ratio 0.6, mtm = −600 × (330 − 350) = +12,000; month with no curve point → net_mwh present, mark and mtm NaN (Review Focus 1); retail book with only unpriced physical sell rows → net negative, mtm 0.0 with mark present (Review Focus 2); a priced physical sell row (fixed retail contract 10 MWh @ 400, mark 330) → mtm = −10 × (330 − 400) = +700.
- [ ] **Step 2–4:** RED, implement, GREEN.
- [ ] **Step 5: Commit** — "Exposure frame and curve-aware MtM".

### Task 7: VaR and Greeks

**Files:**
- Modify: `libs/risk/var.py` (add function; keep `historical_var`, `parametric_var`), `libs/risk/greeks.py` (add function)
- Create: `libs/risk/bess_greeks.py`, `tests/risk/test_var_greeks.py`

**Interfaces — Produces:**

```python
# var.py
def monthly_price_scenarios(hourly: pd.DataFrame, window_days: int = 30, lookback_days: int = 252) -> pd.Series
    # daily change of the trailing-`window_days` mean price (¥/MWh): the v1 proxy for forward-month moves (my call).
def portfolio_var(net_delta_mwh_by_province: dict[str, float], scenarios: dict[str, pd.Series],
                  confidence: float = 0.95, horizon_days: int = 1) -> dict
    # historical: aligned daily scenario P&L = Σ_province delta × Δprice; VaR = −percentile × sqrt(horizon);
    # parametric: per-province sigma, full correlation matrix from the aligned scenarios.
    # returns {"historical": float, "parametric": float, "n_scenarios": int}; < 20 aligned scenarios → both None.
# greeks.py
def option_greeks(option_positions: pd.DataFrame, curve: pd.DataFrame, vol: float, as_of: date) -> dict
    # Black-76 via libs.options.black_scholes.b76_greeks on the month forward; columns needed: direction,
    # volume_mwh, strike_cny_mwh, option_type, delivery_month, province. Empty frame → zeros.
# bess_greeks.py
def bess_monthly_greeks(power_mw, duration_h, roundtrip_eff, months: list[date], curve, province,
                        peak_vol: float, offpeak_vol: float, corr: float = 0.85, as_of: date) -> pd.DataFrame
    # per month: Margrabe spread-call strip (libs.decision_models.bess_spread_call_strip._strip_value /
    # _spread_vol) on that month's peak/offpeak marks, n_days = days in month; delta_peak_mwh and
    # delta_offpeak_mwh by central finite difference (±1 ¥/MWh), vega_cny_per_volpt by ±0.01 on both vols.
    # Month without both peak and offpeak marks → row with NaN Greeks and note "no peak/offpeak mark".
```

- [ ] **Step 1: Tests** — portfolio_var with one province, delta 100 MWh, scenarios = ±1 alternating (σ≈1) → parametric ≈ 100 × 1.645 within 5%; two perfectly anti-correlated provinces with equal deltas → historical VaR ≈ 0; 10 scenarios → `None`s; bess_monthly_greeks: delta_peak_mwh > 0, delta_offpeak_mwh < 0, both bounded by power × duration × days; missing offpeak mark → NaN row with the note; option_greeks on an empty frame → zeros.
- [ ] **Step 2–4:** RED, implement, GREEN.
- [ ] **Step 5: Commit** — "Portfolio VaR, option Greeks, BESS strip Greeks by month".

### Task 8: Refresh job

**Files:**
- Create: `services/market_maker/__init__.py`, `services/market_maker/refresh.py`, `tests/market_maker/__init__.py`, `tests/market_maker/test_refresh.py`

**Interfaces — Produces:**

```python
def refresh(engine, as_of: date, horizon_months: int = 12, dry_run: bool = False) -> dict
    # 1. books = positions.store.load_books; months = next `horizon_months` first-of-months from as_of.
    # 2. per book: wind/solar → daily MWh from marketdata.wind_dispatch_15min (gen_mwh; plant via registry row);
    #    bess → daily net discharge from libs.asset_risk.chain.load_chain(actual_mw, +only, ×0.25) with check_sign=False;
    #    load → rm_customer_contracts of the book's province (active, end_date >= as_of);
    #    replace_physical(book). Books with no data source are skipped and listed.
    # 3. per canonical province with any book: reference_curve(manual rows, traded_points(inputs), spot_seasonal) → save_curve.
    # returns {"books": {book_id: n_rows|reason}, "curves": {province: n_points}, "skipped": [...]}
if __name__ == "__main__": argparse --as-of, --horizon-months, --dry-run (default dry-run False requires --apply)
```

- [ ] **Step 1: Test** (sqlite fixture with one wind book, one BESS book, one load book, 40 days of data): refresh writes physical rows for all three, a curve for the province; second call leaves identical row counts; `dry_run=True` writes nothing and returns the same counts.
- [ ] **Step 2–4:** RED, implement, GREEN.
- [ ] **Step 5: Commit** — "Market-maker refresh job: physical positions and reference curves".

### Task 9: Streamlit app (lazy pages)

**Files:**
- Create: `apps/market_maker/__init__.py`, `app.py`, `page_positions.py`, `page_exposure.py`, `page_var.py`, `page_curves.py`, `Dockerfile`, `requirements.txt`, `tests/market_maker/test_app_structure.py`
- Modify: `.dockerignore` (add `!libs/positions/`, `!libs/curves/`, `!apps/market_maker/`, `!services/market_maker/`, `!libs/decision_models/` if not already allowed)

**Decisions:**
- Port 8530, `--server.baseUrlPath=market-maker`; auth `require_role(["Admin","Trader","Quant","RiskOfficer"])` as asset-risk; engine from `PGURL`/`DB_DSN`.
- Navigation: `LABELS = ["Positions", "Exposure & MtM", "VaR & Greeks", "Curves", "Risk Agent"]` with `st.radio(..., key="mm_page")` and one `if page == ...` block per page (same pattern as `apps/asset_risk/app.py`).
- Sidebar: book multiselect (default all), province filter, "as of" date; pages read `load_positions`, `load_curve`, `exposure`.
- Positions: table grouped by book with source badges (physical / bilateral / manual), `basis` shown for physical rows; CSV download.
- Exposure & MtM: month × book matrix of net MWh, stacked physical vs financial bars, hedge ratio line, MtM table; months with no mark show "—" and a caption listing them.
- VaR & Greeks: portfolio and per-book delta by month; `portfolio_var` (95/99, 1d/10d); BESS Greeks table from `bess_monthly_greeks` with vols from a sidebar input (default peak 0.6, offpeak 0.6); stress sliders (spot ±%) applied to net delta.
- Curves: reference curve by province with source colouring; manual CSV upload (columns `province, delivery_month, tou_block, price_cny_mwh`) saved with `source='manual'`; "Refresh physical positions & curves" button (Admin only) calling `refresh`.
- Chart work follows the `dataviz` skill.

- [ ] **Step 1: ast test** — `LABELS` equals the list above; every `render_*` import sits inside an `if page ==` block; no `st.tabs` call.
- [ ] **Step 2–4:** RED, implement, GREEN; run `streamlit.testing.v1.AppTest` against a sqlite engine to confirm each page renders its empty-state message without exception.
- [ ] **Step 5: Commit** — "Market-maker app: positions, exposure, VaR & Greeks, curves".

### Task 10: Risk Agent

**Files:**
- Create: `services/market_maker/agent_tools.py`, `services/market_maker/headless_agent.py`, `apps/market_maker/page_agent.py`, `tests/market_maker/test_agent_tools.py`
- Modify: `services/hermes/market_agent_bridge.py` (route `market in ("market-maker","risk-book")`)

**Interfaces — Produces:** `AGENT_BASE_SYSTEM` (opens with the grounding rule), `tools`, `execute_tool(name, inputs, engine)` with tools `get_book_list`, `get_exposure(book_id?)`, `get_mtm(book_id?)`, `get_var(confidence?)`, `get_greeks(book_id?)`, `get_curve(province)`. Each returns records with NaN → None, plus `"marks_missing": [month…]` where a mark is absent so the model never reads a blank as zero. Memory app key `market_maker`; page mirrors `apps/asset_risk/tab_agent.py` (history, Haiku auto-extract, memory manager, tool errors as data).

- [ ] **Step 1: Tests** — tool inventory equals the six names; `get_exposure` on the Task 6 fixture returns net 400 and hedge_ratio 0.6; a month without a mark appears in `marks_missing` and its `mtm_cny` is None; a subprocess import test shows no streamlit and no `apps.` modules loaded by `services.market_maker.*`.
- [ ] **Step 2–4:** RED, implement, GREEN.
- [ ] **Step 5: Commit** — "Market-maker risk agent: tools, headless agent, page".

### Task 11: Verification script (spec phase-B check)

**Files:**
- Create: `scripts/market_maker/verify_snapshot.py`

- [ ] **Step 1:** Script loads positions + curve for a given `--as-of`, writes them to `/tmp/mm_snapshot_<date>.parquet`, recomputes exposure/MtM/VaR twice (once from DB, once from the snapshot files only) and prints both with a max-abs-diff line; exit 1 if diff > 1e-6. Run locally against the Task 8 sqlite fixture → `MAX_ABS_DIFF 0.0`.
- [ ] **Step 2: Commit** — "Snapshot recompute check for market-maker risk numbers".

### Task 12: Infrastructure, migration, first deploy — **STOP steps**

**Files:**
- Create: `infra/terraform/market_maker.tf`; Modify: `infra/terraform/variables.tf` (`image_market_maker`, `desired_count_market_maker`), `apps/portal/app.py` (card + URL map entry `/market-maker/`), `LATEST-IMAGES.md`, `CLAUDE.md` (service table row, port 8530)

- [ ] **Step 1:** Terraform resources mirroring asset-risk in `services-new.tf`: ECR repo `bess-market-maker`, target group (health `/market-maker/_stcore/health`), listener rule with Cognito for `/market-maker`, `/market-maker/*`, task definition (0.5 vCPU / 2 GB, env `PGURL`, `ANTHROPIC_API_KEY`/Bedrock as asset-risk, `lifecycle { ignore_changes = [container_definitions] }`), service.
- [ ] **Step 2: STOP — ask:** "Run the additive market-maker migration on RDS (dry-run output attached)?" Then run `scripts/market_maker/migrate.py --apply` via run-task; verify `services/retail_risk` readers with a read-only probe (`book_mtm` on one load book returns as before).
- [ ] **Step 3: STOP — ask:** "Create the market-maker ECR repo, build v1 in CodeBuild, and terraform apply the new service (targeted: the market_maker resources only)?" User runs or approves the targeted apply; set `terraform.tfvars` locally.
- [ ] **Step 4:** Run `refresh --apply` once via run-task; paste the returned summary (books refreshed, curves built, skipped list).
- [ ] **Step 5:** Verify: target healthy; Positions page lists physical + bilateral rows; Exposure shows 零碳46 and at least one retail book; run `verify_snapshot.py` in-container → `MAX_ABS_DIFF 0.0`; ask the Risk Agent one exposure question.
- [ ] **Step 6: STOP — ask** before deploying the portal card (portal image) and the Hermes route.
- [ ] **Step 7:** Log in `LATEST-IMAGES.md`, update `CLAUDE.md`, commit.

---

## Self-review notes

- Spec §6 position model → Tasks 1, 4, 5 (two-store design: only the `rm_positions` store here; `venue.positions` is the venue plan). §7 curves → Tasks 2, 3 (decision 16.2: traded from retail channel prices + `wind_trades`; `exchange` reserved). §8 risk → Tasks 6, 7 (decision 16.1: Margrabe strip by month). §13 phase B app + agent + verification → Tasks 9, 10, 11. §11 roles → Task 9. Deployment → Task 12.
- Calls made here that the spec left to the builder: TOU hours (17–22 peak, 10–16 offpeak, all provinces); expected-volume rule (same calendar month, else trailing 90 days); VaR scenarios = daily change of the 30-day trailing mean spot; default BESS vols 0.6; curve `product='reference'`.
- Not in this plan: retiring anything further in retail-risk (already done), EventBridge schedule for `refresh` (manual/run-task in v1), venue phases C–F.
