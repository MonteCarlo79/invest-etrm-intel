# Poland BESS Revenue Analysis — Design Spec

**Date:** 2026-06-10  
**Scope:** Add ancillary service (AS) data scraping, perfect-foresight dispatch P&L, Kirk-Margrabe strip valuation, and combined revenue → IRR integration to `apps/po-market/app.py`.

---

## 1. Overview

Three new capabilities are added to the existing Poland market app, all building on the `po_day_ahead_prices` table and the shared `bess_spread_call_strip.py` / `optimise_day()` engines:

| Capability | Where |
|------------|-------|
| AS market price scraping (FCR, aFRR, Rynek Mocy) | `services/po_knowledge/entso_scraper.py` + new DB tables |
| BESS P&L Analysis (PF dispatch + AS stacking + options value) | BESS Opportunity tab — new subsection A |
| Kirk-Margrabe spread option strip valuation | BESS Opportunity tab — new subsection B |
| Load combined revenue into IRR model | Investment Analysis tab — one new button |
| AS data controls | Data Management tab — two new sections |

---

## 2. Data Layer

### 2.1 New DB Tables

```sql
-- Weekly FCR & aFRR auction clearing prices
CREATE TABLE intl_market.po_as_prices (
    id                SERIAL PRIMARY KEY,
    week_start        DATE NOT NULL,          -- Monday of auction week
    market_type       TEXT NOT NULL,          -- 'FCR' | 'aFRR_capacity' | 'aFRR_energy'
    price_pln_mw_week NUMERIC(12,2),          -- PLN/MW/week clearing price
    accepted_mw       NUMERIC(10,2),
    source            TEXT DEFAULT 'pse',
    fetched_at        TIMESTAMPTZ DEFAULT now(),
    UNIQUE (week_start, market_type)
);

-- Annual Rynek Mocy (Capacity Market) auction results
CREATE TABLE intl_market.po_capacity_market (
    id              SERIAL PRIMARY KEY,
    delivery_year   INT  NOT NULL,            -- e.g. 2026
    auction_date    DATE,
    price_pln_mw_yr NUMERIC(12,2),            -- PLN/MW/year clearing price
    accepted_mw     NUMERIC(10,2),
    source          TEXT DEFAULT 'tge',
    fetched_at      TIMESTAMPTZ DEFAULT now(),
    UNIQUE (delivery_year)
);
```

Both tables are created at app startup (same pattern as `po_day_ahead_prices`).

### 2.2 New Scraper Functions (`services/po_knowledge/entso_scraper.py`)

| Function | Source | Cadence |
|----------|--------|---------|
| `scrape_po_fcr_prices(conn, weeks_back=52)` | PSE CSV: `pse.pl/dane-systemowe/regulacja-frekwencji` | Tuesday 06:00 CET |
| `scrape_po_afrr_prices(conn, weeks_back=52)` | PSE aFRR auction results page | Tuesday 06:00 CET |
| `scrape_po_capacity_market(conn)` | TGE Rynek Mocy auction results | 1st of month 05:00 CET |
| `get_as_revenue_estimate(conn, power_mw, fcr_pct, afrr_pct)` | DB query helper | on-demand |

`get_as_revenue_estimate` returns a dict:
```python
{
    "fcr_pln_yr":      float,   # avg weekly price × FCR MW × 52
    "afrr_pln_yr":     float,   # avg capacity price × aFRR MW × 52
    "capacity_pln_yr": float,   # latest Rynek Mocy price × total MW
    "total_pln_yr":    float,
    "fcr_weeks":       int,     # data coverage
    "afrr_weeks":      int,
}
```

### 2.3 APScheduler Jobs (added to `app.py` startup block)

```python
scheduler.add_job(_run_as_scrape,  "cron", day_of_week="tue", hour=6,  minute=5,  id="po_as_prices",  replace_existing=True)
scheduler.add_job(_run_cap_scrape, "cron", day=1,              hour=5,  minute=10, id="po_cap_market", replace_existing=True)
```

---

## 3. BESS Opportunity Tab

Two new subsections appended to the existing tab content.

### 3.1 Subsection A — BESS P&L Analysis (Perfect-Forecast Dispatch)

**New function:** `_run_bess_dispatch_po(power_mw, duration_h, roundtrip_eff, price_col) → pd.DataFrame`

- Queries `po_day_ahead_prices` for all complete 24-hour days (hour 0–23 present)
- Divides `price_pln_mwh / 1000` to get PLN/kWh for `optimise_day()`
- Calls `optimise_day()` from `services/bess_map/optimisation_engine.py` per day
- Computes naive profit (charge at min-price hour, discharge at max-price hour)
- Returns DataFrame: `trading_date, pf_profit_pln, naive_profit_pln, options_value_pln, charge_mwh, discharge_mwh`
- Profit columns multiplied by 1000 (same unit fix as PH v15)

Arbitrage slice capacity = `power_mw × arbitrage_pct / 100`

**Config panel (two rows):**
- Row 1: Power (MW) | Duration (h) | Efficiency (%) | Price column (PLN / EUR)
- Row 2: FCR allocation (%) | aFRR allocation (%) | Arbitrage allocation (%) — validated to sum to 100

**Metrics row (6 cards):**

| Card | Formula |
|------|---------|
| Total Annual Revenue | `arb_annual + fcr_pln_yr + afrr_pln_yr + capacity_pln_yr` |
| Arbitrage P&L | PF dispatch profit annualised on arb slice |
| FCR Revenue | `avg_weekly_fcr × fcr_mw × 52` |
| aFRR Revenue | `avg_weekly_afrr × afrr_mw × 52` |
| Rynek Mocy | `latest_price_pln_mw_yr × total_mw` |
| Options Value | `sum(options_value_pln)` from dispatch DataFrame |

**Charts:**
1. Daily arbitrage P&L line (x=trading_date, y=pf_profit_pln)
2. Monthly stacked bar: arbitrage / FCR / aFRR / Rynek Mocy revenue layers
3. Dispatch profile for user-selected date: dual-axis, charge/discharge bars + price line

### 3.2 Subsection B — BESS Spread Option Valuation (Kirk-Margrabe)

**No new pricing code.** Uses existing `libs/decision_models/bess_spread_call_strip.py` engine.

**Calibration from `po_day_ahead_prices` (last 90 days):**
- Peak hours: 08:00–20:00 CET (configurable, default 08–20)
- `peak_forward_pln` = mean of peak-hour prices over window
- `offpeak_forward_pln` = mean of offpeak-hour prices over window
- `peak_vol` = annualised std of log-returns of daily avg peak prices (× √252)
- `offpeak_vol` = same for offpeak

**Config panel:**
- Peak hours (start/end slider, default 8–20)
- O&M cost as strike K (PLN/MWh, default 20)
- Valuation horizon (days, default 365)
- Correlation slider (default 0.85)
- Power, duration, efficiency pre-filled from Subsection A config (editable)

**Metrics row (6 cards):**

| Card | Description |
|------|-------------|
| Strip Value | Total PLN |
| Intrinsic Value | `max(net_spread − K, 0) × q_max × days` |
| Time Value | `strip_value − intrinsic_value` |
| Moneyness | `(F_peak − F2_eff) / F2_eff × 100%` (green ITM / red OTM) |
| Delta | `dV/dF_peak` (PLN per PLN/MWh bump) |
| Vega | `dV/dσ` per 1% vol point |

Moneyness summary table with current forward prices and ITM/OTM indicator.

**Session state:** both Subsection A config and computed revenues are stored in `st.session_state["po_dispatch_results"]` for use by the Investment Analysis tab.

---

## 4. Investment Analysis Tab

**One new element only — "Load from dispatch model" button:**

```
📥 Load from dispatch model  (PLN 2,914,000/yr)
```

- Shown only when `st.session_state.get("po_dispatch_results")` is not None
- Clicking sets the existing annual revenue input widget to total combined revenue
- Collapsed expander below the button shows revenue breakdown:

```
Arbitrage (PF dispatch):   PLN 1,234,000  (42%)
FCR (30% capacity):        PLN   620,000  (21%)
aFRR (30% capacity):       PLN   780,000  (27%)
Rynek Mocy:                PLN   280,000  (10%)
─────────────────────────────────────────────────
Total Annual Revenue:      PLN 2,914,000
```

No other changes to the IRR model, DCF cashflow, or output charts.

---

## 5. Data Management Tab

Two new sections appended to the existing tab.

### Section: Ancillary Service Prices

- Status cards: FCR (N weeks, latest PLN/MW/week), aFRR capacity (same), Rynek Mocy (latest year + PLN/MW/yr)
- Buttons: "Scrape FCR", "Scrape aFRR", "Scrape Capacity Market" (each calls the corresponding scraper function inline, shows spinner + row count)
- 52-week dual-line chart: FCR price and aFRR price over time (PLN/MW/week, Y-axis)

### Section: AS Backfill

- Date range picker (start week) and market type selector (FCR / aFRR / both)
- "Run Backfill" button — calls scraper with `weeks_back` computed from start date

---

## 6. Key Constraints & Decisions

- **No changes to `optimise_day()`** — it is currency-agnostic; PLN prices are divided by 1000 before passing in and profits multiplied by 1000 on the way out (same fix applied in PH v15).
- **Capacity split is user-controlled** — no co-optimisation of AS + energy. Each slice is independent.
- **Kirk-Margrabe reuses existing engine** — `bess_spread_call_strip.py` is called as-is; no fork or copy.
- **New image `bess-po-market:v13` will be built and deployed** — no new ECR repository needed. The Dockerfile must add `COPY libs/ ./libs/` if not already present (required for `bess_spread_call_strip.py`).
- **Rynek Mocy uses latest auction price** — multi-year contract structure is simplified to a flat PLN/MW/yr adder.
- **PSE/TGE scraper failures are non-fatal** — if the endpoint changes or returns no data, the app falls back to showing "No AS data — use reference rates" with the existing hardcoded `_AS_CONTEXT_PO` values.

---

## 7. Files Changed

| File | Change |
|------|--------|
| `services/po_knowledge/entso_scraper.py` | Add 3 scraper functions + `get_as_revenue_estimate()` |
| `apps/po-market/app.py` | Add `_run_bess_dispatch_po()`, 2 BESS Opportunity subsections, IRR load button, 2 Data Management sections, 2 scheduler jobs, table creation at startup |
| `apps/po-market/Dockerfile` | Add `COPY libs/ ./libs/` if missing |

No new files. No changes to shared libraries.
