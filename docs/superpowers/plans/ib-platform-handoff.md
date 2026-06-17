# IB Trading Platform — Session Handoff

> **For a new Claude session:** Read this document first, then proceed directly to the next task.
> Primary working directory: `C:\Users\dipeng.chen\OneDrive\ETRM\ib-platform`
> GitHub remote: `git@github.com:MonteCarlo79/ib-platform.git` (SSH, key at `~/.ssh/id_ed25519`)
> Design spec: `C:\Users\dipeng.chen\OneDrive\ETRM\bess-platform\docs\superpowers\specs\2026-06-14-ib-trading-platform-design.md`

---

## What Has Been Built

### Phase 1 — Foundation (complete)
- `services/broker/base.py` — `BaseBroker` ABC, `Position`, `Order`, `OrderRequest`, `AccountSummary` dataclasses
- `services/broker/ib_broker.py` — IBBroker (ib_insync, TWS port 7497)
- `services/broker/alpaca_broker.py` — AlpacaBroker (REST)
- `services/broker/paper_broker.py` — PaperBroker (simulated fills from bars_1h)
- `services/broker/broker_factory.py` — `get_broker(type)` factory
- `services/broker_service/main.py` — FastAPI app (8 REST endpoints)
- `services/broker_service/order_router.py` — pre-trade risk checks (7 hard blocks)
- `services/broker_service/algo_scheduler.py` — APScheduler strategy loop
- `services/broker_service/data_writer.py` — syncs positions/fills/bars → RDS
- `db/schema.sql` — full `trading.*` schema (accounts, positions, trades, orders, bars, risk, KB, signals, VIX, news, agent_memory)

### Phase 2 — Analytics Libs (complete, 67 tests passing)
All under `libs/` and tested in `tests/libs/`:

| Module | Key exports |
|---|---|
| `libs/pricing/kirk_margrabe.py` | `kirk_spread_call`, `margrabe_exchange` |
| `libs/pricing/vol_surface.py` | `VolSurface(F, slices)` — `.get_vol(K, T)`, `.vol_grid(strikes, expiries)` |
| `libs/pricing/pnl_explain.py` | `explain_pnl` → `PnlExplain` (Δ/Γ/Vega/Θ attribution) |
| `libs/fixed_income/bonds.py` | `bond_price`, `ytm`, `macaulay_duration`, `modified_duration`, `dv01`, `convexity` |
| `libs/fixed_income/yield_curve.py` | `NelsonSiegelCurve.fit(tenors, rates)` → `.rate(t)`, `.discount_factor(t)`; `bootstrap_curve` |
| `libs/fixed_income/swaps.py` | `irs_npv`, `par_rate`, `swap_dv01` |
| `libs/fixed_income/caps_floors.py` | `caplet_black`, `cap_black`, `floor_black` |
| `libs/fx/forwards.py` | `fx_forward(spot, r_domestic, r_foreign, T)`, `forward_points(spot, r_domestic, r_foreign, T)`, `cross_rate` |
| `libs/fx/vol_surface_fx.py` | `FXVolSmile`, `build_fx_smile`, `delta_to_strike` |
| `libs/signals/vix.py` | `VixTermStructure`, `vix_regime`, `contango_pct`, `roll_yield_annualised`, `implied_vol_premium` |

**Critical import pattern** (bess-platform `libs/` collision avoidance):
Both repos have `libs/` packages. Use `importlib.util.spec_from_file_location` to load bess-platform modules by absolute path. See `libs/pricing/kirk_margrabe.py` and `libs/pricing/vol_surface.py` for the exact pattern. For `smile.py` (which imports `black_scholes`), pre-register `sys.modules["libs.options.black_scholes"]` before executing.

---

## What To Build Next

### Phase 3 — Apps + Risk Libs (NOT STARTED)

**Scope from design spec Section 17:**
> `apps/portfolio/` + `apps/markets/` (incl. VIX panel)

**Additional request from user:** Integrate ForexFactory economic calendar (`forexfactory.com/calendar`) as a data source in the Macro tab of `apps/markets/`.

#### Files to create (46 total)

**Risk libs (pure functions, numpy + math only, no scipy needed):**
- `libs/risk/__init__.py`
- `libs/risk/greeks.py` — `PortfolioGreeks` dataclass + `aggregate_greeks(positions: list[dict]) → PortfolioGreeks`
- `libs/risk/var.py` — `historical_var`, `parametric_var`, `cvar`, `component_var`, `var_backtest`; use `math.erfinv` for z-score: `z = math.sqrt(2) * math.erfinv(2*p - 1)`
- `libs/risk/performance.py` — `roace`, `sharpe`, `sortino`, `calmar`, `max_drawdown`, `drawdown_analysis`, `win_stats`, `return_on_risk`, `capital_efficiency`, `attribution`
- `libs/risk/scenarios.py` — `spot_shock(positions, shock_pct)`, `vol_shock(positions, shock_pct)`, `spot_vol_matrix(positions, spot_range, vol_range)`
- `libs/risk/cashflow.py` — `daily_cashflow_statement(conn, day)`, `cumulative_cashflow(conn, start, end)`, `margin_utilisation(conn)`

**Simulation libs:**
- `libs/simulation/__init__.py`
- `libs/simulation/options_scenarios.py` — `options_scenario_matrix(positions, spot_range, vol_range)` — delta-gamma-vega approx per position dict

**Market data connector:**
- `services/market_data/__init__.py`
- `services/market_data/forexfactory.py` — `fetch_calendar(include_next_week=True) → list[dict]`; fetches from `https://nfs.faireconomy.media/ff_calendar_thisweek.json` and `https://nfs.faireconomy.media/ff_calendar_nextweek.json`; no auth; `requests.get(..., timeout=10)`

**Shared DB layer (no streamlit import):**
- `apps/__init__.py`
- `apps/shared/__init__.py`
- `apps/shared/db.py` — all queries use `conn.cursor()` context manager (NOT `pd.read_sql`) so they're mockable; returns `pd.DataFrame`

**Portfolio app (7 tabs):**
- `apps/portfolio/__init__.py`, `apps/portfolio/app.py`
- `apps/portfolio/tabs/__init__.py`
- `apps/portfolio/tabs/positions.py`, `pnl.py`, `risk.py`, `options_book.py`, `fixed_income.py`, `performance.py`, `cashflow.py`

**Markets app (7 tabs):**
- `apps/markets/__init__.py`, `apps/markets/app.py`
- `apps/markets/tabs/__init__.py`
- `apps/markets/tabs/charts.py`, `vol_surface.py`, `options_cockpit.py`, `yield_curves.py`, `fx.py`, `macro.py`, `vix.py`

**Tests:**
- `tests/libs/risk/__init__.py`, `test_greeks.py`, `test_var.py`, `test_performance.py`, `test_scenarios.py`, `test_cashflow.py`
- `tests/libs/simulation/__init__.py`, `test_options_scenarios.py`
- `tests/services/market_data/__init__.py`, `test_forexfactory.py`
- `tests/apps/__init__.py`, `tests/apps/shared/__init__.py`, `tests/apps/shared/test_db.py`

---

## Key Technical Notes (confirmed this session)

### DB schema column facts (from `db/schema.sql`)
- `trading.trades` **has** `account_id` column ✓
- `trading.cashflows`: `cf_type` values are `trade|margin|dividend|coupon|roll|funding` (no `option_premium` — map option premiums to `trade` cf_type)
- `trading.capital_summary`: has `margin_posted`, `margin_available`, `nav`, `capital_employed`, `gross_notional`
- `trading.portfolio_risk`: has `total_delta`, `total_gamma`, `total_theta`, `total_vega`, `dv01`, `fx_delta_usd`, `var_1d_95`, `nav`
- `trading.vix_term_structure`: columns are `ts_date, vix_index, m1..m8, contango_pct, roll_yield_annualised, vvix, regime, source`

### VolSurface API (confirmed from `libs/pricing/vol_surface.py`)
```python
vs = VolSurface(F=100.0, slices={
    0.25: {"strikes": np.array([...]), "vols": np.array([...])},
    1.00: {"strikes": np.array([...]), "vols": np.array([...])},
})
vol = vs.get_vol(K=105.0, T=0.5)
grid = vs.vol_grid(strikes=np.linspace(80,120,40), expiries=np.linspace(0.1,2.0,20))
# vol_grid returns shape (len(expiries), len(strikes))
```

### NelsonSiegelCurve API (confirmed from `libs/fixed_income/yield_curve.py`)
```python
ns = NelsonSiegelCurve.fit(tenors, rates)  # tenors/rates as np.ndarray
rate = ns.rate(t)           # spot rate at tenor t years
df   = ns.discount_factor(t)
```

### FX forwards API (confirmed from `libs/fx/forwards.py`)
```python
from libs.fx.forwards import fx_forward, forward_points, cross_rate
fwd = fx_forward(spot=150.0, r_domestic=0.0, r_foreign=0.05, T=1.0)
pts = forward_points(spot=150.0, r_domestic=0.0, r_foreign=0.05, T=1.0)
```

### bess-platform import pattern (for options_cockpit.py)
```python
import importlib.util, os, sys
_BESS = "C:/Users/dipeng.chen/OneDrive/ETRM/bess-platform"
abs_path = os.path.join(_BESS, "libs/options/black_scholes.py")
spec = importlib.util.spec_from_file_location("_bess_black_scholes", abs_path)
mod = importlib.util.module_from_spec(spec)
sys.modules["_bess_black_scholes"] = mod
spec.loader.exec_module(mod)
b76_price = mod.b76_price
bs_price  = mod.bs_price
bs_greeks = mod.bs_greeks  # verify hasattr before calling
```

### Tab render pattern
Each tab is a module with a single `render(conn, ...)` function. The app's `app.py` wraps `connect()` in `@st.cache_resource`. Tab files never import streamlit at module level — they import inside `render()` if needed to avoid test issues.

### requirements.txt additions needed
Add to `requirements.txt` before creating app files:
```
streamlit==1.37.0
plotly==5.23.0
```

### DB query pattern (cursor-based, NOT pd.read_sql — for mockability)
```python
def get_positions(conn, account_id: str) -> pd.DataFrame:
    sql = """SELECT ... FROM trading.positions WHERE account_id = %s"""
    with conn.cursor() as cur:
        cur.execute(sql, (account_id,))
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description]
    return pd.DataFrame(rows, columns=cols)
```

### Test mock pattern (from existing `tests/broker_service/test_data_writer.py`)
```python
def _mock_conn(rows=None, colnames=None):
    conn = MagicMock()
    cursor = MagicMock()
    cursor.__enter__ = lambda s: s
    cursor.__exit__ = MagicMock(return_value=False)
    cursor.fetchall.return_value = rows or []
    cursor.description = [(c,) for c in (colnames or [])]
    conn.cursor.return_value = cursor
    return conn, cursor
```

### Running tests
```bash
cd C:/Users/dipeng.chen/OneDrive/ETRM/ib-platform
/c/Users/dipeng.chen/AppData/Local/Programs/Python/Python313/python.exe -m pytest tests/ -q
```
Python is at `/c/Users/dipeng.chen/AppData/Local/Programs/Python/Python313/python.exe`.
Must stay green at 67+ passing after every task.

### Environment
- Windows 11, bash shell via Git for Windows
- Primary working directory: `C:\Users\dipeng.chen\OneDrive\ETRM\ib-platform`
- Additional directory: `C:\Users\dipeng.chen\OneDrive\ETRM\bess-platform` (shared infrastructure)
- RDS shared with bess-platform, env var `PGURL`
- Config: `config/.env` in ib-platform root

---

## Git State

Branch: `master` (all work directly on master — no feature branches)
Remote: `git@github.com:MonteCarlo79/ib-platform.git`
Latest commit: `7db12ce fix(libs): apply vix_regime spike rule fix`

All 16 commits pushed. Run `git log --oneline` to see full history.

---

## Suggested Task Order for New Session

1. **Execute Phase 3** using `superpowers:subagent-driven-development` skill with 6 tasks:
   - **Task 1** — `libs/risk/greeks.py` + `libs/risk/var.py` + tests (TDD)
   - **Task 2** — `libs/risk/performance.py` + `libs/risk/scenarios.py` + tests (TDD)
   - **Task 3** — `libs/risk/cashflow.py` + `libs/simulation/options_scenarios.py` + tests (TDD)
   - **Task 4** — `services/market_data/forexfactory.py` + `apps/shared/db.py` + tests
   - **Task 5** — `apps/portfolio/` (7 tabs: positions, pnl, risk, options_book, fixed_income, performance, cashflow) — add streamlit + plotly to requirements.txt first
   - **Task 6** — `apps/markets/` (7 tabs: charts, vol_surface, options_cockpit, yield_curves, fx, macro, vix)

2. After Phase 3 verified, continue with **Phase 4** (market data pipeline + news service)

**Skill to use:** `superpowers:subagent-driven-development` — fresh subagent per task, spec compliance review then code quality review.

### Phase 3 implementation details per task

#### Task 1 — libs/risk/greeks.py + var.py

**`libs/risk/greeks.py`:**
```python
from __future__ import annotations
from dataclasses import dataclass

@dataclass
class PortfolioGreeks:
    total_delta: float = 0.0
    total_gamma: float = 0.0
    total_theta: float = 0.0
    total_vega: float = 0.0
    dv01: float = 0.0
    fx_delta_usd: float = 0.0

def aggregate_greeks(positions: list[dict]) -> PortfolioGreeks:
    """Sum Greeks across positions. Each dict may have: delta, gamma, theta, vega, dv01, fx_delta_usd."""
    pg = PortfolioGreeks()
    for pos in positions:
        pg.total_delta    += float(pos.get("delta",        0.0))
        pg.total_gamma    += float(pos.get("gamma",        0.0))
        pg.total_theta    += float(pos.get("theta",        0.0))
        pg.total_vega     += float(pos.get("vega",         0.0))
        pg.dv01           += float(pos.get("dv01",         0.0))
        pg.fx_delta_usd   += float(pos.get("fx_delta_usd", 0.0))
    return pg
```

**`libs/risk/var.py`:**
```python
from __future__ import annotations
import math
import numpy as np
from typing import List, Dict

def _z(p: float) -> float:
    return math.sqrt(2) * math.erfinv(2 * p - 1)

def historical_var(returns: List[float], confidence: float, horizon_days: int = 1) -> float:
    if not returns:
        return 0.0
    var_1d = float(-np.percentile(returns, (1 - confidence) * 100))
    return max(0.0, var_1d) * math.sqrt(horizon_days)

def parametric_var(positions: List[dict], confidence: float, horizon_days: int = 1) -> float:
    """Delta-Normal. Each position dict: delta (USD sens), sigma (daily vol of underlying)."""
    variance = sum(
        (float(p.get("delta", 0.0)) * float(p.get("sigma", 0.0))) ** 2
        for p in positions
    )
    return _z(confidence) * math.sqrt(variance) * math.sqrt(horizon_days)

def cvar(returns: List[float], confidence: float) -> float:
    """Expected Shortfall. Returns positive loss number."""
    if not returns:
        return 0.0
    arr = sorted(returns)
    n_tail = max(1, int((1 - confidence) * len(arr)))
    return float(-np.mean(arr[:n_tail]))

def component_var(positions: List[dict], confidence: float) -> Dict[str, float]:
    """Per-position VaR contribution. Each dict: position_id, delta, sigma."""
    z = _z(confidence)
    return {
        str(p.get("position_id", i)): z * abs(float(p.get("delta", 0.0)) * float(p.get("sigma", 0.0)))
        for i, p in enumerate(positions)
    }

def var_backtest(var_series: List[float], actual_pnl_series: List[float], confidence: float) -> dict:
    """Kupiec POF test. Returns exceptions count + kupiec_ok flag."""
    if len(var_series) != len(actual_pnl_series):
        raise ValueError("var_series and actual_pnl_series must have same length")
    n = len(var_series)
    if n == 0:
        return {"exceptions": 0, "total": 0, "exception_rate": 0.0,
                "expected_rate": 1 - confidence, "kupiec_ok": True}
    exceptions = sum(1 for v, p in zip(var_series, actual_pnl_series) if -p > v)
    expected_rate = 1.0 - confidence
    return {
        "exceptions": exceptions,
        "total": n,
        "exception_rate": exceptions / n,
        "expected_rate": expected_rate,
        "kupiec_ok": exceptions / n <= 2.0 * expected_rate + 1e-9,
    }
```

#### Task 2 — libs/risk/performance.py + scenarios.py

**`libs/risk/performance.py`:**
```python
from __future__ import annotations
import math
import numpy as np
from typing import List, Dict

def roace(pnl_series: List[float], capital_employed_series: List[float]) -> float:
    if not capital_employed_series:
        return 0.0
    avg_cap = float(np.mean(capital_employed_series))
    return float(np.sum(pnl_series)) / avg_cap if avg_cap != 0.0 else 0.0

def sharpe(returns: List[float], risk_free_rate: float = 0.0, annualise: bool = True) -> float:
    if len(returns) < 2:
        return 0.0
    arr = np.array(returns) - risk_free_rate / 252.0
    std = float(np.std(arr, ddof=1))
    if std == 0.0:
        return 0.0
    ratio = float(np.mean(arr)) / std
    return ratio * math.sqrt(252) if annualise else ratio

def sortino(returns: List[float], target: float = 0.0, annualise: bool = True) -> float:
    if len(returns) < 2:
        return 0.0
    arr = np.array(returns)
    downside = arr[arr < target] - target
    downside_std = float(math.sqrt(float(np.mean(downside ** 2)))) if len(downside) > 0 else 0.0
    if downside_std == 0.0:
        return 0.0
    ratio = (float(np.mean(arr)) - target / 252.0) / downside_std
    return ratio * math.sqrt(252) if annualise else ratio

def calmar(returns: List[float]) -> float:
    if len(returns) < 2:
        return 0.0
    ann_return = float(np.mean(returns)) * 252
    nav = np.cumprod(1 + np.array(returns)).tolist()
    mdd = max_drawdown(nav)
    return ann_return / mdd if mdd != 0.0 else 0.0

def max_drawdown(nav_series: List[float]) -> float:
    if len(nav_series) < 2:
        return 0.0
    arr = np.array(nav_series)
    running_max = np.maximum.accumulate(arr)
    return float(-np.min((arr - running_max) / running_max))

def drawdown_analysis(nav_series: List[float]) -> dict:
    if len(nav_series) < 2:
        return {"max_drawdown": 0.0, "current_drawdown": 0.0, "drawdown_duration": 0}
    arr = np.array(nav_series)
    running_max = np.maximum.accumulate(arr)
    dds = (arr - running_max) / running_max
    dur = 0
    for v in reversed(dds):
        if v < 0:
            dur += 1
        else:
            break
    return {"max_drawdown": float(-np.min(dds)), "current_drawdown": float(-dds[-1]),
            "drawdown_duration": dur}

def win_stats(trades: List[dict]) -> dict:
    if not trades:
        return {"win_rate": 0.0, "profit_factor": 0.0, "avg_winner": 0.0,
                "avg_loser": 0.0, "expectancy": 0.0}
    pnls = [float(t.get("pnl", 0.0)) for t in trades]
    winners = [p for p in pnls if p > 0]
    losers  = [p for p in pnls if p <= 0]
    gross_profit = sum(winners) if winners else 0.0
    gross_loss   = abs(sum(losers)) if losers else 0.0
    return {
        "win_rate":      len(winners) / len(pnls),
        "profit_factor": gross_profit / gross_loss if gross_loss > 0 else float("inf"),
        "avg_winner":    sum(winners) / len(winners) if winners else 0.0,
        "avg_loser":     sum(losers)  / len(losers)  if losers  else 0.0,
        "expectancy":    sum(pnls) / len(pnls),
    }

def return_on_risk(pnl: float, var_1d_95: float) -> float:
    return pnl / var_1d_95 if var_1d_95 != 0.0 else 0.0

def capital_efficiency(gross_notional: float, nav: float) -> float:
    return gross_notional / nav if nav != 0.0 else 0.0

def attribution(pnl_series: List[float], strategy_pnl_dict: Dict[str, List[float]]) -> dict:
    total = sum(pnl_series)
    return {
        sid: {"total_pnl": sum(sp), "pct_of_portfolio": sum(sp) / total if total != 0 else 0.0}
        for sid, sp in strategy_pnl_dict.items()
    }
```

**`libs/risk/scenarios.py`:**
```python
from __future__ import annotations
from typing import List

def spot_shock(positions: List[dict], shock_pct: float) -> float:
    """P&L from spot shock: Δ·dS + 0.5·Γ·dS². Each dict: delta, gamma, spot."""
    pnl = 0.0
    for pos in positions:
        ds = float(pos.get("spot", 100.0)) * shock_pct
        pnl += float(pos.get("delta", 0.0)) * ds + 0.5 * float(pos.get("gamma", 0.0)) * ds * ds
    return pnl

def vol_shock(positions: List[dict], shock_pct: float) -> float:
    """P&L from vol shock: Vega·dσ. Each dict: vega, vol."""
    return sum(float(p.get("vega", 0.0)) * float(p.get("vol", 0.2)) * shock_pct for p in positions)

def spot_vol_matrix(positions: List[dict], spot_range: List[float], vol_range: List[float]) -> List[List[float]]:
    """2D grid: rows=spot shocks, cols=vol shocks."""
    return [[spot_shock(positions, s) + vol_shock(positions, v) for v in vol_range] for s in spot_range]
```

#### Task 3 — libs/risk/cashflow.py + libs/simulation/options_scenarios.py

**`libs/risk/cashflow.py`:**
```python
from __future__ import annotations
from typing import List

def daily_cashflow_statement(conn, day: str) -> dict:
    sql = """SELECT cf_type, COALESCE(SUM(amount), 0) FROM trading.cashflows
             WHERE ts::date = %s GROUP BY cf_type"""
    with conn.cursor() as cur:
        cur.execute(sql, (day,))
        m = {row[0]: float(row[1]) for row in cur.fetchall()}
    trade      = m.get("trade",    0.0)
    commission = m.get("commission", 0.0)  # note: commission amounts are negative in DB
    margin     = m.get("margin",   0.0)
    dividends  = m.get("dividend", 0.0) + m.get("coupon", 0.0)
    roll       = m.get("roll",     0.0)
    funding    = m.get("funding",  0.0)
    net = trade + commission + margin + dividends + roll + funding
    return {"trade_proceeds": trade, "commissions": commission, "variation_margin": margin,
            "dividends": dividends, "roll_costs": roll, "funding_costs": funding, "net_cashflow": net}

def cumulative_cashflow(conn, start: str, end: str) -> List[dict]:
    sql = """SELECT ts::date, SUM(amount) FROM trading.cashflows
             WHERE ts::date BETWEEN %s AND %s GROUP BY ts::date ORDER BY ts::date"""
    with conn.cursor() as cur:
        cur.execute(sql, (start, end))
        rows = cur.fetchall()
    result, cumulative = [], 0.0
    for row in rows:
        net = float(row[1])
        cumulative += net
        result.append({"date": str(row[0]), "net_cashflow": net, "cumulative": cumulative})
    return result

def margin_utilisation(conn) -> float:
    with conn.cursor() as cur:
        cur.execute("SELECT margin_posted, margin_available FROM trading.capital_summary ORDER BY ts_date DESC LIMIT 1")
        row = cur.fetchone()
    if row is None:
        return 0.0
    posted, available = float(row[0] or 0), float(row[1] or 0)
    total = posted + available
    return posted / total if total > 0 else 0.0
```

**`libs/simulation/options_scenarios.py`:**
```python
from __future__ import annotations
from typing import List

def options_scenario_matrix(positions: List[dict], spot_range: List[float], vol_range: List[float]) -> List[List[float]]:
    """2D P&L grid: delta-gamma-vega approx. Each dict: delta, gamma, vega, spot, vol."""
    result = []
    for spot_shock in spot_range:
        row = []
        for vol_shock in vol_range:
            pnl = 0.0
            for pos in positions:
                ds     = float(pos.get("spot", 100.0)) * spot_shock
                dsigma = float(pos.get("vol",  0.2))   * vol_shock
                pnl += (float(pos.get("delta", 0.0)) * ds
                        + 0.5 * float(pos.get("gamma", 0.0)) * ds * ds
                        + float(pos.get("vega", 0.0)) * dsigma)
            row.append(pnl)
        result.append(row)
    return result
```

#### Task 4 — services/market_data/forexfactory.py + apps/shared/db.py

**`services/market_data/forexfactory.py`:**
```python
from __future__ import annotations
import requests
from typing import List

_THIS_WEEK = "https://nfs.faireconomy.media/ff_calendar_thisweek.json"
_NEXT_WEEK = "https://nfs.faireconomy.media/ff_calendar_nextweek.json"

def fetch_calendar(include_next_week: bool = True) -> List[dict]:
    """Returns list of {title, country, date, time, impact, forecast, previous}."""
    urls = [_THIS_WEEK, _NEXT_WEEK] if include_next_week else [_THIS_WEEK]
    events = []
    for url in urls:
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        events.extend(resp.json())
    return events
```

**`apps/shared/db.py`** — all functions use cursor pattern (NOT pd.read_sql), returns pd.DataFrame:
```python
from __future__ import annotations
import os
import psycopg2
import pandas as pd
from typing import List, Optional

def connect():
    return psycopg2.connect(os.environ["PGURL"])

def _fetch(conn, sql: str, params: tuple = ()) -> pd.DataFrame:
    with conn.cursor() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description]
    return pd.DataFrame(rows, columns=cols)

def get_positions(conn, account_id: str) -> pd.DataFrame:
    return _fetch(conn, """SELECT symbol, asset_class, expiry, strike, "right",
        quantity, avg_cost, unrealised_pnl, currency, ts_snapshot
        FROM trading.positions WHERE account_id = %s ORDER BY symbol""", (account_id,))

def get_trades(conn, account_id: str, days: int = 30) -> pd.DataFrame:
    return _fetch(conn, """SELECT trade_id, symbol, asset_class, side, quantity,
        fill_price, commission, strategy_id, ts_fill
        FROM trading.trades WHERE account_id = %s AND ts_fill >= NOW() - INTERVAL %s
        ORDER BY ts_fill DESC""", (account_id, f"{days} days"))

def get_strategy_pnl(conn, days: int = 90) -> pd.DataFrame:
    return _fetch(conn, """SELECT strategy_id, ts_date, realized_pnl, unrealized_pnl,
        trades_n, capital_employed, roace_daily, sharpe_rolling_30d
        FROM trading.strategy_pnl WHERE ts_date >= CURRENT_DATE - INTERVAL %s
        ORDER BY ts_date""", (f"{days} days",))

def get_capital_summary(conn, days: int = 90) -> pd.DataFrame:
    return _fetch(conn, """SELECT ts_date, nav, cash_free, margin_posted, margin_available,
        capital_employed, capital_utilisation_pct, gross_notional, leverage_ratio
        FROM trading.capital_summary WHERE ts_date >= CURRENT_DATE - INTERVAL %s
        ORDER BY ts_date""", (f"{days} days",))

def get_cashflows(conn, start: str, end: str) -> pd.DataFrame:
    return _fetch(conn, """SELECT cf_id, ts, cf_type, symbol, amount, currency, strategy_id
        FROM trading.cashflows WHERE ts::date BETWEEN %s AND %s ORDER BY ts""", (start, end))

def get_portfolio_risk(conn, account_id: str) -> pd.DataFrame:
    return _fetch(conn, """SELECT ts, total_delta, total_gamma, total_theta, total_vega,
        dv01, fx_delta_usd, var_1d_95, nav
        FROM trading.portfolio_risk WHERE account_id = %s ORDER BY ts DESC LIMIT 90""", (account_id,))

def get_bars_1d(conn, symbol: str, n: int = 252) -> pd.DataFrame:
    df = _fetch(conn, """SELECT ts_date, open, high, low, close, volume
        FROM trading.bars_1d WHERE symbol = %s ORDER BY ts_date DESC LIMIT %s""", (symbol, n))
    return df.sort_values("ts_date").reset_index(drop=True) if not df.empty else df

def get_bars_1h(conn, symbol: str, n: int = 168) -> pd.DataFrame:
    df = _fetch(conn, """SELECT ts, open, high, low, close, volume
        FROM trading.bars_1h WHERE symbol = %s ORDER BY ts DESC LIMIT %s""", (symbol, n))
    return df.sort_values("ts").reset_index(drop=True) if not df.empty else df

def get_options_chain(conn, symbol: str, expiry: str) -> pd.DataFrame:
    return _fetch(conn, """SELECT strike, "right", bid, ask, iv, delta, gamma, theta, vega, ts_snapshot
        FROM trading.options_chain WHERE symbol = %s AND expiry = %s
        ORDER BY ts_snapshot DESC, strike""", (symbol, expiry))

def get_yield_curve(conn, curve_id: str = "USD") -> pd.DataFrame:
    return _fetch(conn, """SELECT ts_date, tenor_label, tenor_years, rate, source
        FROM trading.yield_curves WHERE curve_id = %s ORDER BY ts_date DESC, tenor_years LIMIT 500""",
        (curve_id,))

def get_fx_rates(conn, pairs: Optional[List[str]] = None) -> pd.DataFrame:
    if pairs:
        return _fetch(conn, """SELECT pair, ts, spot, bid, ask, source FROM trading.fx_rates
            WHERE pair = ANY(%s) ORDER BY pair, ts DESC""", (pairs,))
    return _fetch(conn, """SELECT DISTINCT ON (pair) pair, ts, spot, bid, ask, source
        FROM trading.fx_rates ORDER BY pair, ts DESC""")

def get_vix_term_structure(conn, n_days: int = 30) -> pd.DataFrame:
    df = _fetch(conn, """SELECT ts_date, vix_index, m1, m2, m3, m4, m5, m6, m7, m8,
        contango_pct, roll_yield_annualised, vvix, regime
        FROM trading.vix_term_structure ORDER BY ts_date DESC LIMIT %s""", (n_days,))
    return df.sort_values("ts_date").reset_index(drop=True) if not df.empty else df

def get_vol_surface(conn, symbol: str) -> pd.DataFrame:
    return _fetch(conn, """SELECT ts_date, expiry, strike, iv FROM trading.vol_surface
        WHERE underlying = %s ORDER BY ts_date DESC, expiry, strike LIMIT 5000""", (symbol,))
```

#### Task 5 — apps/portfolio/ (7 tabs)

All tabs follow `render(conn, account_id)` or `render(conn)` pattern. Import Streamlit and Plotly inside the function or at top of file (these modules are never imported in test files).

**`apps/portfolio/app.py`:**
```python
import os, sys
import streamlit as st
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))
from apps.shared.db import connect
from apps.portfolio.tabs import positions, pnl, risk, options_book, fixed_income, performance, cashflow

st.set_page_config(page_title="Portfolio", layout="wide", page_icon="📊")

@st.cache_resource
def _get_conn():
    return connect()

conn = _get_conn()
account_id = os.environ.get("ACCOUNT_ID", "paper_default")
st.title("Portfolio Dashboard")
tab_labels = ["Positions", "P&L", "Risk", "Options Book", "Fixed Income", "Performance", "Cash Flow"]
tabs = st.tabs(tab_labels)
with tabs[0]: positions.render(conn, account_id)
with tabs[1]: pnl.render(conn)
with tabs[2]: risk.render(conn, account_id)
with tabs[3]: options_book.render(conn, account_id)
with tabs[4]: fixed_income.render(conn, account_id)
with tabs[5]: performance.render(conn)
with tabs[6]: cashflow.render(conn)
```

**`apps/portfolio/tabs/positions.py`** — `render(conn, account_id)`: call `get_positions`, show metrics (count, unrealised P&L, asset class count), st.dataframe, plotly pie by asset class.

**`apps/portfolio/tabs/pnl.py`** — `render(conn)`: call `get_strategy_pnl(conn, days=365)`, show YTD/MTD/all-time realized P&L metrics, plotly stacked bar by strategy per day.

**`apps/portfolio/tabs/risk.py`** — `render(conn, account_id)`: call `get_portfolio_risk`, show latest Greeks as metrics (delta/gamma/theta/vega/dv01), compute `historical_var` + `cvar` from nav daily returns, show `spot_vol_matrix` as plotly imshow heatmap (aggregate pos dict from total_delta/gamma/vega).

**`apps/portfolio/tabs/options_book.py`** — `render(conn, account_id)`: get positions filtered to asset_class='option', for each expiry show `get_options_chain` Greek ladder table, then build `VolSurface` from `get_vol_surface` data and show 3D surface.

**`apps/portfolio/tabs/fixed_income.py`** — `render(conn, account_id)`: show FI positions table, call `get_yield_curve`, fit `NelsonSiegelCurve.fit(tenors, rates)`, plot market quotes + NS fitted curve.

**`apps/portfolio/tabs/performance.py`** — `render(conn)`: call `get_strategy_pnl` + `get_capital_summary`, compute `sharpe/sortino/calmar/roace/drawdown_analysis` from portfolio-aggregated series, per-strategy metrics table.

**`apps/portfolio/tabs/cashflow.py`** — `render(conn)`: call `daily_cashflow_statement(conn, today)` for today's breakdown, `cumulative_cashflow(conn, ytd_start, today)` for YTD chart, `margin_utilisation(conn)` for gauge.

#### Task 6 — apps/markets/ (7 tabs)

**`apps/markets/app.py`:** same structure as portfolio app, 7 tabs.

**`apps/markets/tabs/charts.py`** — `render(conn)`: text_input for symbol, call `get_bars_1d`, compute MA20/MA50/Bollinger/RSI, plotly make_subplots candlestick + RSI.

**`apps/markets/tabs/vol_surface.py`** — `render(conn)`: text_input for symbol, `get_vol_surface`, date selector, build `VolSurface` slices dict, 3 sub-tabs: 3D surface (go.Surface), term structure (ATM vol per expiry), skew (moneyness vs IV).

**`apps/markets/tabs/options_cockpit.py`** — `render()` (no conn — pure calculation): load bess-platform black_scholes via `importlib` with `@st.cache_resource`, sidebar params for S/K/T/vol/r, call `b76_price`/`bs_price`, show Greeks table if `bs_greeks` available, P&L heatmap with `px.imshow`.

**`apps/markets/tabs/yield_curves.py`** — `render(conn)`: curve_id selectbox (USD/EUR/GBP/JPY), date selectbox, `get_yield_curve`, `NelsonSiegelCurve.fit`, plotly line chart with NS fit + 3 historical date overlays.

**`apps/markets/tabs/fx.py`** — `render(conn)`: `get_fx_rates` latest spot table, pair selectbox, forward rates table for tenors [1/12, 3/12, 6/12, 1.0, 2.0]yr using `fx_forward(spot, r_domestic, r_foreign, T)` and `forward_points(spot, r_domestic, r_foreign, T)`.

**`apps/markets/tabs/macro.py`** — `render(conn)`: call `fetch_calendar(include_next_week=True)` from `services.market_data.forexfactory`, multiselect impact filter (High/Medium/Low), styled DataFrame with colour-coded impact, plus side-by-side panels for latest USD yield curve and FX spot rates from DB.

**`apps/markets/tabs/vix.py`** — `render(conn)`: `get_vix_term_structure(conn, n_days=30)`, show regime badge (contango/backwardation/spike), metrics (VIX, M1, contango%, roll yield), plotly bar chart of M1-M8 levels + horizontal VIX line, contango% history chart, IVP gauge (go.Indicator) using `implied_vol_premium`.

---

## Git State After This Session

Branch: `master`
ib-platform latest commit: `7db12ce fix(libs): apply vix_regime spike rule fix` (unchanged — no Phase 3 code written yet)
bess-platform: this handoff document updated.
