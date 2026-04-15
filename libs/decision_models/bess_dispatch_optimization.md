# bess_dispatch_optimization

BESS perfect-foresight dispatch optimisation — shared model library entry.

---

## Source of truth

**Engine:** `services/bess_map/optimisation_engine.py` — `optimise_day()`

**Production pipeline that uses this engine:**
`services/bess_map/run_capture_pipeline.py`

The model library wrapper (`libs/decision_models/bess_dispatch_optimization.py`) is
a thin registry adapter over the engine. All LP logic stays in the engine.

---

## Models

| Model name | Scope | Entry module |
|---|---|---|
| `bess_dispatch_optimization` | Single day, 24 hourly prices | `libs/decision_models/bess_dispatch_optimization.py` |
| `bess_dispatch_simulation_multiday` | Multi-day batch, hourly price series | `libs/decision_models/bess_dispatch_simulation_multiday.py` |

---

## Assumptions and limitations

| Property | Value |
|---|---|
| Optimisation horizon | Single day (24 hours) |
| Time granularity | Hourly only (24 intervals per day) |
| Required price vector length | Exactly 24 floats |
| Initial SOC | 0 (forced at t=0) |
| Terminal SOC | Unconstrained (not fixed at end of day) |
| Cross-day SOC carryover | **No** — each day solved independently |
| Simultaneous charge/discharge | **Not allowed** — enforced by binary variable |
| Efficiency model | Symmetric sqrt: `eta_c = eta_d = sqrt(roundtrip_eff)` |
| Solver | PuLP CBC (MILP) |
| Problem type | Arbitrage maximisation |

**Known limitations:**
- No cross-day SOC carryover (each day resets to SOC=0)
- Hourly granularity only — does not support 15-min dispatch
- Perfect foresight on prices — not forecast-based or real-time
- No ramp rate constraints
- No minimum charge/discharge duration
- No terminal SOC constraint (battery may end the day at any SOC)
- No degradation model beyond optional throughput/cycle caps

> **Note:** The legacy `services/bess_map/storage_optimisation.py` (2017 pairwise-spread
> formulation) is **not** this engine and should not be called from new code.

---

## Example: direct engine call

No registry needed — use directly in pipeline scripts.

```python
from services.bess_map.optimisation_engine import optimise_day
import numpy as np

prices = np.array([30]*8 + [120]*4 + [30]*4 + [120]*4 + [30]*4, dtype=float)

result = optimise_day(
    prices=prices,
    power_mw=100.0,
    duration_h=2.0,
    roundtrip_eff=0.85,
)

print(result.profit)       # float — daily profit
print(result.status)       # "Optimal"
print(result.charge_mw)    # np.ndarray shape (24,)
print(result.discharge_mw) # np.ndarray shape (24,)
print(result.soc_mwh)      # np.ndarray shape (24,)
```

---

## Example: registry runner (apps and agents)

```python
import libs.decision_models.bess_dispatch_optimization   # register
from libs.decision_models.runners.local import run

result = run("bess_dispatch_optimization", {
    "prices_24": [30]*8 + [120]*4 + [30]*4 + [120]*4 + [30]*4,
    "power_mw": 100.0,
    "duration_h": 2.0,
    "roundtrip_eff": 0.85,
    # optional:
    # "max_throughput_mwh": 150.0,
    # "max_cycles_per_day": 1.0,
})

result["profit"]            # float
result["solver_status"]     # "Optimal"
result["charge_mw"]         # list[float], length 24
result["discharge_mw"]      # list[float], length 24
result["dispatch_grid_mw"]  # list[float], length 24  (= discharge - charge)
result["soc_mwh"]           # list[float], length 24
result["energy_capacity_mwh"]  # float = power_mw * duration_h
```

---

## Example: multi-day batch simulation

```python
import libs.decision_models.bess_dispatch_simulation_multiday   # register
from libs.decision_models.runners.local import run

# Build hourly price records for multiple days
hourly_prices = [
    {"datetime": "2026-01-01T00:00:00", "price": 30.0},
    {"datetime": "2026-01-01T01:00:00", "price": 35.0},
    # ... 24 entries for 2026-01-01 ...
    {"datetime": "2026-01-02T00:00:00", "price": 28.0},
    # ... 24 entries for 2026-01-02 ...
]

result = run("bess_dispatch_simulation_multiday", {
    "hourly_prices": hourly_prices,
    "power_mw": 100.0,
    "duration_h": 2.0,
    "roundtrip_eff": 0.85,
})

result["n_days_solved"]      # int
result["n_days_skipped"]     # int — days with missing price data
result["dispatch_records"]   # list of per-hour dicts with datetime, charge_mw, etc.
result["daily_profit"]       # list of {date, profit} dicts
result["energy_capacity_mwh"]
```

Or use the engine directly (recommended for batch pipeline scripts):

```python
import pandas as pd
from services.bess_map.optimisation_engine import compute_dispatch_from_hourly_prices

hourly_series = pd.Series(
    {pd.Timestamp("2026-01-01 00:00"): 30.0, pd.Timestamp("2026-01-01 01:00"): 35.0, ...}
)

dispatch_df, profit_s = compute_dispatch_from_hourly_prices(
    hourly_prices=hourly_series,
    power_mw=100.0,
    duration_h=2.0,
    roundtrip_eff=0.85,
)
# dispatch_df: DataFrame indexed by datetime
# profit_s:   Series indexed by date
```

---

## Example: Streamlit page

```python
from libs.decision_models.adapters.app.dispatch_page import render_dispatch_page
render_dispatch_page()
```

---

## Example: agent tool (Claude API)

```python
from libs.decision_models.adapters.agent.tools import DECISION_MODEL_TOOLS, handle_tool_call

response = client.messages.create(
    model="claude-opus-4-6",
    tools=DECISION_MODEL_TOOLS,
    messages=[{"role": "user", "content": "Optimise BESS dispatch for these prices: ..."}],
)

if response.stop_reason == "tool_use":
    for block in response.content:
        if block.type == "tool_use":
            result_json = handle_tool_call(block.name, block.input)
```

---

## Running tests

```bash
cd bess-platform
pytest libs/decision_models/tests/test_bess_dispatch_optimization.py -v
```

---

## File map

```
services/bess_map/
  optimisation_engine.py          ← LP engine (source of truth)
  run_capture_pipeline.py         ← production pipeline (imports engine)
  storage_optimisation.py         ← LEGACY, orphaned, do not use

libs/decision_models/
  bess_dispatch_optimization.py            ← single-day model (registry wrapper)
  bess_dispatch_simulation_multiday.py     ← multi-day model (registry wrapper)
  bess_dispatch_optimization.md            ← this file
  schemas/
    bess_dispatch_optimization.py          ← single-day input/output schema
    bess_dispatch_simulation_multiday.py   ← multi-day input/output schema
  adapters/
    app/dispatch_page.py                   ← Streamlit page wrapper
    agent/tools.py                         ← Claude API tool definitions
  tests/
    test_bess_dispatch_optimization.py     ← single-day + multi-day tests
```
