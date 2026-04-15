# price_forecast_dayahead

Province-level day-ahead hourly RT price forecast — shared model library entry.

---

## Source of truth

**Engine:** `services/bess_map/forecast_engine.py` — `build_forecast()`

**Production pipeline that uses this engine:**
`services/bess_map/run_capture_pipeline.py`

The model library wrapper (`libs/decision_models/price_forecast_dayahead.py`) is
a thin registry adapter over the engine. All forecast logic stays in the engine.

---

## Scope and assumptions

| Property | Value |
|---|---|
| Spatial resolution | **Province-level** — NOT nodal / NOT per-asset |
| Time granularity | **Hourly** — 24 intervals per day, NOT 15-min |
| Forecast horizon | Day-ahead: predict RT price for tomorrow using today's DA price |
| Target variable | Hourly RT settlement price (Yuan/MWh) |
| Training method | Rolling OLS (no pretrained artifact on disk) |
| Deterministic | Yes — same inputs produce the same outputs |
| Confidence intervals | Not implemented |
| Cross-day leakage | None — only data strictly before target date used for training |

**Known limitations:**
- Province-level only — not nodal / not per-asset
- Hourly granularity only — not 15-min
- OLS features are DA price and hour-of-day only (no additional market signals)
- Rolling OLS fitted fresh on each call — no persistent model artifact
- Falls back to naive_da when < min_train_days of training data is available
- No confidence intervals

---

## Models

| Model name | Description | Fallback |
|---|---|---|
| `ols_da_time_v1` | OLS with [1, da_price, sin(2πh/24), cos(2πh/24)] features. Rolling lookback. | Falls back to naive_da if < min_train_days |
| `naive_da` | RT prediction = DA price (identity). No training required. | N/A |

---

## Example: direct engine call

No registry needed — use directly in pipeline scripts.

```python
from services.bess_map.forecast_engine import build_forecast
import pandas as pd

# hourly must have DatetimeIndex and columns: rt_price, da_price
hourly = pd.read_sql(...)  # your price DataFrame

rt_pred = build_forecast(
    hourly,
    model="ols_da_time_v1",
    min_train_days=7,
    lookback_days=60,
)
# rt_pred: pd.Series with DatetimeIndex, name="rt_pred"
```

---

## Example: registry runner (apps and agents)

```python
import libs.decision_models.price_forecast_dayahead   # register
from libs.decision_models.runners.local import run

# Build hourly price records: history + target day
hourly_prices = [
    {"datetime": "2026-04-14T00:00:00", "rt_price": 62.5, "da_price": 58.0},
    {"datetime": "2026-04-14T01:00:00", "rt_price": 55.0, "da_price": 52.0},
    # ... all 24 hours of each history day ...
    # Target day: no rt_price (not yet known), only da_price
    {"datetime": "2026-04-15T00:00:00", "rt_price": None, "da_price": 60.0},
    {"datetime": "2026-04-15T01:00:00", "rt_price": None, "da_price": 57.0},
    # ... 24 hours of target day ...
]

result = run("price_forecast_dayahead", {
    "hourly_prices": hourly_prices,
    "target_date": "2026-04-15",
    "model": "ols_da_time_v1",  # optional (default)
    "min_train_days": 7,         # optional (default)
    "lookback_days": 60,         # optional (default)
})

result["rt_pred"]    # list[float], length 24 — hourly RT predictions
result["datetimes"]  # list[str], length 24 — ISO timestamps for each hour
result["model_used"] # "ols" or "naive_da" (actual model used — may differ from requested if fallback triggered)
result["target_date"]
result["model"]
```

---

## Example: agent tool (Claude API)

```python
from libs.decision_models.adapters.agent.tools import DECISION_MODEL_TOOLS, handle_tool_call

response = client.messages.create(
    model="claude-opus-4-6",
    tools=DECISION_MODEL_TOOLS,
    messages=[{"role": "user", "content": "Forecast RT prices for Inner Mongolia on 2026-04-15..."}],
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
pytest libs/decision_models/tests/test_price_forecast_dayahead.py -v
```

---

## File map

```
services/bess_map/
  forecast_engine.py              ← forecast logic (source of truth)
  run_capture_pipeline.py         ← production pipeline (imports forecast_engine)

libs/decision_models/
  price_forecast_dayahead.py             ← model wrapper (registry entry)
  price_forecast_dayahead.md             ← this file
  schemas/
    price_forecast_dayahead.py           ← input/output schema
  adapters/
    agent/tools.py                       ← Claude API tool definition
  tests/
    test_price_forecast_dayahead.py      ← tests
```
