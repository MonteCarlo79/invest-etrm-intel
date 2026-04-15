# libs/decision_models — Overview

Shared model library for the BESS trading platform.
A platform layer that makes decision models reusable by multiple apps, agents, and pipelines — without duplicating logic.

---

## What belongs here

**Model assets** — pure-computation decision models wrapped in a standard registry format:
- Optimization models (LP/MILP dispatch solvers)
- Simulation models (multi-day batch simulations)
- Forecast models (price forecasts)
- Analytics models (P&L attribution, scenario engines)

Each asset is one file (`libs/decision_models/<model_name>.py`) that:
1. Contains a `run_fn` with validated inputs/outputs
2. Registers itself into the module-level `registry` at import time
3. Exposes `MODEL_ASSUMPTIONS` (machine-readable metadata dict)

## What does NOT belong here

| Does not belong | Reason | Where it should live |
|---|---|---|
| DB queries / ORM calls | I/O side-effect | `services/` or `apps/` |
| REST endpoint handlers | Web framework concern | `apps/api/` |
| Streamlit rendering | UI concern | `libs/decision_models/adapters/app/` |
| Model training pipelines | Long-running batch job | `services/` or `ml/` |
| Pre-trained artifact files | Binary assets | S3 / artifact store |
| Pipeline orchestration | Ops concern | `services/` |

---

## Three-layer architecture

```
libs/decision_models/
│
├── <model_name>.py          ← MODEL ASSET (this layer)
│   Pure computation. Thin wrapper over an engine in services/.
│   Self-registers via registry.register() at import time.
│
├── adapters/
│   ├── agent/tools.py       ← AGENT TOOL ADAPTER
│   │   Claude API tool definitions. Translates tool_use blocks into run() calls.
│   │   One entry per model asset that should be exposed to agents.
│   │
│   └── app/dispatch_page.py ← APP ADAPTER
│       Streamlit page components. Renders forms and calls run().
│       No computation here.
│
├── runners/
│   ├── local.py             ← LOCAL RUNNER
│   │   run(model_name, inputs) — validates, calls run_fn, validates output.
│   │   Used by apps, agents, and test code.
│   │
│   └── ecs_batch.py         ← ECS BATCH RUNNER
│       submit_ecs_task() — boto3 RunTask wrapper for ECS batch jobs.
│
└── tests/                   ← TESTS
    Per-model tests + cross-model contract tests.
```

---

## Metadata standard

Every model must include these keys in `ModelSpec.metadata`. Defined in `model_spec.py::REQUIRED_METADATA_KEYS`.

| Key | Type | Description |
|---|---|---|
| `category` | str | `"optimization"` \| `"simulation"` \| `"forecast"` \| `"analytics"` |
| `scope` | str | Input scope: `"single_day"`, `"multi_day"`, `"province_level"`, `"asset_level"` |
| `market` | str \| None | Target market context, e.g. `"mengxi"`, or `None` if market-agnostic |
| `asset_type` | str | `"bess"` \| `"wind"` \| `"solar"` |
| `granularity` | str | Time granularity: `"hourly"` \| `"15min"` \| `"daily"` |
| `horizon` | str | Time horizon: `"single_day"` \| `"multi_day"` \| `"day_ahead"` \| `"historical"` |
| `deterministic` | bool | `True` if same inputs always produce the same output |
| `model_family` | str | Implementation: `"lp_milp"` \| `"ols"` \| `"identity"` \| `"rule_based"` |
| `source_of_truth_module` | str | File path relative to repo root |
| `source_of_truth_functions` | list[str] | Core function names in that module |
| `assumptions` | dict \| list | Machine-readable assumptions (also exported as `MODEL_ASSUMPTIONS`) |
| `limitations` | list[str] | Known limitations; mirrored from `MODEL_ASSUMPTIONS["limitations"]` |
| `fallback_behavior` | str \| None | Describe any fallback, or `None` |
| `status` | str | `"production"` \| `"experimental"` |
| `owner` | str | Team or system responsible |

Values may be `None` where the concept does not apply, but the **key must always be present**.
Compliance is verified by `tests/test_metadata_contract.py`.

---

## Schema standard

Each model has a pair of dataclasses in `schemas/<model_name>.py`:

```python
@dataclass
class <Model>Input:
    # All required inputs with types and docstrings
    # Optional inputs use Optional[T] with sensible defaults

@dataclass
class <Model>Output:
    # All outputs with types and docstrings
    # Everything JSON-serialisable (lists, floats, str, bool)
```

Rules:
- All field types must be JSON-serialisable (no numpy arrays, no `date` objects in output)
- Optional inputs use `Optional[T] = None`
- No business logic in schema files

---

## Testing standard

Every model must have `tests/test_<model_name>.py` covering:

| Test class | What it tests |
|---|---|
| `TestRegistration` | Model is registered; `run_fn` is callable |
| `TestMetadataContract` | Model-specific metadata claims match actual behaviour |
| `TestOutputContract` | All expected output keys present; correct types and lengths |
| `TestPhysics` / `TestBehaviour` | Core correctness (profit > 0 for spread prices, etc.) |
| `TestInputValidation` | Bad inputs raise `ValueError` with informative messages |

The cross-model contract tests in `tests/test_metadata_contract.py` run automatically for every registered model — no per-model updates needed.

---

## Registry introspection

```python
from libs.decision_models.registry import registry

# List all registered models
registry.list_models()                 # -> List[ModelSpec]

# Get metadata dict for one model
registry.get_model_metadata("bess_dispatch_optimization")  # -> Dict

# Get full JSON-serialisable descriptor
registry.describe_model("price_forecast_dayahead")         # -> Dict

# Get descriptors for all models (sorted by name)
registry.summarize()                                       # -> List[Dict]
```

---

## Registered models

| Model | Category | Granularity | Status |
|---|---|---|---|
| `bess_dispatch_optimization` | optimization | hourly | production |
| `bess_dispatch_simulation_multiday` | simulation | hourly | production |
| `price_forecast_dayahead` | forecast | hourly | production |
| `revenue_scenario_engine` | analytics | 15min | production |

---

## Adding a new model — checklist

1. Create `libs/decision_models/<model_name>.py` with `MODEL_ASSUMPTIONS` and `_SPEC`
2. Create `libs/decision_models/schemas/<model_name>.py` with `Input` / `Output` dataclasses
3. Create `libs/decision_models/tests/test_<model_name>.py`
4. Import-register in `libs/decision_models/adapters/agent/tools.py` (add tool definition + dispatch)
5. Update `tests/test_metadata_contract.py::ALL_MODEL_NAMES` to include the new model
6. Run `pytest libs/decision_models/tests/ -v` — all tests must pass
