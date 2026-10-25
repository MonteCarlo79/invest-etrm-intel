# Mixed-unit days in `marketdata.spot_prices_hourly` (found 2026-10-25, option-engine Task 8 gate)

## The flaw

`spot_prices_hourly` mixes **¥/kWh flat-fill days** (old LingFeng pipeline: one daily average repeated 24h, ~0.2–0.4 scale) with **¥/MWh segments** (new pipeline, ~200–1200 scale) **inside a single rolling window**. 山东 flips ~18 times in 12 months; 山西 92/375 and 蒙西 60/373 days are flat-fill kWh days.

Any **whole-column** normalization heuristic (e.g. `nanmedian < 5 → ×1000`) mis-normalizes one segment of a mixed window — the first 山东 option-engine run produced V_tot = 254k ¥/kW with clip bounds [-90000, 1383400] before this was caught.

## The working rule (implemented in `services/option_engine/readers.py::_normalize_units`)

Per-(day, column): a day is ¥/kWh iff **max(|values|) < 5** — MAX not median (half-placeholder days median 0 but are ¥/MWh), ABS (山东 2026-01 has genuinely all-negative ¥/MWh days). Evidence: day-level maxima are bimodal with an **empty [2, 50) gap** on all five probed provinces (kWh-side day max ≤ 0.26; MWh-side day min ≥ 50).

Companion rule: per-hour RT fallback triggers on **rt == 0 or NaN only** (exact parity with `run_capture_pipeline.py:249`) — genuine negative prices (山东) must survive; they are paid-to-charge value, not missing data.

## Cross-pillar impact (OPEN DECISION — Pillar 2 owner)

The incumbent **capture pipeline** (`run_capture_pipeline.py`) computes `bess_capture_daily` on **raw mixed-unit data with no normalization**: flat-fill kWh days contribute ~0 profit (spreads 1000× too small), depressing V_hist ≈ **50% 山东 / 25% 山西 / 16% 蒙西**. Until a re-run on normalized prices, every calibration ratio and bess-map theoretical value in those provinces inherits the skew, and 山东's in-band calibration pass is partly coincidental (normalized numerator ÷ depressed denominator). 广东/江苏 (clean data) are the genuine validations.

Secondary: flat-fill days damp the month-hour curve spread → extrinsic share likely overstated (山东 33%, 蒙西 46% of energy value); `fetch_price_wind_correlation`'s whole-column median rule inherits the same flaw (not modified — existing code). Source-level re-ingest or tag-and-exclude of flat-fill days is a data-platform decision.

## Status

- Option engine (`services/option_engine/`): normalizes at read, writes masking counts into every snapshot's `mc_params` (rt/da days_scaled, fallback filled, negatives_kept).
- Capture pipeline (Pillar 2): **unchanged — re-run decision pending with the user.**
