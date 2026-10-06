# dipeng/Curve Shaping

- id: `dipeng_curve_shaping` · class: practice · type: folder
- topic: Power Forward Curve Shaping · level: intermediate · market: mixed · year: 2014
- worked examples: True · code: True

## Concepts
- **Forward curve construction** — Building a granular hourly or half-hourly power price curve from liquid quoted products.
- **Curve shaping** — Distributing flat-period forward prices into sub-period shapes using seasonal or profile weights.
- **Calendar/quarterly/monthly product hierarchy** — Mapping quoted annual, seasonal, quarterly, and monthly forward contracts into a consistent price structure.
- **Baseload and peakload splitting** — Deriving off-peak prices from quoted baseload and peakload instruments.
- **Day-of-week and hour-of-day profiles** — Applying time-of-use shape factors to capture intra-week and intra-day price variation.
- **Market data ingestion** — Sourcing and formatting daily broker or Reuters settlement prices as inputs to the curve builder.
- **Cross-market curve building** — Applying a common curve-building framework across different national power markets such as DE, GB, and NL.

## Methods
- Spreadsheet-based curve builder (Excel/VBA)
- Shape-factor interpolation
- Baseload-peakload arbitrage-free splitting
- Iterative reconciliation of shaped curve to quoted forwards
- Reuters daily price feed integration

## Implied prerequisites
- Power market product conventions (base, peak, off-peak)
- Forward curve basics
- Excel and VBA proficiency
- Understanding of delivery period structures (hourly, daily, monthly)
