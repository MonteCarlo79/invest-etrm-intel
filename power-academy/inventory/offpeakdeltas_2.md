# OffpeakDeltas

- id: `offpeakdeltas_2` · class: library · type: txt
- topic: Offpeak delta term structure by month · level: intermediate · market: DE · year: 2012
- worked examples: False · code: True

## Concepts
- **Offpeak delta** — Sensitivity or differential value attributed to the offpeak portion of a power delivery period, expressed as a fraction or price increment per offpeak hour.
- **Peak/offpeak split** — Decomposition of a baseload power contract into peak and offpeak sub-periods with separately quoted or modelled values.
- **Monthly term structure** — Representation of a quantity (here offpeak delta) across consecutive calendar months forming a forward curve.
- **Offpeak hour count** — Number of offpeak hours in a given calendar month, used as a weighting factor in aggregation or averaging.

## Methods
- Tabular term structure construction
- Monthly hour-count weighting

## Implied prerequisites
- Power market product definitions (peak, offpeak, baseload)
- Forward curve construction basics
- Calendar arithmetic for hour counting
