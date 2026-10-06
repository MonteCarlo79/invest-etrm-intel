# main

- id: `main` · class: library · type: txt
- topic: Power tolling asset valuation with forward price curves and discounting · level: intermediate · market: GB · year: 2012
- worked examples: True · code: True

## Concepts
- **Tolling agreement valuation** — Valuing the optionality embedded in a power plant tolling contract over a specified delivery period.
- **Valuation date vs. start/end date** — Distinguishing the as-of pricing date from the forward period over which cash flows are modelled.
- **Date collection** — Constructing an ordered sequence of daily delivery dates spanning a contract period.
- **Discount factor** — Computing present-value weights for each future cash flow date using a flat risk-free rate.
- **Daily discounting** — Applying date-specific discount factors to a daily delivery schedule within the valuation period.
- **Forward price selection** — Extracting market-quoted forward power prices as of a chosen price date for use in valuation.
- **Price curve mapping** — Aligning broker-quoted forward prices to each date in the delivery date list.
- **File-based market data ingestion** — Locating and reading CSV price records from a structured folder hierarchy keyed by asset, provider and valuation date.

## Methods
- CSV market data loading
- Date-list generation
- Flat-rate continuous or periodic discounting
- Forward price curve alignment
- Object-oriented valuation workflow (class-based discount and price selection objects)

## Implied prerequisites
- Python programming basics
- mx.DateTime date arithmetic
- Forward curve construction
- Time value of money / discounting
- Power market settlement conventions
- CSV data handling
