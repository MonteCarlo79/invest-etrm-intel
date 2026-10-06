# main

- id: `main_2` · class: library · type: txt
- topic: Power tolling asset valuation · level: intermediate · market: GB · year: 2012
- worked examples: True · code: True

## Concepts
- **Tolling agreement valuation** — Valuing the optionality embedded in a power tolling contract over a specified delivery period.
- **Valuation date vs. delivery period** — Distinguishing the mark-to-market date from the forward start and end dates of the tolled asset.
- **Date collection / delivery schedule** — Constructing an ordered sequence of daily dates spanning the tolling delivery window.
- **Discount factor** — Computing present-value discount factors on a daily basis using a given continuously- or annually-compounded rate.
- **Forward price selection** — Extracting market forward power prices observed on a specific price date and mapping them to the delivery date list.
- **Market data ingestion** — Locating, reading and parsing CSV price files supplied by a named market data provider.

## Methods
- Daily discount factor construction
- CSV market data parsing
- Date-series generation
- Forward price curve mapping

## Implied prerequisites
- Python 2.x programming
- mx.DateTime date handling
- Power market forward curve structure
- Discounted cash flow fundamentals
- Tolling contract mechanics
