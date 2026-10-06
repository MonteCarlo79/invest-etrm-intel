# dipeng/Power Deal Structuring

- id: `dipeng_power_deal_structuring` · class: practice · type: folder
- topic: GB Electricity Balancing Mechanism and Cash-Out Reform · level: intermediate · market: GB · year: 2014
- worked examples: False · code: False

## Concepts
- **Balancing Mechanism** — The real-time market layer where participants submit bids and offers to decrement or increment output, with National Grid as sole counterparty, covering approximately 2% of volume.
- **Gate Closure** — The point one hour before a settlement period at which participants must submit their final contracted positions before balancing actions are taken.
- **Dual Cash-Out Pricing** — The existing imbalance settlement mechanism that applies a more favourable price to parties that aid system balance and a harsher marginal price to parties that worsen it.
- **Single Cash-Out Pricing** — A proposed reform that removes the price spread between opposing imbalance directions, applying one price regardless of whether the party aids or worsens balance.
- **Price Average Reference (PAR)** — The volume of the most expensive balancing actions over which a weighted average price is calculated for imbalance settlement, subject to phased reduction toward marginal pricing.
- **Marginal Pricing** — The pricing approach achieved as PAR is reduced to 1 MWh, causing the cash-out price to reflect the single most expensive balancing action dispatched.
- **Reserve Scarcity Pricing (RSP)** — A scarcity-based pricing function that determines the cost of reserve actions in cash-out based on the level of capacity available on the system at gate closure.
- **Value of Lost Load (VoLL)** — An administratively set price used to value demand control actions in the balancing stack, escalating from £3,000/MWh to £6,000/MWh across the reform timeline.
- **Demand Control Pricing** — The mechanism that prices involuntary demand curtailment actions into cash-out at VoLL, creating a scarcity signal during tight system margins.
- **Imbalance Volume** — The difference between a participant's contracted position and their metered output or import, settled financially in half-hourly periods.
- **Half-Hourly Settlement** — The settlement framework in which each 30-minute period is separately metered and financially settled, proposed for mandatory extension to profile classes 5–8.
- **Profile Classes** — Demand customer categories (1–8) that determine metering and settlement methodology, with classes 5–8 covering non-domestic customers below 100 kW.
- **Smart Meter Roll-Out** — The programme to install half-hourly-capable meters across domestic and small-business consumers between 2015 and 2020, enabling time-of-use settlement.
- **EBSCR (Electricity Balancing Significant Code Review)** — Ofgem's coordinated review and reform package covering marginal pricing, single cash-out, reserve scarcity pricing and demand control pricing with phased implementation timescales.

## Methods
- Merit-order stacking of balancing actions by price
- Weighted average price calculation over PAR volume
- Scarcity pricing function (RSP) based on system margin at gate closure
- Administrative VoLL setting for demand control
- Phased regulatory implementation via Code Modifications

## Implied prerequisites
- Basic electricity market structure (forwards, spot, balancing)
- Concepts of system operator roles and responsibilities
- Fundamentals of electricity imbalance settlement
- UK energy regulation and Ofgem's mandate
- Metering and settlement period conventions
