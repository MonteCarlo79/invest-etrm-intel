# power/Rijnmond Dec17

- id: `power_rijnmond_dec17` · class: practice · type: folder
- topic: Natural gas transmission service conditions and tariff framework · level: intermediate · market: NL · year: 2017
- worked examples: True · code: False

## Concepts
- **Entry-exit system** — A transmission framework separating entry capacity rights from exit capacity rights at defined points on the national grid.
- **Firm vs interruptible capacity** — A distinction between guaranteed transmission capacity and capacity subject to interruption by the TSO, with interruptible capacity priced at a discount to firm tariffs.
- **Capacity tranche** — A band of interruptible capacity levels sharing a common probability of interruption and corresponding tariff reduction class.
- **Monthly factor** — A seasonal weighting coefficient applied to tariff calculations reflecting the differential value of winter, shoulder and summer gas months.
- **Wheeling** — A cross-border transport service allowing a shipper to move gas through the national grid between specified entry-exit combinations at a fixed per-kWh-per-hour-per-year tariff.
- **Backhaul capacity** — Transmission capacity enabling counter-flow gas movement, where the relevant entry point is treated as an exit point and vice versa.
- **Diversion** — A service allowing a shipper to alter contracted flat capacity at an entry or exit point, charged as a fixed administrative fee per request.
- **Title Transfer Facility (TTF)** — A virtual entry-exit point on the Dutch national grid at which shippers can transfer gas title anonymously, supported by a registration service provided by GTS.
- **Transfer of capacity rights** — A service enabling one shipper to assign contracted entry or exit capacity rights to another party, subject to a fixed registration fee.
- **Nomination and renomination** — The hourly scheduling messages submitted by a shipper to GTS specifying quantities of gas to be delivered or received at each entry and exit point within a portfolio.
- **Portfolio balancing** — The obligation on a shipper to maintain equality between entry gas and exit gas quantities on an hourly basis within a given portfolio.
- **Capacity overshoot** — A penalty condition triggered when actual entry or exit gas in an hour exceeds 102% of contracted capacity.
- **Contract unit** — A standard twelve-month (or shorter residual) billing period over which capacity is ranked and tariff calculations are performed.
- **Capacity ranking (TR/WR)** — The ordering of monthly contracted capacities from largest to smallest within a contract unit to apply the stepped seasonal tariff formula.
- **Tariff formula for entry/exit/wheeling** — A mathematical expression combining ranked capacity differences, seasonal monthly factors and unit tariffs to compute monthly charges for transmission services.
- **Surrender of capacity** — A process by which a shipper returns contracted capacity to GTS for re-auction, subject to advance notice requirements aligned with the PRISMA auction calendar.
- **Capacity conversion** — A service enabling a shipper to bundle unbundled firm entry or exit capacity at one side of an interconnection point with capacity at the other side into a bundled product.
- **Wobbe label** — A gas quality classification assigned to each entry or exit point defining the calorific value band applicable for network code compliance.
- **EURIBOR-linked interest** — A payment default and settlement adjustment mechanism tying late-payment and provisional-invoice interest rates to the one-month EURIBOR rate plus a spread.
- **Credit limit and financial security** — A creditworthiness framework under which GTS can require additional collateral from shippers whose imbalance exposure exceeds an assigned credit threshold.
- **Exit capacity contract terms** — Contractual rules governing reduction, termination and notice periods for firm exit-capacity bookings on the Dutch national gas grid.
- **Capacity decrease penalty structure** — Tiered charge schedule applied when a shipper reduces contracted exit capacity by more than specified percentage thresholds within a notice period.
- **Grid Connection Agreement (GCA)** — Separate bilateral agreement between GTS and an end-user governing physical connection at an entry or exit point, which can supersede general conditions.
- **Operational Balancing Agreement (OBA)** — Agreement at an entry or exit point facilitating gas-flow balancing procedures between GTS and adjacent network operators.
- **Force majeure provisions** — Contractual conditions under which a party is relieved of gas-transport obligations, including carve-outs for payment and system-balancing duties.
- **Liability cap structure** — Per-event monetary ceiling on TSO or shipper liability linked to contracted service value at the relevant entry/exit point.
- **Wilful misconduct and gross negligence carve-out** — Exception to limited-liability regime when damage arises from intentional or recklessly negligent acts by managerial-level personnel.
- **Assignment and portfolio transfer** — Rules permitting transfer of all rights and obligations under a transport agreement, restricted to whole-portfolio transfers and requiring counterparty consent.
- **CCGT dispatch optimisation** — Hourly scheduling of a combined-cycle gas turbine to maximise spread between power revenues and fuel plus carbon costs.
- **Spark spread / dark spread valuation** — Margin metric capturing the difference between electricity price and variable generation cost (gas consumption plus CO2 allowances) for a thermal plant.
- **Forward curve construction (power, gas, carbon)** — Building term price curves for Dutch electricity, TTF gas and EUA CO2 from market quotes used as inputs to plant valuation models.
- **Intrinsic value vs. extrinsic (optionality) value** — Decomposition of a generation asset's value into the locked-in dispatch margin from current forward curves and the additional option value from price volatility.
- **Hourly backtesting of dispatch** — Historical simulation of plant dispatch decisions at hourly resolution to validate optimisation model performance against realised prices.
- **Shape price / profile value** — Within-day and within-month price differentials for power that reflect load-shape patterns and affect peaking-plant revenues.
- **Term management and hedging strategy** — Rolling programme of forward sales and fuel/carbon purchases to lock in margins on committed plant generation over multiple delivery periods.
- **Dutch Network Code (Transmissiecode Gas – TSO)** — Regulatory code governing balancing obligations, nomination procedures and TSO–shipper rights on the Dutch high-pressure gas network.
- **Euribor-linked late-payment interest** — Contractual interest rate benchmark (Euribor plus spread) applied to overdue amounts under the gas transport service contract.
- **Confidentiality and information-sharing obligations** — Contractual framework restricting disclosure of transport agreement data and defining permitted exceptions for affiliates, regulators and financiers.

## Methods
- Seasonal capacity ranking algorithm
- Weighted-average monthly factor calculation
- Interruptible tariff discount calculation via capacity tranches
- Stepped tariff formula (capacity range decomposition)
- EURIBOR-based interest accrual
- Provisional and final invoice reconciliation
- Hourly plant dispatch optimisation
- Forward curve calibration (power, gas, CO2)
- Spark/clean-spark spread calculation
- Intrinsic value extraction from forward curves
- Stochastic/Monte Carlo plant valuation (implied by extrinsic value files)
- Historical backtesting of dispatch decisions
- Shape-price decomposition
- Rolling term-management hedge accounting

## Implied prerequisites
- Entry-exit gas transmission system architecture
- Natural gas unit conventions (kWh, m³(n), bar)
- Gas day and gas month definitions
- Basic capacity contract structures
- Nomination and scheduling processes in gas networks
- Dutch Gas Act and Network Code framework
- European gas market regulation (EC 715/2009)
- Commodity derivatives pricing
- Energy market fundamentals (gas, power, carbon)
- Thermal generation economics and heat-rate modelling
- Options theory (real options on physical assets)
- Dutch/EU gas market structure and regulation
- EU Emissions Trading System mechanics
- Numerical optimisation basics
- Contract law and regulatory framework familiarity
