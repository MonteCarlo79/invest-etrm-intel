# power/Peaker

- id: `power_peaker` · class: practice · type: folder
- topic: GB gas peaker tolling and route-to-market structuring · level: advanced · market: GB · year: 2018
- worked examples: True · code: False

## Concepts
- **Tolling agreement** — A contractual structure whereby a toller (SEEL) takes full dispatch control of generation assets and pays a capacity-based fee to the asset owner in exchange for the commodity margin.
- **Adjusted Gross Margin** — The net revenue metric defined as power revenues minus gas costs, carbon price floor, and variable O&M, plus embedded benefits and non-CM ancillary revenues, used as the profit-sharing base.
- **Clean Spark Spread** — The margin earned by a gas-fired generator from selling power after deducting gas fuel cost and carbon cost, used here as the core commodity gross margin component.
- **Profit-share / floor / cap structure** — A tiered commercial arrangement specifying a minimum annual fee floor, a profit-share threshold, and an upper cap beyond which incremental margin is split at a different ratio between toller and asset owner.
- **Embedded Benefits (GDUoS)** — Revenue credits accruing to sub-transmission-connected (11 kV) generators arising from avoided Distribution Use of System charges, segmented into red, amber, and green time bands.
- **Capacity Market revenue** — Payments received under the GB Capacity Market auction, explicitly excluded from the tolling gross margin and retained by the asset owner.
- **Balancing Mechanism (BM) / NIV-chasing** — GB real-time dispatch optimisation through National Grid's balancing mechanism, including submission of bids and offers and opportunistic response to net imbalance volume signals.
- **Route-to-market service** — The bundled provision of gas supply, power marketing, meter-point registration, and all trading decisions for a portfolio of generation assets by a single counterparty.
- **Imbalance / forecast error risk** — The financial exposure arising from deviations between scheduled and actual generation or load, driving the strategic rationale for holding flexible peaking capacity.
- **System price / capture premium analysis** — Historical analysis of GB system price exceedance above a threshold (e.g. £60/MWh) used to estimate the gross margin available to peaking plant during tight market periods.
- **STOR (Short Term Operating Reserve)** — An ancillary service product procured by National Grid for short-notice reserve delivery, included as an additional non-CM revenue stream within the tolling margin definition.
- **Plant availability and efficiency obligations** — Contractual requirements on the asset owner to maintain a minimum availability (95%) and specified thermal efficiency, with penalty and non-firm capacity provisions for shortfalls.
- **Transition / readiness period** — An interim commercial phase prior to full BM capability during which dispatch is limited to day-ahead nominations, with intraday and balancing activity handled by the incumbent operator.
- **BSUoS (Balancing Services Use of System)** — A system charge levied on generators for their contribution to balancing costs, explicitly excluded from embedded benefits in the tolling margin calculation.
- **Portfolio aggregation and shape management** — The use of flexible peaking capacity to cover forecast errors across a renewable and load aggregation portfolio, quantified in GWh and MW terms.

## Methods
- Historical system price exceedance analysis
- Gross margin back-testing against price history (2014–2017)
- Capacity-based fee and profit-share tier modelling
- Embedded benefits quantification by DNO zone and time band
- Clean spark spread calculation
- Forecast error sizing for portfolio imbalance exposure
- Forward curve building (GB power and gas)
- Peaker dispatch optimisation / valuation modelling

## Implied prerequisites
- GB electricity market structure (wholesale, BM, imbalance settlement)
- Gas-fired generation economics and heat-rate concepts
- UK Capacity Market rules and auction mechanics
- Distribution network charging methodology (DUoS/GDUoS)
- Ancillary services landscape (STOR, BM)
- Commodity derivatives and forward curve construction
- Tolling and power purchase agreement legal frameworks
- Renewable energy portfolio management and imbalance risk
