# power/German I&C

- id: `power_german_i_c` · class: practice · type: folder
- topic: German industrial & commercial power supply: operations setup, balancing-group management, load forecasting, tranche procurement and imbalance pricing · level: advanced · market: DE · year: 2016
- worked examples: True · code: False

## Concepts
- **Balancing Group (Bilanzkreis)** — A virtual energy account registered with a TSO into which all supply and offtake volumes of a market participant must be allocated to achieve settlement balance.
- **Balancing Group Coordinator (TSO role)** — The transmission system operator acting as administrator of balancing groups, responsible for invoicing imbalance energy to market participants within each control area.
- **Control Area (Regelzone)** — One of four geographically defined high-voltage network zones in Germany, each operated by a separate TSO, determining where energy schedules and imbalance settlement apply.
- **MaBiS (Marktprozesse für die Bilanzierung Strom)** — The regulatory framework governing pre- and post-monthly processing, time-series allocation, and invoicing of supplier and balancing-group energy volumes with TSOs.
- **GPKE (Geschäftsprozesse zur Kundenbelieferung mit Elektrizität)** — Standardised business processes mandated by the Bundesnetzagentur for registering and de-registering end customers with distribution system operators.
- **Standard Load Profile (SLP)** — A synthetic consumption time series assigned to small customers without interval metering, used by suppliers and balancing-group managers for forecasting and settlement.
- **Daily Parameter-Dependent Load Profile (TLP)** — A load profile adjusted each day by external parameters such as temperature, used for medium-consumption customers whose metered data is not read in real time.
- **Registered Power Measurement (RLM)** — Interval metering applied to large industrial customers, providing actual 15-minute consumption data for scheduling and settlement rather than synthetic profiles.
- **Load Forecasting** — The process of predicting customer consumption at 15-minute resolution ahead of delivery to generate trading and scheduling positions and minimise balancing energy costs.
- **Scheduling (Fahrplanmanagement)** — The regulated daily communication process of nominating energy volume time series to TSOs for each control area ahead of and during the delivery day.
- **Intraday 15-Minute Contract Trading** — The sub-process of buying or selling 15-minute structured power products on intraday markets to close residual portfolio imbalances that cannot be offset by day-ahead positions.
- **REBAP (Regelenergiebeschaffungs- und Abrechnungsprozess / Imbalance Price)** — The German balancing energy price time series published by the TSOs' grid control cooperation, reflecting the cost of deviations between scheduled and actual energy volumes.
- **Horizontal Tranche Procurement** — A structured forward-buying strategy in which annual baseload volume is divided into equally-shaped tranches purchased incrementally over time at EEX forward market prices.
- **Asian Option on Power** — An option whose payoff depends on the arithmetic average of the underlying power price over the option period, used here for calendar-year electricity hedges with separate call and put volatilities.
- **Transportation Pass-Through (TPT)** — A contractual clause shifting the financial risk of grid-use charges directly to the end customer, eliminating the supplier's exposure to changes in distribution network tariffs.
- **Full-Time Equivalent (FTE) Costing** — A staffing and cost-estimation method that converts operational time requirements into standardised headcount units to quantify personnel expenditure for new business processes.
- **Make-or-Buy / Outsourcing Assessment** — A structured evaluation framework applying company-specific and company-independent criteria to determine whether operational processes should be insourced or contracted to third parties.
- **ETRM System (Energy Trading and Risk Management)** — An integrated software platform, here Aligne, that serves as the central database for trade capture, position management, and downstream data provision across operations systems.
- **Demand-Supply Balancing** — The process of monitoring contracted supply and demand volumes, identifying deviations, and reporting balancing energy exposure including ex-post analysis of spot and imbalance volumes.
- **DSO Contract and Grid Registration** — The legal and operational process by which a supplier concludes framework agreements with distribution system operators and registers or de-registers metering points under GPKE.
- **Market Roles (Marktrollen)** — The legally defined participant categories in the German power market—supplier, balancing group manager, distribution system operator, TSO—each carrying distinct contractual and regulatory obligations.
- **Balancing Energy (Regelenergie)** — Energy activated by TSOs to correct real-time deviations between scheduled and actual power flows, charged back to the responsible balancing group at REBAP prices.
- **StromNZV / StromNEV Compliance** — Adherence to the German Electricity Grid Access Ordinance and Grid Charge Ordinance governing entry conditions, network access rights, and tariff structures for power market participants.

## Methods
- 15-minute interval load data processing
- Statistical and parametric load forecasting
- Balancing group time-series allocation
- Forward tranche (horizontal) procurement scheduling
- Asian option pricing with separate call/put volatilities
- Make-or-buy scoring matrix
- FTE-based personnel cost estimation (gross and net with synergy adjustment)
- System landscape evaluation and module cost estimation
- EPEX intraday auction price benchmarking
- Process mapping and contract life-cycle analysis

## Implied prerequisites
- German power market structure and regulatory framework (StromNZV, StromNEV, EnWG)
- Balancing group mechanics and TSO settlement processes
- Energy trading fundamentals (spot, forward, intraday markets)
- Load profile types (SLP, TLP, RLM) and their use in settlement
- Basic options pricing concepts (call/put, implied volatility)
- ETRM system architecture
- Project management and organisational design
- German energy law and Bundesnetzagentur regulatory regime
