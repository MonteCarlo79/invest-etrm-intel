# dipeng/Data

- id: `dipeng_data` · class: practice · type: folder
- topic: Power and energy market data aggregation and management · level: intermediate · market: mixed · year: 2016
- worked examples: False · code: True

## Concepts
- **Single imbalance price** — The administratively set reference price used to settle energy imbalances between nominated and actual generation or consumption in a given settlement period.
- **Imbalance settlement mechanism** — The regulatory and pricing framework by which transmission system operators financially settle deviations from contracted positions in real time.
- **Day-ahead market (DAM) price** — The hourly or half-hourly clearing price established in an exchange-run auction for electricity delivery the following day.
- **European Union Allowance (EUA) futures** — Exchange-traded futures contracts whose underlying asset is the right to emit one tonne of CO2 under the EU Emissions Trading Scheme.
- **API 2 coal price index** — A benchmark price index for coal delivered to Northwest European ports, used as a reference for fuel cost and dark-spread analysis.
- **TTF day-ahead gas price** — The spot or prompt gas price at the Title Transfer Facility virtual hub in the Netherlands, a primary benchmark for continental European natural gas.
- **Baseload power price** — The flat 24-hour average electricity price for a given delivery period, covering constant consumption or generation across all hours.
- **Peak power price** — The electricity price applicable only during defined on-peak hours within a delivery period, typically hours 8 to 20 on business days.
- **System Buy Price (SBP) and System Sell Price (SSP)** — The two-sided imbalance prices in the GB balancing mechanism at which the system operator buys or sells energy to correct system imbalances.
- **Wind generation forecasting model** — A quantitative model that converts meteorological wind observations or forecasts into expected electrical power output from wind farms.
- **Weather data feed** — Numerical weather prediction or observed meteorological data supplied by a third-party provider for use in generation and demand modelling.
- **Peg Nord gas price** — The spot and forward natural gas price at the French Peg Nord virtual trading hub, a reference for French and northern European gas markets.
- **NCG gas price** — The spot and forward natural gas price at the NetConnect Germany virtual trading hub, a primary German gas market benchmark.
- **Gaspool gas price** — The spot and forward natural gas price at the Gaspool virtual trading hub in northern Germany, one of two main German gas market entry points.
- **Emissions allowance options** — Derivative contracts granting the right but not the obligation to buy or sell EUA or CER carbon allowances at a specified price on or before expiry.
- **CER (Certified Emission Reduction)** — Carbon credits generated under the Kyoto Protocol Clean Development Mechanism, tradeable within the EU ETS up to regulatory limits.
- **Data normalisation (matrix to column conversion)** — The process of reshaping time-series price data from a date-by-hour matrix format into a single chronological column for downstream analysis.
- **Forward curve construction** — The assembly of a term structure of commodity prices from traded market instruments spanning prompt to long-dated delivery periods.

## Methods
- Data scraping and ingestion from exchange and TSO portals
- Spreadsheet-based data reformatting and normalisation
- Matrix-to-column time-series reshaping
- Python scripting for data blending
- Historical price series construction across multiple commodities
- Wind-to-power conversion modelling

## Implied prerequisites
- Familiarity with European electricity market structure
- Understanding of balancing mechanism and imbalance pricing
- Basic knowledge of EU Emissions Trading Scheme
- Spreadsheet data manipulation skills
- Elementary Python programming
- Understanding of gas hub pricing and virtual trading points
