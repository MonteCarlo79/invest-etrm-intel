# dipeng/DE Gas and Power

- id: `dipeng_de_gas_and_power` · class: practice · type: folder
- topic: German Gas and Power Markets Data and Modelling · level: intermediate · market: DE · year: None
- worked examples: True · code: False

## Concepts
- **Renewable Generation Normalisation** — Adjusting historical German solar and wind output for structural changes in installed capacity to produce comparable time series.
- **Long-Term Contract Pricing** — Valuation and structuring of multi-year gas or power supply contracts using formula-based or index-linked price mechanisms.
- **Gas-Oil Linkage** — Relationship between natural gas contract prices and crude oil benchmarks such as Brent used in legacy continental gas pricing.
- **EEX Power Price Data** — Historical spot and forward electricity price series sourced from the European Energy Exchange for German power markets.
- **NCG Gas Price Data** — Historical hub price series for the Net Connect Germany virtual trading point covering spot and forward gas markets.
- **TTF Gas Price Data** — Historical hub price series for the Title Transfer Facility benchmark used as a European gas price reference.
- **Wind Generation Data** — Historical output series for German onshore and offshore wind used as input to renewable modelling and capture-price analysis.
- **TGSA / Demand Charge Records** — Recorded Transportation and Gas Supply Agreement or distribution cost data used in supply cost stack analysis.
- **Brent Crude Historical Prices** — Time series of Brent crude benchmark prices used for oil-indexed gas contract formula calibration.
- **Go / Force Outage Historical Data** — Logged generation outage events capturing planned and unplanned capacity unavailability for power balance modelling.

## Methods
- Time-series data extraction and querying (ShellDataQuery tooling)
- Historical normalisation of renewable generation
- Long-term contract price formula construction
- Oil-index regression / linkage calibration
- Spread and basis analysis across hub prices
- Outage and availability factor estimation
- Excel-based scenario and sensitivity analysis

## Implied prerequisites
- Basic energy market structure (generation, transmission, supply)
- European gas hub mechanics (TTF, NCG)
- EEX market conventions and products
- Commodity price index arithmetic
- Time-series data handling in Excel
- Fundamental understanding of renewable intermittency
