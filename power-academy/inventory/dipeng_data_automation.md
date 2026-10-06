# dipeng/Data Automation

- id: `dipeng_data_automation` · class: practice · type: folder
- topic: Power and gas market data automation and volatility analysis · level: intermediate · market: mixed · year: None
- worked examples: True · code: True

## Concepts
- **Baseload volatility** — Annualised percentage price volatility of baseload power forward contracts across seasonal and calendar tenors.
- **Peak volatility** — Annualised percentage price volatility of peak power forward contracts across seasonal and calendar tenors.
- **Gas-power spread volatility** — Volatility of the price differential between a power forward and its associated gas benchmark, measured across contract tenors.
- **Gas-power correlation** — Pearson correlation between power forward prices and gas benchmark prices over equivalent contract periods.
- **NBP volatility** — Annualised price volatility of UK National Balancing Point gas forward contracts used as the gas leg in power-gas spread analysis.
- **TTF volatility** — Annualised price volatility of Dutch Title Transfer Facility gas forward contracts used as the gas benchmark for Continental power markets.
- **Forward contract tenors** — Standardised delivery periods—day-ahead, month-ahead, quarter-ahead, season-ahead, year-ahead—used to structure power and gas price series.
- **Seasonal segmentation** — Splitting historical price data into summer and winter delivery seasons to capture intra-year volatility patterns.
- **Data automation pipeline** — Systematic extraction, transformation and versioned storage of daily forward-price data across multiple markets and products using spreadsheet macros and Python scripts.
- **Market coverage across NL, UK, DE, FR, BE** — Parallel data collection for power markets in the Netherlands, United Kingdom, Germany, France and Belgium including baseload, peak and hourly products.
- **ETS carbon price data** — Automated collection and storage of EU Emissions Trading System allowance price series used alongside power and gas data.
- **API coal price series** — Automated collection of API coal benchmark price data integrated into multi-commodity forward-price datasets.

## Methods
- Historical volatility estimation
- Rolling correlation analysis
- Spread computation (power minus gas)
- Seasonal data segmentation
- Automated data extraction via Excel VBA macros
- Python scripting for file naming and data manipulation
- Versioned time-series output to timestamped Excel files

## Implied prerequisites
- Forward curve concepts in power and gas markets
- Basic statistics: variance, standard deviation, correlation
- Excel and VBA familiarity
- Elementary Python scripting
- Understanding of energy market products: baseload, peak, off-peak
