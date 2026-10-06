# power/capacity market

- id: `power_capacity_market` · class: practice · type: folder
- topic: Capacity Market supplier charges and settlement · level: foundation · market: GB · year: 2016
- worked examples: True · code: False

## Concepts
- **Capacity Market Supplier Charge** — Monthly levy on licensed suppliers proportional to their share of net demand during periods of high demand, funding capacity provider payments.
- **Period of High Demand** — Defined window of 16:00–19:00 on working days from November to February used as the reference period for metered demand allocation.
- **Demand Forecast** — Forward-looking estimate of supplier net demand submitted by 1 June before a Delivery Year, used to set initial Supplier Charge and Credit Cover obligations.
- **Credit Cover** — Financial security (cash or letter of credit) lodged by suppliers at 110% of their monthly Supplier Charge, due 12 working days before each month.
- **Mutualisation** — Mechanism that redistributes a defaulting supplier's Supplier Charge across non-defaulting suppliers in proportion to their net demand.
- **Credit Default** — Two-stage process triggered when a supplier fails to lodge required Credit Cover, leading to mutualisation of their charge obligations.
- **Monthly Weighting Factor** — Profile factor applied to total annual capacity payments to determine the share attributable to each individual month of the Delivery Year.
- **Penalty Residual Supplier Amount** — Credit returned to suppliers representing residual capacity provider penalty payments net of over-delivery payments after a stress event.
- **Monthly Reconciliation** — Recalculation of each month's Supplier Charge replacing forecast demand with actual metered data and adjusting for revised capacity provider payments.
- **Annual Reconciliation** — End-of-Delivery-Year settlement process running three re-determination rounds that consolidates all monthly adjustments including Penalty Residual Supplier Amounts.
- **Settlement Costs Levy** — Separate charge on suppliers to recover the administrative costs of running EMR settlement, calculated using only final settlement run data.
- **Late Payment Interest** — Interest charged at 5% per annum above Bank of England base rate on overdue Supplier Charge, Settlement Costs Levy, and reconciliation amounts.
- **Net Demand** — Supplier metered demand net of embedded generation, adjusted for distribution losses but not transmission losses, used as the allocation base.
- **Delivery Year** — The contractual year over which capacity obligations are fulfilled, typically October to September, defining the reference period for all charge calculations.
- **EMR Settlement Limited (EMRS)** — The settlement body responsible for calculating, invoicing, and collecting Capacity Market charges from suppliers on behalf of the Electricity Settlements Company.

## Methods
- Pro-rata demand share allocation
- Forecast-to-actual data transition in reconciliation
- Credit cover sizing at 110% of monthly charge
- Mutualisation pro-rata redistribution
- Multi-run reconciliation timeline (3 monthly, 3 annual rounds)
- Settlement period-level metered data aggregation

## Implied prerequisites
- Basic electricity market structure (GB)
- Capacity Market auction mechanics
- BSC settlement calendar familiarity
- Metering and settlement data flows (BSC/DTC)
- Supplier licensing obligations
