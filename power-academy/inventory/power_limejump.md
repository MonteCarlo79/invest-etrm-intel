# power/Limejump

- id: `power_limejump` · class: practice · type: folder
- topic: Power Purchase Agreement intermediation and back-to-back PPA structuring · level: intermediate · market: GB · year: 2017
- worked examples: False · code: False

## Concepts
- **Power Purchase Agreement (PPA)** — Contractual arrangement for the sale and purchase of electricity output from a generation asset, covering pricing, volume and term.
- **Back-to-back PPA** — Structure in which an intermediary passes through physical energy flows and pricing terms between a generator and an offtaker via two mirrored contracts.
- **Grid Trade Master Agreement (GTMA)** — Standard UK bilateral electricity trading master agreement governing the terms of individual power transactions between parties.
- **Imbalance risk** — The financial exposure arising from differences between contracted/nominated volumes and actual metered generation settled under the GB balancing mechanism.
- **ECVN / MVRN (Energy Contract Volume Notification / Metered Volume Reallocation Notification)** — Elexon notification mechanisms used to reallocate contracted and metered generation volumes between supplier licence holders in the SVA settlement process.
- **Supplier Volume Allocation (SVA)** — The GB half-hourly settlement process that allocates metered and nominated generation volumes to the relevant licensed electricity supplier.
- **MPAN (Meter Point Administration Number)** — Unique identifier for a UK electricity supply point used for registration, data flows and settlement.
- **Embedded benefits** — Financial payments accruing to embedded generators connected to distribution networks, including TNUoS triad avoidance and distribution use-of-system credits.
- **Renewable obligation certificates (ROCs)** — UK tradeable certificates issued to accredited renewable generators per MWh produced, used to demonstrate compliance with the Renewables Obligation.
- **Feed-in Tariff (FiT)** — UK government subsidy providing guaranteed generation and export payments to small-scale low-carbon generators below 5 MW.
- **Renewable Energy Guarantee of Origin (REGO)** — Certificate issued to renewable generators in GB certifying the renewable origin of each MWh for disclosure and marketing purposes.
- **Fixed-price PPA** — PPA variant in which the wholesale power price is locked at a specified £/MWh for the contract term, transferring market price risk to the offtaker.
- **Flexible / index PPA** — PPA variant in which the power price is linked to a market index with a pass-through percentage, retaining some price exposure for the generator.
- **Combination PPA** — PPA structure blending fixed-price ECVN/MCVN volumes with an index-linked product for the remaining metered volume.
- **Step-in rights** — Contractual mechanism allowing a creditworthy counterparty to assume the obligations of a defaulting intermediary and continue the PPA.
- **Know Your Customer (KYC) / due diligence** — Regulatory and counterparty credit checks performed on generators before entering into individual PPA transactions.
- **Credit support** — Security arrangements (parent company guarantee, investment-grade rating or debt instrument) required to underwrite counterparty obligations under each individual PPA.
- **Day-ahead nomination** — The process of submitting forecast generation volumes to Elexon on a day-ahead basis under the SVA settlement timetable.
- **R1 imbalance reconciliation** — First formal reconciliation of initial settlement imbalance charges issued by Elexon approximately two months after the delivery period.
- **Imbalance premium** — An additional charge included in PPA pricing to compensate the offtaker for bearing the financial risk of forecast versus actual generation imbalance.
- **Supplier licence registration** — The requirement for a licensed electricity supplier to register generation MPANs under its licence for settlement and billing purposes.
- **Self-billing invoice** — Invoice raised by the buyer on behalf of the seller, used here to pay the generator under the PPA without a separate generator invoice.
- **Generation forecasting** — Prediction of site-level and portfolio-level output used to produce nominations and manage imbalance exposure ahead of delivery.

## Methods
- Back-to-back contractual structuring
- Day-ahead generation forecasting and nomination
- Dual-invoice billing engine design
- Imbalance risk allocation and hedging
- KYC and credit due diligence workflow
- Embedded benefit calculation and pass-through
- Step-in rights structuring for credit events
- Quote generation with bid-offer spread management

## Implied prerequisites
- GB electricity market settlement and balancing mechanism
- Elexon SVA data flows and notification types
- Renewables Obligation and FiT subsidy schemes
- Basics of power purchase agreements
- Counterparty credit risk assessment
- UK supplier licence obligations
- Half-hourly metering and data reconciliation
