# dipeng/NL CSS

- id: `dipeng_nl_css` · class: practice · type: folder
- topic: Dutch Clean Spark Spread (CSS) Daily Options · level: intermediate · market: NL · year: 2013
- worked examples: False · code: False

## Concepts
- **Clean Spark Spread (CSS)** — The margin from converting gas to power net of carbon cost, used as the underlying for the option structure.
- **Daily Call Option on CSS** — A strip of European-style daily exercisable options giving the holder the right to receive physical power delivery against a floating price each calendar day.
- **Floating Price Formula** — The settlement price for each delivery day constructed from TTF gas price, EUA carbon price, and a fixed efficiency coefficient, plus a strike.
- **TTF Day-Ahead Index** — The Heren-published day-ahead Dutch gas price used as the gas cost input in the CSS floating price calculation.
- **EUA Carbon Cost** — The ICE ECX front December EUA futures settlement price applied per tonne of CO2, scaled by a carbon intensity factor, to compute the clean component of the spread.
- **Gas Efficiency Factor** — The thermal conversion ratio (MWh gas per MWh power, e.g. 2 MWhg/MWhe = 50%) linking gas consumption to power output in the spread formula.
- **Strike Price** — The fixed floor level (here 0 EUR/MWh) subtracted from the floating price to determine option intrinsic value on each exercise day.
- **D-1 Exercise** — The option exercise mechanism whereby the holder must notify the writer by a specified deadline on the business day before each delivery day.
- **Physical Settlement** — The delivery obligation under which the writer supplies baseload or peak power on the Dutch HV grid on exercised days, with corresponding gas flow from holder to writer.
- **Quarter-Ahead vs Day-Ahead Reference Price** — The distinction between forward quarterly indices (used to set price floors) and day-ahead spot indices (used for daily settlement), relevant to index selection for NAM calculations.
- **Peak vs Baseload Delivery Profile** — The distinction between full 24-hour baseload delivery and restricted peak-hour delivery (e.g. Monday–Friday 08:00–20:00 CET) in the physical power leg.
- **Non-exercise Default** — The contractual provision that if the holder fails to nominate, the volume for that delivery day is assumed zero and no physical delivery occurs.

## Methods
- Floating price construction from component indices
- Index benchmarking against actual traded data (Trayport)
- Carbon-adjusted spark spread calculation
- Strip option structuring (daily exercise schedule)
- Physical nomination and settlement workflow

## Implied prerequisites
- Spark spread fundamentals
- European energy commodity markets (power and gas)
- Options pricing basics (call options, strike, premium)
- Carbon markets (EU ETS, EUA futures)
- EFET/ISDA confirmation documentation
- Energy market index conventions (Heren, ICE ECX)
