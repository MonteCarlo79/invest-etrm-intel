---
id: forward_curve_structure_and_products
track: hedging_trading
level: foundation
prerequisites: []
markets:
- EU
- GB
- US
- AU
status: drafted
sources:
- id: dipeng_rules
  use: practice_example
- id: power_rijnmond_dec17
  use: practice_example
- id: 111102_collector
  use: background
originality: synthesized
translations:
  zh:
    status: none
    en_hash: null
---
## Learning objectives

- Name the standard traded products (baseload, peakload, offpeak, week/weekend blocks) and their exact hour definitions per market.
- Explain the cascade structure (year → quarters → months → weeks) and why the curve must be arbitrage-consistent across it.
- Build an hourly curve from block quotes plus a shape, and verify it reprices every input quote.
- Compute implied offpeak from base and peak, and explain why a mismatch is an arbitrage.

## Intuition

Forwards do not trade in hours. They trade in blocks: "baseload March", "peakload Q3", "weekend next week". Plants, dispatch models, and risk systems, however, live in hours. The forward curve is the bridge: an hourly price series built from the traded block quotes, consistent with every one of them.

Two disciplines keep the bridge honest. First, **consistency**: baseload is by definition the average of all hours, so base, peak, and offpeak cannot move independently — if base is 50 and peak is 70, offpeak is 30, whether or not anyone quotes it. A curve that violates this is not a curve, it's three unrelated numbers, and a desk that trades on it gets picked off. Second, **shape**: the block quote fixes only the *average*; the hourly shape inside the block (peak hours higher in winter evenings, negative midday prices in solar hours) is where valuation and hedging actually happen. Shaping is an assumption, not a quote — say so, and keep the block averages exact.

## Formal treatment

**Products (EEX/GB convention).** Baseload: all 24h. Peakload: hours 08:00–20:00, Monday–Friday. Offpeak: the complement. Week/weekend blocks analogously. A product is a set of hours $B$ with a quoted average price $F_B = \frac{1}{|B|} \sum_{h \in B} p_h$.

**Consistency.** Since base hours partition into peak and offpeak hours,

$$F_{base} = \frac{|B_{pk}|\,F_{pk} + |B_{op}|\,F_{op}}{|B_{pk}| + |B_{op}|}, \qquad F_{op}^{implied} = \frac{|B|\,F_{base} - |B_{pk}|\,F_{pk}}{|B_{op}|}.$$

The implied-offpeak formula uses the **actual block hours of the calendar month** — peak days exclude weekends (and holidays), so the split is rarely 12/12. Only when peak and off-peak hours are equal does it reduce to the back-of-envelope $F_{op} = 2 F_{base} - F_{pk}$. Using the simplified formula on a real calendar misprices baseload — in the lab, by up to 6 €/MWh.

**Cascade.** A year contract = its quarters; a quarter = its months. The cascade must reprice: the year's price is the hour-weighted average of the quarters', and so on down. When only some levels are quoted, the curve inherits the finest available quote per period and back-fills the rest from the parent.

**Shaping.** Given a block quote $F_B$ and a shape profile $w_h$ (e.g. historical hourly pattern), set $p_h = F_B \cdot w_h / \bar w_B$ for $h \in B$ with $\bar w_B = \frac{1}{|B|}\sum_{h \in B} w_h$. The block average then equals the quote by construction, while the profile carries the assumed shape.

## Worked example

Three monthly quotes: base $[50, 55, 48]$, peak $[70, 75, 65]$ €/MWh (simplified: every month has 12 peak + 12 offpeak hours per day).

The synthetic calendar (3 months × 10 days, weekends excluded from peak) gives 96/84/84 peak hours and 144/156/156 off-peak hours per month.

- Implied offpeak with the actual block hours: $[36.7, 44.2, 38.8]$ €/MWh (the 12/12 shortcut would give $[30, 35, 31]$ — visibly wrong, and the volume-weighted base then misses the quote by several €/MWh).
- Hourly curve with a peak-hour profile $w_h$: block averages reprice base and peak quotes to $< 10^{-6}$; the shaped peak-block energy matches the quote times block hours exactly; the equal-hours special case is asserted as a special case, not the rule.

`labs/forward_curve_structure_and_products/compute.py` does the build; its test asserts implied offpeak, repricing to 1e-6, and shape preservation.

## Market variants

- **EU (EEX/EPEX):** peak 08–20 Mon–Fri; German auction calendar drives day-ahead shape; holiday calendars matter for block hour counts.
- **GB:** EFET peak 07–19 weekdays on the OTC market; N2EX/EPEX day-ahead for shape.
- **US:** 5x16 (peak), 7x24 (base), 2x16 (offpeak) conventions per ISO; weekends and NERC holidays define block membership.
- **AU:** quarters are the liquid product; peak defined 07:00–22:00 weekdays.
- **CN:** 中长期 contracts trade in 分时段 blocks (尖峰/峰/平/谷 time-of-use buckets) that differ by province; the "shape" is administratively defined per province's time blocks, and the base-vs-block consistency check is the same discipline in 元/MWh.

## Common errors

1. **Base ≠ weighted peak/offpeak** — quoting (or worse, trading) the three independently; the inconsistency is arbitrage, not a view.
2. **Weekends in the peak block** — peak excludes weekends by definition; including them understates the peak average.
3. **Shape that breaks the block average** — any profile must be normalised per block; an unnormalised shape silently moves the quote.
4. **Ignoring the cascade** — building months from the year but forgetting to check the quarter reprices; downstream models then value months inconsistently with the traded quarter.
5. **Wrong calendar** — block hour counts shift with holidays; a fixed 12/12 split is an approximation, state it.

## Assessable questions

1. Base is 60 and peak 85 €/MWh. What is implied offpeak?
2. Write the peak product's hour set under the EEX convention and under the US 5x16 convention.
3. Why must a shaped hourly curve reprice the block quote exactly, and what breaks if it doesn't?
4. A quarter's three months quote 50, 55, 48. The quarter trades at 52. Is there an inconsistency? (Months have equal hours for simplicity.)
5. Explain the difference between the quote (block average) and the shape (profile within the block), and name one decision that depends on each.
