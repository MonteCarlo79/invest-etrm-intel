---
id: heat_rate_and_plant_parameters
track: asset_valuation
level: foundation
prerequisites:
- spark_dark_spread_fundamentals
markets:
- EU
- GB
- US
status: drafted
sources:
- id: ccgt_replication
  use: derivation_reference
- id: clewlow
  use: background
- id: framework_tseng
  use: background
- id: optimal_switching_with_applications_to_energy_tolling_agreem
  use: background
- id: power_european_toll
  use: practice_example
originality: synthesized
translations:
  zh:
    status: drafted
    en_hash: 20c6c584894a23dd
---
## Learning objectives

- List the physical parameters of a thermal plant that a valuation or dispatch model needs, and explain what each one costs when ignored.
- Explain why heat rate is a *curve* (function of load), not a constant, and compute marginal cost at partial load.
- Compute the correct start-cost tier from offline duration (hot / warm / cold).
- State minimum up/down times and ramp rates as constraints on the set of reachable operating schedules.

## Intuition

A power plant is not a light switch; it is closer to a truck. It has a minimum speed below which it stalls (minimum stable load), it burns more fuel per kilometre in city traffic than on the motorway (heat-rate curve), it takes time and fuel to warm up after sitting in the cold (start costs), and once started it should stay on the road for a while (minimum up time). Valuation models that treat the plant as a switch systematically overstate its value — they let it capture spreads it physically cannot reach.

For modelling, a plant is a small parameter sheet. Get the sheet right and the spread maths from the previous concept turns into realistic cashflows; get it wrong and the most sophisticated stochastic model still prices a plant that does not exist.

## Formal treatment

**Output limits.** Maximum Export Limit (MEL, nameplate) and Stable Export Limit (SEL, minimum stable generation, typically 40–60% of MEL for CCGT). Output $q$ must satisfy $q \in \{0\} \cup [SEL, MEL]$ — nothing in between.

**Heat-rate curve.** Efficiency is best at full load and degrades at partial load, so heat rate rises as output falls. A workable two-point approximation:

$$HR(q) = HR_{full} + (HR_{min} - HR_{full}) \cdot \frac{MEL - q}{MEL - SEL}, \quad q \in [SEL, MEL]$$

with $HR_{full}$ at MEL and $HR_{min}$ at SEL. Marginal fuel cost at output $q$ is $HR(q) \cdot G$ (plus carbon $HR(q) \cdot E \cdot ef$ in clean form). Because $HR$ rises at low load, the *marginal* cost at SEL can exceed the full-load figure by 15–25%.

**Start costs by tier.** The boiler and turbine cool down once offline; restarting costs fuel and wear, more the colder the machine:

$$SC(\tau) = \begin{cases} c_{hot}, & \tau < T_1 \\ c_{warm}, & T_1 \le \tau < T_2 \\ c_{cold}, & \tau \ge T_2 \end{cases}$$

with $\tau$ the offline duration and typical boundaries $T_1 \approx 6$–8h, $T_2 \approx 36$–48h for CCGT (coal: longer).

**Inter-temporal constraints.** Minimum up time $U$ (once on, stay on at least $U$ hours), minimum down time $D$ (once off, stay off at least $D$ hours), ramp rate $r$ (MW/min) limiting $|q_t - q_{t-1}|$, and variable O&M $v$ (€/MWh) added to marginal cost.

## Worked example

Plant: $MEL = 500$ MW, $SEL = 200$ MW (40%), $HR_{full} = 2.0$, $HR_{min} = 2.4$ MWh_th/MWh_e, start tiers $c_{hot} = 30$k€, $c_{warm} = 60$k€, $c_{cold} = 120$k€ at $T_1 = 6$h, $T_2 = 48$h; gas $G = 30$ €/MWh_th, carbon ignored for readability.

- Marginal cost at full load: $2.0 \times 30 = 60.0$ €/MWh.
- Marginal cost at 60% load ($q = 300$ MW): $HR(300) = 2.0 + 0.4 \times \frac{500-300}{300} = 2.267$ → $2.267 \times 30 = 68.0$ €/MWh — 13% above the full-load figure.
- The plant was offline 20h → warm tier → start cost 60 k€.
- Amortised over a minimum run of $U = 8$h at MEL, the start adds $60{,}000 / (8 \times 500) = 15.0$ €/MWh to the break-even spread of that run.

`labs/heat_rate_and_plant_parameters/compute.py` reproduces each figure; its test asserts them.

## Market variants

- **EU/GB:** parameter sheets feed clean-spread dispatch, so heat-rate curves are quoted alongside carbon intensity; balancing-market and reserve products pay for precisely the parameters the energy market ignores (ramp, part-load capability).
- **US:** heat-rate curves in BTU/kWh vs load are standard in bids (PJM/MISO incremental offer curves); start offers are split hot/intermediate/cold with market-verified boundaries.
- **CN:** the equivalent parameter is 供电煤耗 (net coal-consumption rate, gce/kWh) quoted at load points; start costs are rarely published and must be estimated from coal consumed during 启动 plus ignition oil; AGC and 最小方式 constraints play the role of ramp/min-run rules.

## Common errors

1. **Using nameplate efficiency everywhere** — a model with a single heat rate overstates value at partial load, exactly where peaking plants live.
2. **Treating SEL as zero** — the hours between 0 and SEL are not reachable; dispatch models must force on/off, not a continuum down to zero.
3. **Start-tier boundary errors** — 6h offline is not warm if the tier is defined as $\tau < 6$h hot; check which side of the boundary is inclusive.
4. **Double-counting VOM** — adding VOM inside the spread and again as a separate cost line.
5. **Ignoring amortisation** — comparing a 120 k€ cold start to one hour's margin; start costs only make sense spread over the minimum run they commit you to.

## Assessable questions

1. With the plant above, what is the marginal cost at 80% load?
2. The plant goes offline Friday 22:00 and is needed Monday 06:00 (56h). Which start tier applies, and what does the start cost?
3. Why does the heat-rate curve make a plant *less* flexible in value terms than the on/off switch model assumes?
4. A cold start costs 120 k€ and the expected clean spark is 45 €/MWh at MEL = 500 MW. How many hours must the plant run to break even on the start alone?
5. Convert a US incremental heat rate of 7,800 BTU/kWh at 60% load to MWh_th/MWh_e and to efficiency.
