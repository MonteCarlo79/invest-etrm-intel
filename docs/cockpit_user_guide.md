# BESS Options Cockpit — User Guide & Methodology

**Location:** `apps/mengxi-dashboard/app.py` → Options Cockpit tab  
**Audience:** Traders, Quants, Asset Managers  
**Last updated:** 2026-04-22

---

## 1. What the Cockpit Does

The cockpit treats each BESS asset as a **strip of daily spread call options** and prices the entire fleet using the Kirk/Margrabe closed-form approximation. It answers the question:

> *Given current market price levels and volatility, what is the forward-looking optionality value of each BESS asset's arbitrage right over the selected horizon?*

It does **not** simulate actual dispatch. It values the *right* to arbitrage the peak/offpeak spread each day, accounting for roundtrip efficiency and O&M costs.

---

## 2. Market Data Definitions

### 2.1 Peak and Offpeak Hours

| Period | Hours (CST) | 15-min Slot Indices | Rationale |
|--------|-------------|---------------------|-----------|
| **Offpeak** | 08:00 – 16:00 | 32 – 63 | Solar generation peak; prices suppressed (avg ~70 ¥/MWh, Q1/Q2 2026) |
| **Peak** | 00:00 – 08:00 and 17:00 – 24:00 | 0 – 31, 64 – 95 | No solar; morning ramp + evening demand (avg ~250 ¥/MWh) |

**Why solar-based rather than demand-based?**  
Inner Mongolia is a high-renewables grid. Midday solar generation routinely suppresses or inverts RT clearing prices during 08:00–16:00. BESS profitability arises from charging during this solar trough and discharging into the evening demand peak — the opposite of traditional demand-side peak definitions.

The hour definitions are empirically derived from 60-day trailing average RT clearing prices from `hist_mengxi_provincerealtimeclearprice_15min`. They should be reviewed quarterly as the renewable mix evolves.

### 2.2 Forward Prices

`peak_fwd` and `offpeak_fwd` are the **arithmetic mean of daily average RT clearing prices** over the selected vol window (default 60 days), split by the peak/offpeak definition above.

```
peak_fwd    = mean over [today − vol_window, today] of daily-avg price during peak hours
offpeak_fwd = mean over [today − vol_window, today] of daily-avg price during offpeak hours
```

These are **historical averages used as forward proxies**, not true forward curve prices. They are equal for all 8 assets — the provincial RT price is a single-price area at the province level.

### 2.3 Implied Volatilities

Annualised volatilities are estimated from daily log-returns of the same price series:

```
σ_peak    = std(log(P_t / P_{t-1})) × √252  over the vol window
σ_offpeak = std(log(P_t / P_{t-1})) × √252  over the vol window
```

Days with zero or negative prices are excluded before computing log-returns (returns 30% fallback if fewer than 5 valid observations remain).

**Vol window control:** sidebar selectbox — 30 / 60 / 90 days.

---

## 3. Pricing Model: Kirk/Margrabe Spread Call Strip

### 3.1 The BESS as a Spread Call Option

Each day, a BESS asset can earn:

```
Payoff per MWh discharged = max(F_pk − F_off/η − K, 0)
```

where:
- `F_pk` = peak forward price (¥/MWh)
- `F_off` = offpeak forward price (¥/MWh) — the charge cost
- `η` = roundtrip efficiency (default 0.85)
- `K` = effective strike = `om_cost − subsidy_yuan_per_mwh`

The division `F_off/η` inflates the effective buy cost to account for energy lost in the charge/discharge cycle. Combined with the strike, the full effective second forward is:

```
F2_eff = F_off / η + K
```

The option is **in the money (ITM)** when `F_pk > F2_eff` — i.e., when peak prices exceed the efficiency-adjusted offpeak cost plus net O&M.

### 3.2 Kirk/Margrabe Closed Form

The spread call is priced using the **Margrabe exchange option formula** with Kirk's approximation to absorb the strike into the second forward:

```
C(T) = e^{−rT} [F_pk · N(d₁) − F2_eff · N(d₂)]

d₁ = [ln(F_pk / F2_eff) + ½ σ_s² T] / (σ_s √T)
d₂ = d₁ − σ_s √T

σ_s = √(σ_pk² − 2ρ σ_pk σ_off + σ_off²)   (Margrabe spread vol)
```

where `ρ` is the peak/offpeak correlation (sidebar slider, default 0.85).

No external pricing libraries are used — the normal CDF is computed via `math.erfc`.

### 3.3 Strip Construction

The strip is the sum of `N` daily options, each with its own time-to-expiry:

```
Strip value = q_max × Σᵢ₌₁ᴺ C(Tᵢ),   Tᵢ = i / 252 years
```

where:
- `q_max = η × power_mw × duration_h` (MWh dischargeable per day)
- `N` = strip horizon in calendar days (sidebar slider, default 365)
- Time convention: trading-day based (`T_i = i/252`)

**The strip value is the total value of the optionality over the entire horizon** — it is *not* annualised. A 365-day strip at 252 trading days per year effectively prices slightly more than one trading year of daily options.

### 3.4 Subsidy Adjustment

Each asset has a `subsidy_yuan_per_mwh` (度电补贴) that reduces the effective strike:

```
K_eff = om_cost − subsidy_yuan_per_mwh
```

The cockpit prices each asset **twice** — once with no subsidy (market-only) and once with subsidy — and reports both:

- **Market Value**: strip value ignoring subsidy (`K = om_cost`)
- **Subsidy Value**: incremental value from the subsidy (`Total − Market`)
- **Total Value**: full strip value with subsidy reducing the effective strike

Default subsidy rates (source: 资产清单2026.xlsx):

| Asset | Subsidy (¥/MWh) |
|-------|----------------|
| suyou, hangjinqi, siziwangqi, gushanliang, bameng, wulate, wuhai | 350 |
| wulanchabu | 0 |

---

## 4. Table Columns Explained

| Column | Definition |
|--------|-----------|
| **Total Value (¥)** | Full strip value including subsidy over the horizon |
| **Market Value (¥)** | Strip value without subsidy |
| **Subsidy Value (¥)** | `Total − Market`; incremental value from 度电补贴 |
| **Subsidy (¥/MWh)** | Per-MWh subsidy rate for that asset |
| **Per-Day Total (¥)** | `Total Value / N days` — average daily option value |
| **Net Spread Fwd (¥/MWh)** | `F_pk − F_off/η` — raw spread before strike deduction |
| **Moneyness (%)** | `(F_pk − F2_eff) / F2_eff × 100` — how far ITM/OTM the option is |
| **Intrinsic (¥)** | `max(F_pk − F2_eff, 0) × q_max × N` — value if exercised at forward prices today |
| **Time Value (¥)** | `Total Value − Intrinsic` — value from price uncertainty |
| **Delta (¥/¥/MWh)** | Strip value change per +1 ¥/MWh move in peak forward |
| **Vega (¥/vol pt)** | Strip value change per +1% move in peak vol |
| **Theta (¥/day)** | Strip value decay per calendar day (≤ 0 for long option) |

### Moneyness interpretation

| Moneyness | Colour | Meaning |
|-----------|--------|---------|
| > +10% | Green | ITM — peak prices comfortably above efficiency-adjusted offpeak + costs |
| −5% to +10% | Yellow | Near-the-money — small price moves flip the sign |
| < −5% | Red | OTM — asset only earns from tail upside (time value dominates) |

### Intrinsic vs Time Value

- **Intrinsic = 0** is normal when moneyness is negative. The asset still has positive strip value because of time value — even if the *average* spread is negative, individual days with high price spikes generate positive payoffs.
- A BESS with high time value and low intrinsic is a **volatility play**: it earns from price spikes, not from a structural spread.
- A BESS with significant intrinsic value has a **structural arbitrage**: prices reliably favour discharge.

---

## 5. Greeks

All Greeks are computed by **numerical finite differences**:

| Greek | Bump | Interpretation |
|-------|------|----------------|
| Delta | +1 ¥/MWh in F_pk | ¥ gained per ¥/MWh rise in peak forward price |
| Vega | +1 vol point in σ_peak | ¥ gained per 1% increase in peak price volatility |
| Theta | −1 calendar day | ¥ lost per day of time passing (always ≤ 0) |

Theta accelerates as options approach expiry. High vega with low intrinsic indicates the asset's value is primarily driven by volatility — relevant for hedging decisions.

---

## 6. Realization Overlay

The scatter plot (Row 3) overlays **historical realization ratios** from `monitoring.asset_realization_status` against the model strip value:

- **X-axis**: 30-day rolling realization ratio (cleared actual PnL / grid-feasible benchmark PnL)
- **Y-axis**: model strip value (¥)
- **Bubble size**: `q_max` (MWh/day capacity)
- **Bubble colour**: fragility level (green=LOW, orange=MEDIUM, red=HIGH/CRITICAL)

Assets in the **top-right** (high value, high realization) are performing well against both the model and the market. Assets in the **top-left** (high theoretical value, low realization) have unexploited optionality — likely due to grid restrictions or suboptimal dispatch strategy.

---

## 7. Sidebar Controls

| Control | Effect |
|---------|--------|
| **Strip horizon (calendar days)** | Number of daily options priced. 365 = one full calendar year. |
| **Vol window (days)** | Lookback for computing forward prices and vols. Shorter = more reactive. |
| **O&M cost (¥/MWh)** | Adds to the effective strike. Reduces moneyness and strip value. |
| **Peak/offpeak correlation** | Higher correlation reduces spread vol and strip value. |
| **Asset spec overrides** | Override MW, duration, efficiency per asset. Subsidy is fixed from asset spec. |

---

## 8. Limitations and Caveats

1. **Flat vol surface** — no vol smile or term structure. Actual Mengxi price distributions have fat tails; the model understates deep-OTM option value.
2. **Constant correlation** — correlation between peak/offpeak prices is fixed. In practice it varies with weather and grid conditions.
3. **Province-level prices** — all assets share the same RT clearing price. Node-level locational differences (congestion, curtailment) are not modelled.
4. **Historical averages as forwards** — `peak_fwd` and `offpeak_fwd` are trailing averages, not true forward curve prices. Seasonal patterns are ignored.
5. **Daily granularity** — intraday optionality (multiple charge/discharge cycles) is not captured.
6. **No capacity or state-of-charge constraints** — the model assumes full flexibility every day.
7. **Realization ratios are empty until `run_realization_monitor.py` is run** — monitoring tables are populated by `services/monitoring/`.

---

## 9. Data Sources

| Data | Table | Update frequency |
|------|-------|-----------------|
| RT clearing prices | `public.hist_mengxi_provincerealtimeclearprice_15min` | Daily via mengxi ingestion |
| Realization status | `monitoring.asset_realization_status` | Daily via `run_realization_monitor.py` |
| Fragility status | `monitoring.asset_fragility_status` | Daily via `run_fragility_monitor.py` |
| Asset specs | `_ASSET_SPECS` in `cockpit_page.py` | Manual update from 资产清单2026.xlsx |
