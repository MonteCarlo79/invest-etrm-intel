"""Worked example for heat_rate_and_plant_parameters.

Plant parameter sheet: MEL/SEL, two-point heat-rate curve, start-cost tiers.
Units: MW, EUR, hours. Deterministic; no external data.
"""

MEL = 500.0        # MW, maximum export limit
SEL = 200.0        # MW, stable export limit (minimum stable load)
HR_FULL = 2.0      # MWh_th/MWh_e at MEL
HR_MIN = 2.4       # MWh_th/MWh_e at SEL
T1, T2 = 6.0, 48.0  # hot/warm/cold boundaries (h)
C_HOT, C_WARM, C_COLD = 30_000.0, 60_000.0, 120_000.0  # EUR
MIN_UP = 8.0       # h
GAS = 30.0         # EUR/MWh_th


def heat_rate_at(q: float) -> float:
    """Two-point linear heat-rate curve; q must be within [SEL, MEL]."""
    if not SEL <= q <= MEL:
        raise ValueError(f"q={q} outside [{SEL}, {MEL}] (or 0 for offline)")
    return HR_FULL + (HR_MIN - HR_FULL) * (MEL - q) / (MEL - SEL)


def start_cost(offline_h: float) -> float:
    if offline_h < 0:
        raise ValueError("offline_h must be >= 0")
    if offline_h < T1:
        return C_HOT
    if offline_h < T2:
        return C_WARM
    return C_COLD


def marginal_cost(q: float, gas: float = GAS) -> float:
    """EUR/MWh at output q (carbon ignored in this worked example)."""
    return heat_rate_at(q) * gas


def start_amortised_per_mwh(offline_h: float, run_h: float = MIN_UP,
                            output_mw: float = MEL) -> float:
    """Start cost spread over the committed run, EUR/MWh."""
    return start_cost(offline_h) / (run_h * output_mw)


if __name__ == "__main__":
    print(f"MC at full load (500 MW): {marginal_cost(500):.1f} EUR/MWh")
    print(f"MC at 60% load (300 MW):  {marginal_cost(300):.1f} EUR/MWh "
          f"(HR={heat_rate_at(300):.3f})")
    print(f"Start after 20h offline:  {start_cost(20)/1000:.0f} kEUR (warm)")
    print(f"Amortised over {MIN_UP:.0f}h at MEL: {start_amortised_per_mwh(20):.1f} EUR/MWh")
