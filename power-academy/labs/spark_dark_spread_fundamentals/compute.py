"""Worked example for spark_dark_spread_fundamentals.

Spreads from power price, fuel price, efficiency, carbon price.
Units: EUR/MWh throughout. Deterministic; no external data.
"""

EF_GAS = 0.202   # tCO2 per MWh_th, natural gas
EF_COAL = 0.341  # tCO2 per MWh_th, coal


def heat_rate(eta: float) -> float:
    """Heat rate (MWh_fuel/MWh_e) from electrical efficiency."""
    return 1.0 / eta


def hr_btu_per_kwh(eta: float) -> float:
    """US-convention heat rate (BTU/kWh) from electrical efficiency."""
    return 3412.0 / eta


def spark_spread(power: float, gas: float, eta: float) -> float:
    return power - heat_rate(eta) * gas


def clean_spark_spread(power: float, gas: float, eua: float, eta: float) -> float:
    return power - heat_rate(eta) * (gas + eua * EF_GAS)


def dark_spread(power: float, coal: float, eta: float) -> float:
    return power - heat_rate(eta) * coal


def clean_dark_spread(power: float, coal: float, eua: float, eta: float) -> float:
    return power - heat_rate(eta) * (coal + eua * EF_COAL)


def fuel_switch_carbon_price(gas, coal, eta_g, eta_c) -> float:
    """Carbon price (EUR/t) at which clean spark == clean dark."""
    hr_g, hr_c = heat_rate(eta_g), heat_rate(eta_c)
    return (hr_c * coal - hr_g * gas) / (hr_g * EF_GAS - hr_c * EF_COAL)


if __name__ == "__main__":
    P, G, C, E, ETA_G, ETA_C = 80.0, 30.0, 15.0, 80.0, 0.50, 0.38
    print(f"HR_g = {heat_rate(ETA_G):.3f}  HR_c = {heat_rate(ETA_C):.3f}")
    print(f"Spark       = {spark_spread(P, G, ETA_G):+.2f}")
    print(f"Clean spark = {clean_spark_spread(P, G, E, ETA_G):+.2f}")
    print(f"Dark        = {dark_spread(P, C, ETA_C):+.2f}")
    print(f"Clean dark  = {clean_dark_spread(P, C, E, ETA_C):+.2f}")
    print(f"Fuel-switch carbon price = {fuel_switch_carbon_price(G, C, ETA_G, ETA_C):.2f} EUR/t")
