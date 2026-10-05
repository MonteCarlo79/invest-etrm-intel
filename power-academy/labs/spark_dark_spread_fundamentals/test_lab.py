import compute as c

P, G, C, E, ETA_G, ETA_C = 80.0, 30.0, 15.0, 80.0, 0.50, 0.38


def test_heat_rate_conversions():
    assert c.heat_rate(0.50) == 2.0
    assert abs(c.heat_rate(ETA_C) - 2.6315789) < 1e-6
    assert abs(c.hr_btu_per_kwh(0.50) - 6824.0) < 1e-6
    assert abs(c.hr_btu_per_kwh(0.55) - 6203.6) < 0.1


def test_worked_example_spreads():
    assert round(c.spark_spread(P, G, ETA_G), 2) == 20.00
    assert round(c.clean_spark_spread(P, G, E, ETA_G), 2) == -12.32
    assert round(c.dark_spread(P, C, ETA_C), 2) == 40.53
    assert round(c.clean_dark_spread(P, C, E, ETA_C), 2) == -31.26


def test_signs_and_fuel_switch():
    assert c.spark_spread(P, G, ETA_G) > 0
    assert c.clean_spark_spread(P, G, E, ETA_G) < 0
    # at E=0 clean spreads reduce to plain spreads
    assert c.clean_spark_spread(P, G, 0.0, ETA_G) == c.spark_spread(P, G, ETA_G)
    # rising carbon favours gas over coal: switch price is positive and finite
    e_switch = c.fuel_switch_carbon_price(G, C, ETA_G, ETA_C)
    assert e_switch > 0
    assert c.clean_dark_spread(P, C, e_switch, ETA_C) == \
        c.clean_spark_spread(P, G, e_switch, ETA_G)
