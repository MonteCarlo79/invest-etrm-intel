from . import compute as c
from baseload_peak_offpeak_delta_decomposition import compute as dec


def _setup():
    days = dec.simulate(level_sd=8.0, hour_sd=1.5)
    lad = c.ladder(days, dec.K_PEAKER)
    vols = c.volumes(lad)
    return days, lad, vols


def test_volumes_match_ladder():
    _, lad, vols = _setup()
    assert abs(vols["base"] - c.MEL * c.N_DAYS * lad["base"]) < 1e-6
    assert abs(vols["inc_peak"] - c.MEL * c.N_DAYS * (lad["peak"] - lad["prod_peak"])) < 1e-6
    assert abs(vols["base"] - 12_160) < 100


def test_no_double_count_of_peak_hours():
    _, lad, vols = _setup()
    # the peak leg carries only the incremental part, not the full peak delta
    full_peak_vol = c.MEL * c.N_DAYS * lad["peak"]
    assert abs(vols["inc_peak"]) < 0.15 * full_peak_vol


def test_residual_par_reduced_when_block_risk_dominates():
    days, _, vols = _setup()
    par_u, _ = c.residual_par(days, dec.K_PEAKER, {"base": 0.0, "inc_peak": 0.0})
    par_h, _ = c.residual_par(days, dec.K_PEAKER, vols)
    assert par_h < 0.5 * par_u


def test_block_hedge_cannot_fix_idiosyncratic_noise():
    # with hourly noise dominating, the same mechanics help far less
    days = dec.simulate(level_sd=0.0, hour_sd=6.0)
    lad = c.ladder(days, dec.K_PEAKER)
    vols = c.volumes(lad)
    par_u, _ = c.residual_par(days, dec.K_PEAKER, {"base": 0.0, "inc_peak": 0.0})
    par_h, _ = c.residual_par(days, dec.K_PEAKER, vols)
    assert par_h > 0.8 * par_u          # basis risk: block forward can't reach it
