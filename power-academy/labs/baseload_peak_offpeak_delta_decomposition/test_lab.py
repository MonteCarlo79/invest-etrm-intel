from . import compute as c


def _days():
    return c.simulate()


def test_decomposition_identity():
    days = _days()
    for k in (c.K_MUSTRUN, c.K_PEAKER):
        assert abs(c.fd_delta(days, k, "base")
                   - (c.fd_delta(days, k, "peak") + c.fd_delta(days, k, "offpeak"))) < 1e-9


def test_must_run_deltas_are_block_hours():
    days = _days()
    assert abs(c.fd_delta(days, c.K_MUSTRUN, "peak") - 12.0) < 0.05
    assert abs(c.production_delta(days, c.K_MUSTRUN, "peak") - 12.0) < 0.05
    # no boundary effect for a must-run plant
    assert abs(c.fd_delta(days, c.K_MUSTRUN, "peak")
               - c.production_delta(days, c.K_MUSTRUN, "peak")) < 0.1


def test_peaker_deltas_differ_at_boundary():
    days = _days()
    prod = c.production_delta(days, c.K_PEAKER, "peak")
    fd = c.fd_delta(days, c.K_PEAKER, "peak")
    assert abs(prod - 5.40) < 0.2
    assert abs(fd - prod) > 0.005 * prod          # measurable boundary effect
    assert c.production_delta(days, c.K_PEAKER, "offpeak") < 0.05


def test_aom_caveat_naive_hedge_leaves_more_par():
    days = _days()
    naive = c.production_delta(days, c.K_PEAKER, "peak")
    full = c.fd_delta(days, c.K_PEAKER, "peak")
    par_naive = c.residual_par(days, c.K_PEAKER, "peak", naive)
    par_fd = c.residual_par(days, c.K_PEAKER, "peak", full)
    assert par_naive > par_fd
