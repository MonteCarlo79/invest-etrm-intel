# test_dp_dispatch.py
import platform

import numpy as np
import pytest

from libs.option_value.contracts import BESSParams
from libs.option_value.dp_dispatch import dp_dispatch_paths, dp_dispatch_day
from services.bess_map.optimisation_engine import optimise_window


@pytest.fixture(autouse=True)
def _solver_backend():
    """arm64 Mac without Rosetta: bundled CBC is Intel-only; HiGHS solves the
    identical LP — test-side backend substitution only, production path untouched."""
    import pulp

    def _cbc_broken() -> bool:
        if platform.system() == "Darwin" and platform.machine() == "arm64":
            return True
        try:
            s = pulp.PULP_CBC_CMD(msg=False, timeLimit=5)
            if not s.available():
                return True
            lp = pulp.LpProblem("probe", pulp.LpMaximize)
            x = pulp.LpVariable("x", lowBound=0, upBound=1)
            lp += x
            lp += x <= 2
            lp.solve(s)
            return abs(pulp.value(x) - 1.0) > 1e-6
        except Exception:
            return True

    if _cbc_broken():
        pulp.PULP_CBC_CMD = lambda msg=False, timeLimit=None, **kw: pulp.HiGHS(
            msg=msg, timeLimit=timeLimit
        )
    yield


P = BESSParams(power_mw=1.0, duration_h=2.0, rte=0.85)

def test_crafted_day_arbitrages():
    day = np.array([10.0]*12 + [500.0]*12)
    rev, dis = dp_dispatch_day(day, P)
    assert rev > 0 and dis > 0
    # charge must happen in the cheap half: revenue bounded by 2 MWh grid delivery
    assert dis <= 2.0 + 1e-9
    # Regression tripwire for taper-consistency semantics: DP matches the LP
    # optimum exactly on this day (1000*sqrt(0.85) - 20/sqrt(0.85)); a broken
    # backward/roll-out clip symmetry would silently pass the 2% median gate.
    assert abs(rev - 900.2613999474231) < 1e-6
    assert abs(rev - optimise_window(day, 1.0, 2.0, 0.85).profit) < 1e-6

def test_dp_lp_gate_synthetic():
    rng = np.random.default_rng(3)
    gaps = []
    for _ in range(100):
        shape = 200 + 150*np.sin(2*np.pi*(np.arange(24)-6)/24)
        day = shape + rng.normal(0, 40, 24)
        lp = optimise_window(day, power_mw=1.0, duration_h=2.0, roundtrip_eff=0.85)
        dp_rev, _ = dp_dispatch_day(day, P)
        assert dp_rev <= lp.profit * 1.005 + 1e-6      # DP discretization cannot over-earn
        gaps.append((lp.profit - dp_rev) / max(lp.profit, 1e-9))
    assert float(np.median(gaps)) < 0.02                # THE GATE (spec §5.3)

def test_vectorized_matches_single():
    rng = np.random.default_rng(5)
    days = 200 + 100*np.sin(2*np.pi*np.arange(24)/24) + rng.normal(0, 30, (7, 24))
    batch = dp_dispatch_paths(days, P)
    singles = [dp_dispatch_day(d, P)[0] for d in days]
    assert np.allclose(batch.revenue_per_path, singles, rtol=1e-6)
