"""dp_dispatch_modes: charge/discharge pattern for the fast (DP-fixed-mode LP)
path. Pure numpy — no solver needed. Added 2026-11-24 after the subsidised
MILP took 50-120 s per node (nodal-pf-daily: 40-55 h per trading day)."""
import numpy as np

from services.bess_map.optimisation_engine import dp_dispatch_modes

ETA = 0.85 ** 0.5
KW = dict(power_mw=100.0, e_cap=200.0, eta_c=ETA, eta_d=ETA, dt=0.25)


def _prices():
    p = np.full(96, 400.0)
    p[8:24] = 100.0      # cheap night block
    p[72:88] = 900.0     # evening peak
    return p


def test_charges_cheap_discharges_peak():
    m = dp_dispatch_modes(_prices(), **KW)
    assert set(np.where(m == -1)[0]) <= set(range(8, 24)) and (m == -1).sum() >= 8
    assert set(np.where(m == 1)[0]) <= set(range(72, 88)) and (m == 1).sum() >= 8


def test_subsidy_never_makes_a_slot_do_both_and_adds_cycles():
    p = _prices()
    base = dp_dispatch_modes(p, **KW)
    sub = dp_dispatch_modes(p, compensation_yuan_per_mwh=350.0, **KW)
    assert set(np.unique(sub)) <= {-1, 0, 1}            # one mode per slot by construction
    assert (sub == 1).sum() > (base == 1).sum()         # subsidy makes extra cycling worthwhile


def test_zone_masks_respected():
    p = _prices()
    m = dp_dispatch_modes(p, compensation_yuan_per_mwh=280.0,
                          no_charge_slots=set(range(8, 24)), no_discharge_slots=set(range(72, 80)), **KW)
    assert not (m[8:24] == -1).any() and not (m[72:80] == 1).any()


def test_flat_prices_without_subsidy_stay_idle():
    assert not dp_dispatch_modes(np.full(96, 300.0), **KW).any()
