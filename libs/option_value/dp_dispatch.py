"""Vectorized dynamic-programming BESS dispatch — option-value engine core.

Pure computation: no DB, no file I/O, no streamlit. Prices ¥/MWh, dt = 1.0 h.

Method (pinned, spec §5.3)
--------------------------
SOC grid: ``0 .. power_mw*duration_h`` with step ``soc_grid_step_frac * E``.
Action set (nominal grid power): {idle, charge P/2, charge P, discharge P/2,
discharge P}. Physics identical to
``services.bess_map.optimisation_engine.optimise_window``:

    soc' = soc + ch*eta_c*dt - dis*dt/eta_d,   eta_c = eta_d = sqrt(rte)

Boundary conditions: SOC[0] = 0, terminal SOC free (V_T = 0 everywhere).

Backward value iteration is vectorized over (n_paths, n_soc) per timestep, with
linear interpolation of V_next at the (generally off-grid) landing SOC. Only the
argmax action tensor ``policy[t, soc_idx, path]`` (int8) is retained — the full
value tensor is discarded to bound memory. A forward roll-out from SOC=0 then
reconstructs realised revenue (¥) and grid-delivered discharge (MWh).

Taper-consistent actions (the LP-matching ingredient)
-----------------------------------------------------
The LP oracle uses continuous partial power only at cycle boundaries
(bang-bang interior + tapers that exactly fill/empty the tank). The DP
reproduces this by evaluating each of the 5 nominal actions with *boundary
tapering*: the landing SOC is clipped to [0, E] and the executed
charge/discharge power is the partial power implied by the actual SOC delta,

    ch_exec = (soc_land - soc) / (eta_c * dt),   dis_exec = (soc - soc_land) * eta_d / dt

and the immediate reward is priced on the *executed* energy. The forward
roll-out applies the same clip, so policy and execution are consistent.
Without taper-consistency the discrete action set caps revenue well below the
LP optimum (measured ~14% median gap at any grid fineness: the action set, not
the state grid, is the binding constraint); with it the gap comes almost
entirely from mid-cycle P/2-vs-partial mismatches. Because every executed
dispatch is physically feasible, DP revenue is always <= the LP optimum.

Grid choice
-----------
Validated against the production LP oracle on the 100 seeded synthetic days of
tests/test_dp_dispatch.py::test_dp_lp_gate_synthetic (sinusoidal shape
200 + 150*sin(2*pi*(h-6)/24), noise sigma = 40). The default grid
``soc_grid_step_frac = 0.1`` (11 SOC points for a 1 MW / 2 h unit) passes the
< 2% median revenue gap gate — measured median gap 1.09% (mean 1.36%,
p90 2.66%, max 4.42%, zero over-earn days). Finer grids do not help: with
taper-consistent actions the residual gap is dominated by the P/2 interior
granularity, not the SOC grid (frac = 0.05 on the same 100 days: median 1.08%,
indistinguishable from 0.1).
"""

from __future__ import annotations

import numpy as np

from libs.option_value.contracts import BESSParams, DispatchResult

# Discrete action set, per-unit of rated power (charge / discharge MW tables).
_CH_PU = np.array([0.0, 0.5, 1.0, 0.0, 0.0])   # idle, ch P/2, ch P, dis P/2, dis P
_DIS_PU = np.array([0.0, 0.0, 0.0, 0.5, 1.0])
_DT = 1.0  # interval duration [h] — pinned by the method


def _backward_policy(paths: np.ndarray, params: BESSParams):
    """Backward value iteration. Returns (policy, soc_grid).

    policy: int8 array (T, n_soc, n_paths) — argmax action id per state.
    Actions are taper-consistent: landing SOC clipped to [0, E], executed
    power = the partial power implied by the actual SOC delta, reward priced
    on executed energy.
    """
    n_paths, T = paths.shape
    power = float(params.power_mw)
    e_cap = power * float(params.duration_h)
    eta = float(np.sqrt(params.rte))  # eta_c = eta_d

    step = float(params.soc_grid_step_frac) * e_cap
    n_soc = int(round(e_cap / step)) + 1
    soc_grid = np.linspace(0.0, e_cap, n_soc)
    step = soc_grid[1] - soc_grid[0]  # exact grid spacing

    ch = _CH_PU * power   # (A,) nominal charge power MW per action
    dis = _DIS_PU * power  # (A,) nominal discharge power MW per action

    # Landing SOC for every (grid point, action) — path-independent physics.
    # Taper: clip to [0, E] is the feasibility rule; executed power follows
    # from the actual SOC delta (partial power at the boundaries).
    soc_land = np.clip(
        soc_grid[:, None] + ch[None, :] * eta * _DT - dis[None, :] * _DT / eta,
        0.0, e_cap,
    )                                                     # (S, A)
    ch_exec = np.maximum(soc_land - soc_grid[:, None], 0.0) / (eta * _DT)
    dis_exec = np.maximum(soc_grid[:, None] - soc_land, 0.0) * eta / _DT

    # Linear-interpolation weights for V_next at the landing SOC.
    fidx = soc_land / step                                # (S, A)
    i0 = np.minimum(np.floor(fidx).astype(np.int64), n_soc - 2)
    w = fidx - i0                                         # in [0, 1]

    V = np.zeros((n_paths, n_soc))  # terminal SOC free: V_T = 0
    policy = np.zeros((T, n_soc, n_paths), dtype=np.int8)

    for t in range(T - 1, -1, -1):
        price = paths[:, t]                                  # (n_paths,)
        immediate = price[:, None, None] * (dis_exec - ch_exec)[None, :, :] * _DT
        v_next = V[:, i0] * (1.0 - w) + V[:, i0 + 1] * w     # (n_paths, S, A)
        q = immediate + v_next
        best = np.argmax(q, axis=2)                          # (n_paths, S)
        policy[t] = best.T.astype(np.int8)
        V = np.take_along_axis(q, best[:, :, None], axis=2)[:, :, 0]

    return policy, soc_grid


def _rollout(paths: np.ndarray, params: BESSParams, policy: np.ndarray,
             soc_grid: np.ndarray):
    """Forward roll-out from SOC=0 following the tabulated policy.

    Nearest grid-point policy lookup; the power clip reproduces the backward
    pass's boundary tapering exactly. Returns (revenue ¥ per path,
    grid-delivered discharge MWh per path).
    """
    n_paths, T = paths.shape
    power = float(params.power_mw)
    e_cap = power * float(params.duration_h)
    eta = float(np.sqrt(params.rte))
    step = soc_grid[1] - soc_grid[0]
    n_soc = len(soc_grid)

    ch_tab = _CH_PU * power
    dis_tab = _DIS_PU * power

    soc = np.zeros(n_paths)
    revenue = np.zeros(n_paths)
    discharge = np.zeros(n_paths)
    rows = np.arange(n_paths)

    for t in range(T):
        sidx = np.clip(np.rint(soc / step).astype(np.int64), 0, n_soc - 1)
        act = policy[t, sidx, rows]
        ch = ch_tab[act]
        dis = dis_tab[act]
        # Boundary taper (identical to the backward pass): clip power so the
        # landing SOC stays within [0, E]; executed power follows the delta.
        ch = np.minimum(ch, (e_cap - soc) / (eta * _DT))
        dis = np.minimum(dis, soc * eta / _DT)
        revenue += paths[:, t] * (dis - ch) * _DT
        discharge += dis * _DT
        soc = np.clip(soc + ch * eta * _DT - dis * _DT / eta, 0.0, e_cap)

    return revenue, discharge


def dp_dispatch_paths(paths: np.ndarray, params: BESSParams) -> DispatchResult:
    """Dispatch a fleet of price paths (n_paths, T) ¥/MWh.

    Returns total revenue ¥ and grid-delivered discharge MWh per path, for the
    ``power_mw`` unit. SOC[0] = 0, terminal SOC free.
    """
    paths = np.asarray(paths, dtype=float)
    policy, soc_grid = _backward_policy(paths, params)
    revenue, discharge = _rollout(paths, params, policy, soc_grid)
    return DispatchResult(revenue_per_path=revenue,
                          discharge_mwh_per_path=discharge)


def dp_dispatch_day(prices_24: np.ndarray, params: BESSParams) -> tuple[float, float]:
    """Single-day convenience wrapper: returns (revenue ¥, discharge MWh)."""
    res = dp_dispatch_paths(np.asarray(prices_24, dtype=float)[None, :], params)
    return float(res.revenue_per_path[0]), float(res.discharge_mwh_per_path[0])
