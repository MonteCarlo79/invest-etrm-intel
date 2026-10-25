"""Value-stack decomposition, policy levers, annuitization, Greeks.

V_total = intrinsic + extrinsic + policy_net, all in ¥/kW/yr (per kW of
rated power). intrinsic values the deterministic expected curve; extrinsic
= mean simulated path revenue − intrinsic (the Jensen term: BESS >= 0 by
convexity of the dispatch value, asserted; wind/solar typically negative
through the price x volume copula, reported with its sign and NEVER
floored); policy_net sums the applicable PolicyLevers valued at the MEAN
simulated path volume (spec §11.8).

Horizon discipline (load-bearing): extrinsic = mean(path revenue) −
intrinsic is meaningful only when paths and intrinsic cover the same
hours. compute_greeks therefore simulates with n_days = curve.n_days so
paths cover exactly len(curve.hourly). compute_value_stack accepts paths
on any day-aligned prefix of the curve and values the intrinsic on the
PATHS' horizon (curve truncated to the first T hours), so the extrinsic
is same-horizon by construction.

Greeks use common random numbers: base and bumped re-values share
PCASimConfig.seed, and renewable CF copula noise shares the fixed
_CF_COPULA_SEED, so a Greek isolates the bump rather than resampling
noise.

Pure computation only: no DB, no file I/O, no streamlit.
Prices ¥/MWh in, values ¥/kW/yr out.
"""

from __future__ import annotations

import dataclasses
from datetime import timedelta

import numpy as np
import pandas as pd

from .contracts import PCASimConfig, PCAv2Params, PolicyLevers, ValueStack
from .curves import build_expected_curve
from .pca_simulator import fit_pca_v2, simulate_pca_v2

_CF_COPULA_SEED = 0  # fixes renewable copula noise so Greek re-values share CRN

_APPLICABLE: dict[str, tuple[str, ...]] = {
    "bess": ("capacity_price", "cap_comp", "fr", "discharge_comp", "sysopfee"),
    "wind": ("mechanism_premium", "green_cert", "sysopfee"),
    "solar": ("mechanism_premium", "green_cert", "sysopfee"),
}


def apply_policy(
    levers: PolicyLevers,
    asset_type: str,
    volume_mwh_per_kw: float,
    rte: float,
) -> tuple[float, dict]:
    """Value the policy levers at ``volume_mwh_per_kw``.

    All quantities are per kW of rated power, per year. ``volume_mwh_per_kw``
    is the mean simulated path volume: grid-delivered discharge for BESS,
    generation for wind/solar. Pinned formulas:

    - capacity_price / cap_comp / fr: pass through (already ¥/kW/yr)
    - discharge_comp = rate_¥kWh × volume × 1000
    - sysopfee = −fee_¥kWh × (volume/rte) × 1000 (bess: fee on charge energy)
                 −fee_¥kWh × volume × 1000       (wind/solar: on generation)
    - mechanism_premium = premium_¥MWh × volume
    - green_cert = rate_¥MWh × volume

    Levers not applicable to ``asset_type`` are zeroed and named in
    ``comp["not_applicable"]``. Returns (net ¥/kW/yr, components dict) —
    net is the sum of every numeric component.
    """
    if asset_type not in _APPLICABLE:
        raise ValueError(f"no applicable-lever map for asset_type={asset_type!r}")
    v = float(volume_mwh_per_kw)
    comp: dict = {
        "capacity_price": float(levers.capacity_price_yuan_kw_yr),
        "cap_comp": float(levers.cap_comp_yuan_kw_yr),
        "fr": float(levers.fr_yuan_kw_yr),
        "discharge_comp": float(levers.discharge_comp_yuan_kwh) * v * 1000.0,
        "sysopfee": (
            -float(levers.sysopfee_yuan_kwh)
            * (v / rte if asset_type == "bess" else v)
            * 1000.0
        ),
        "mechanism_premium": float(levers.mechanism_premium_yuan_mwh) * v,
        "green_cert": float(levers.green_cert_yuan_mwh) * v,
    }
    not_applicable = [k for k in comp if k not in _APPLICABLE[asset_type]]
    for k in not_applicable:
        comp[k] = 0.0
    comp["not_applicable"] = not_applicable
    net = sum(x for x in comp.values() if isinstance(x, (int, float)))
    return float(net), comp


def _unit_mw(model) -> float:
    """Rated MW of the model's unit: power_mw (BESS) or capacity_mw."""
    p = getattr(model, "params", None)
    return float(getattr(p, "power_mw", getattr(p, "capacity_mw", 1.0)))


def compute_value_stack(
    model,
    curve,
    paths: np.ndarray,
    levers: PolicyLevers,
    capture_rate: float | None,
) -> ValueStack:
    """Decompose simulated value into intrinsic / extrinsic / policy.

    ``paths`` must be a day-aligned prefix of the curve: T hours with T a
    multiple of 24 and T <= len(curve.hourly). The intrinsic is valued on
    the paths' horizon (curve truncated to the first T hours) so that
    extrinsic = mean(path revenue) − intrinsic is same-horizon by
    construction. Simulate with ``n_days = curve.n_days`` to value the full
    curve. The model runs at ``power_mw`` / ``capacity_mw`` MW scale;
    revenue and volume are divided by ``unit_mw × 1000`` for the per-kW
    basis before policy is applied. Policy levers are valued at the MEAN
    path volume.
    """
    paths = np.asarray(paths, dtype=float)
    n_hours = curve.hourly.shape[0]
    if (
        paths.ndim != 2
        or paths.shape[1] == 0
        or paths.shape[1] > n_hours
        or paths.shape[1] % 24 != 0
    ):
        raise ValueError(
            f"paths must be (n_paths, T) with T a multiple of 24 and "
            f"0 < T <= {n_hours} (a day-aligned prefix of the curve); "
            f"got shape {paths.shape}"
        )
    T = paths.shape[1]
    horizon_curve = (
        curve
        if T == n_hours
        else dataclasses.replace(curve, hourly=curve.hourly[:T], n_days=T // 24)
    )

    unit_kw = _unit_mw(model) * 1000.0
    rte = float(getattr(model.params, "rte", 1.0))

    int_rev, _ = model.intrinsic(horizon_curve)
    intrinsic = int_rev / unit_kw

    rev_paths, vol_paths = model.value_paths(
        paths, curve=horizon_curve, seed=_CF_COPULA_SEED
    )
    rev_kw = np.asarray(rev_paths, dtype=float) / unit_kw
    vol_kw_mean = float(np.mean(vol_paths)) / unit_kw

    total_energy = float(np.mean(rev_kw))
    extrinsic = total_energy - intrinsic
    if model.asset_type == "bess":
        assert extrinsic >= -1e-6, (
            f"BESS extrinsic negative ({extrinsic:.6g} ¥/kW): the dispatch "
            f"value is convex in price, so the simulated mean must not "
            f"undershoot the intrinsic"
        )

    policy_net, _ = apply_policy(levers, model.asset_type, vol_kw_mean, rte)
    total = intrinsic + extrinsic + policy_net
    capture_adj = (
        capture_rate * (intrinsic + extrinsic) + policy_net
        if capture_rate is not None
        else None
    )
    p10, p90 = np.percentile(rev_kw, [10.0, 90.0]) - intrinsic

    return ValueStack(
        intrinsic_yuan_kw=float(intrinsic),
        extrinsic_yuan_kw=float(extrinsic),
        policy_net_yuan_kw=float(policy_net),
        total_yuan_kw=float(total),
        capture_adj_yuan_kw=capture_adj,
        extrinsic_p10=float(p10),
        extrinsic_p90=float(p90),
    )


def annuitize(annual_yuan_kw: float, hurdle: float = 0.08, life_years: int = 15) -> float:
    """Capital recovery: level ¥/kW/yr ↔ present value per kW.

    ``annual × (1 − (1+hurdle)^−life) / hurdle``; hurdle == 0 falls back to
    ``annual × life_years``.
    """
    if hurdle == 0:
        return annual_yuan_kw * life_years
    return annual_yuan_kw * (1.0 - (1.0 + hurdle) ** (-life_years)) / hurdle


def compute_greeks(
    model,
    curve,
    sim_params: PCAv2Params,
    cfg: PCASimConfig,
    levers: PolicyLevers,
    capture_rate: float | None,
    prices: pd.Series,
) -> dict:
    """Finite-difference Greeks of V_total (¥/kW/yr), common random numbers.

    - delta: curve bumped +10 ¥/MWh — month_hour_mean AND hourly, and
      floor/cap with them so the simulator's clip bounds shift with the
      price level instead of truncating the parallel shift — re-simulated
      with the SAME fitted params and seed (re-fit nothing); minus base.
    - vega: ``sim_params.inno_scale × 1.1``, same seed; minus base.
    - theta: curve rebuilt from ``prices`` with window_end − 30 days, full
      refit (fit_pca_v2) and re-simulation with the same seed; minus base.
      ``prices`` is the raw hourly series the curve was built from — the
      refit is impossible without it, so it is a required argument even
      though delta/vega do not use it.
    """
    base_paths = simulate_pca_v2(sim_params, curve, curve.n_days, cfg)
    base = compute_value_stack(
        model, curve, base_paths, levers, capture_rate
    ).total_yuan_kw

    curve_d = dataclasses.replace(
        curve,
        month_hour_mean=curve.month_hour_mean + 10.0,
        hourly=curve.hourly + 10.0,
        floor=curve.floor + 10.0,
        cap=curve.cap + 10.0,
    )
    paths_d = simulate_pca_v2(sim_params, curve_d, curve.n_days, cfg)
    delta = (
        compute_value_stack(model, curve_d, paths_d, levers, capture_rate).total_yuan_kw
        - base
    )

    params_v = dataclasses.replace(sim_params, inno_scale=sim_params.inno_scale * 1.1)
    paths_v = simulate_pca_v2(params_v, curve, curve.n_days, cfg)
    vega = (
        compute_value_stack(model, curve, paths_v, levers, capture_rate).total_yuan_kw
        - base
    )

    curve_t = build_expected_curve(prices, curve.window_end - timedelta(days=30))
    params_t = fit_pca_v2(prices, curve_t, cfg)
    paths_t = simulate_pca_v2(params_t, curve_t, curve_t.n_days, cfg)
    theta = (
        compute_value_stack(model, curve_t, paths_t, levers, capture_rate).total_yuan_kw
        - base
    )

    return {"delta": float(delta), "vega": float(vega), "theta": float(theta)}
