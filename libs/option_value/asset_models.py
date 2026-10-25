"""Asset-model registry for the option-value pricing engine.

Each asset model wraps the Task 1-4 primitives behind a uniform two-call
interface: ``intrinsic(curve)`` values the deterministic expected curve,
``value_paths(paths, curve, seed)`` values a fleet of simulated price paths.
Both return raw ¥ / MWh totals for the unit defined by the model's params
(``power_mw`` for BESS, ``capacity_mw`` for wind/solar) — per-kW scaling
happens in value_stack.

BESS is long volatility (DP dispatch monetises price dispersion around the
curve); wind/solar are short it through the price x volume copula (negative
extrinsic reported with its sign, never floored). Coal is a phase-2 stub.

Pure computation only: no DB, no file I/O, no streamlit.
Prices are ¥/MWh internally everywhere.
"""

from __future__ import annotations

import dataclasses

import numpy as np

from .contracts import BESSParams, ExpectedCurve, RenewableParams
from .dp_dispatch import dp_dispatch_paths
from .renewable_models import (
    renewable_intrinsic,
    renewable_revenue_paths,
    simulate_cf_paths,
)

COAL_STUB_MSG = "coal dark-spread DP: phase 2, spec §5 asset plugin"

_BESS_LEVERS = ["capacity_price", "cap_comp", "fr", "discharge_comp", "sysopfee"]
_RENEWABLE_LEVERS = ["mechanism_premium", "green_cert", "sysopfee"]


class BESSDispatchOption:
    """Battery storage as a dispatch option on the price curve."""

    asset_type = "bess"
    applicable_levers = _BESS_LEVERS

    def __init__(self, params: BESSParams):
        self.params = params

    def intrinsic(self, curve: ExpectedCurve) -> tuple[float, float]:
        """DP dispatch of the expected curve as one deterministic path.

        Returns (revenue ¥, grid-delivered discharge MWh) for the
        ``power_mw`` unit. An empty curve (insufficient history) yields
        (0.0, 0.0).
        """
        res = dp_dispatch_paths(curve.hourly[None, :], self.params)
        return float(res.revenue_per_path[0]), float(res.discharge_mwh_per_path[0])

    def value_paths(
        self, paths: np.ndarray, curve: ExpectedCurve | None = None, seed: int = 0
    ) -> tuple[np.ndarray, np.ndarray]:
        """DP dispatch per path: (revenue ¥, discharge MWh) per path for the
        ``power_mw`` unit. ``curve``/``seed`` unused — BESS has no copula;
        they exist for interface parity with the renewable options."""
        res = dp_dispatch_paths(np.asarray(paths, dtype=float), self.params)
        return res.revenue_per_path, res.discharge_mwh_per_path


class _RenewableRevenueOption:
    """Shared wind/solar implementation: price x CF copula revenue."""

    applicable_levers = _RENEWABLE_LEVERS

    def __init__(self, params: RenewableParams):
        self.params = params

    def intrinsic(self, curve: ExpectedCurve) -> tuple[float, float]:
        """Deterministic curve revenue: (¥, generation MWh) for the
        ``capacity_mw`` unit."""
        return renewable_intrinsic(curve, self.params)

    def value_paths(
        self, paths: np.ndarray, curve: ExpectedCurve | None = None, seed: int = 0
    ) -> tuple[np.ndarray, np.ndarray]:
        """Copula CF paths valued against the price paths:
        (revenue ¥, generation MWh) per path for the ``capacity_mw`` unit.
        The curve is required — its calendar and month-hour means drive the
        CF copula. ``seed`` fixes the copula noise so Greek re-values share
        common random numbers."""
        if curve is None:
            raise ValueError(
                "renewable value_paths requires the ExpectedCurve "
                "(calendar + month-hour means drive the CF copula)"
            )
        cf = simulate_cf_paths(
            np.asarray(paths, dtype=float), curve, self.params, seed=seed
        )
        return renewable_revenue_paths(paths, cf, self.params)


class WindRevenueOption(_RenewableRevenueOption):
    asset_type = "wind"

    def __init__(self, params: RenewableParams):
        super().__init__(dataclasses.replace(params, asset_type="wind"))


class SolarRevenueOption(_RenewableRevenueOption):
    asset_type = "solar"

    def __init__(self, params: RenewableParams):
        super().__init__(dataclasses.replace(params, asset_type="solar"))


class CoalDarkSpread:
    """Phase-2 stub: coal valued as a dark-spread option (spec §5 plugin)."""

    asset_type = "coal"
    applicable_levers: list[str] = []

    def __init__(self, params=None):
        self.params = params

    def intrinsic(self, curve: ExpectedCurve) -> tuple[float, float]:
        raise NotImplementedError(COAL_STUB_MSG)

    def value_paths(
        self, paths: np.ndarray, curve: ExpectedCurve | None = None, seed: int = 0
    ) -> tuple[np.ndarray, np.ndarray]:
        raise NotImplementedError(COAL_STUB_MSG)


ASSET_MODELS: dict[str, type] = {
    "bess": BESSDispatchOption,
    "wind": WindRevenueOption,
    "solar": SolarRevenueOption,
    "coal": CoalDarkSpread,
}
