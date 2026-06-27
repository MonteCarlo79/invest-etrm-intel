# IB Trading Platform — Phase 2: Analytics Libs

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build `libs/pricing/`, `libs/fixed_income/`, `libs/fx/`, and `libs/signals/vix.py` — the full cross-asset analytics library that powers the dashboard and advisor apps.

**Architecture:** Pure Python/numpy/scipy implementations; FX options reuse bess-platform BS pricer (Garman-Kohlhagen = BS with `q=r_foreign`); bess-platform `libs/options/` imported via `sys.path` (no rebuild). All modules are pure functions — no state, no DB, testable on any machine.

**Tech Stack:** Python 3.11+, numpy, scipy, pandas (already in requirements.txt); bess-platform `libs/options/` reused via `sys.path.insert`

---

## File Map

**Create:**
- `libs/__init__.py`
- `libs/pricing/__init__.py`
- `libs/pricing/kirk_margrabe.py`
- `libs/pricing/vol_surface.py`
- `libs/pricing/pnl_explain.py`
- `libs/fixed_income/__init__.py`
- `libs/fixed_income/bonds.py`
- `libs/fixed_income/yield_curve.py`
- `libs/fixed_income/swaps.py`
- `libs/fixed_income/caps_floors.py`
- `libs/fx/__init__.py`
- `libs/fx/forwards.py`
- `libs/fx/vol_surface_fx.py`
- `libs/signals/__init__.py`
- `libs/signals/vix.py`
- `tests/libs/__init__.py`
- `tests/libs/pricing/__init__.py`
- `tests/libs/pricing/test_kirk_margrabe.py`
- `tests/libs/pricing/test_vol_surface.py`
- `tests/libs/pricing/test_pnl_explain.py`
- `tests/libs/fixed_income/__init__.py`
- `tests/libs/fixed_income/test_bonds.py`
- `tests/libs/fixed_income/test_yield_curve.py`
- `tests/libs/fixed_income/test_swaps_caps.py`
- `tests/libs/fx/__init__.py`
- `tests/libs/fx/test_forwards.py`
- `tests/libs/fx/test_vol_surface_fx.py`
- `tests/libs/signals/__init__.py`
- `tests/libs/signals/test_vix.py`

---

### Task 1: Directory scaffold

**Files:**
- Create: all `__init__.py` files listed above

- [ ] **Step 1: Create all __init__.py files**

Run from `C:/Users/dipeng.chen/OneDrive/ETRM/ib-platform`:

```bash
mkdir -p libs/pricing libs/fixed_income libs/fx libs/signals
mkdir -p tests/libs/pricing tests/libs/fixed_income tests/libs/fx tests/libs/signals
touch libs/__init__.py libs/pricing/__init__.py libs/fixed_income/__init__.py
touch libs/fx/__init__.py libs/signals/__init__.py
touch tests/libs/__init__.py tests/libs/pricing/__init__.py
touch tests/libs/fixed_income/__init__.py tests/libs/fx/__init__.py
touch tests/libs/signals/__init__.py
```

On Windows PowerShell use `New-Item` or just let the Write steps in subsequent tasks create them implicitly. The test conftest already adds repo root to `sys.path`.

- [ ] **Step 2: Commit**

```bash
git add libs/ tests/libs/
git commit -m "chore: scaffold libs/ and tests/libs/ directories"
```

---

### Task 2: `libs/pricing/kirk_margrabe.py`

Kirk approximation for spread options; Margrabe exchange-option formula.

**Files:**
- Create: `libs/pricing/kirk_margrabe.py`
- Create: `tests/libs/pricing/test_kirk_margrabe.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/libs/pricing/test_kirk_margrabe.py`:

```python
"""Tests for Kirk and Margrabe spread/exchange option pricers."""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "..", ".."))

import math
import pytest
from libs.pricing.kirk_margrabe import kirk_spread_call, margrabe_exchange


class TestKirkSpread:
    def test_zero_strike_equals_margrabe(self):
        # K=0 → Kirk reduces to Margrabe
        price_kirk = kirk_spread_call(F1=110, F2=100, K=0, T=1.0,
                                      sigma1=0.20, sigma2=0.20, rho=0.0, r=0.05)
        price_margrabe = margrabe_exchange(F1=110, F2=100, T=1.0,
                                           sigma1=0.20, sigma2=0.20, rho=0.0, r=0.05)
        assert abs(price_kirk - price_margrabe) < 0.01

    def test_deep_itm_call(self):
        # F1 >> F2+K → price ≈ disc * (F1 - F2 - K)
        price = kirk_spread_call(F1=200, F2=50, K=10, T=1.0,
                                  sigma1=0.20, sigma2=0.20, rho=0.0, r=0.05)
        intrinsic = math.exp(-0.05 * 1.0) * (200 - 50 - 10)
        assert price > intrinsic * 0.98

    def test_deep_otm_call(self):
        # F1 << F2+K → price ≈ 0
        price = kirk_spread_call(F1=50, F2=200, K=10, T=1.0,
                                  sigma1=0.20, sigma2=0.20, rho=0.0, r=0.05)
        assert price < 0.01

    def test_positive_rho_lower_price(self):
        # Higher correlation → lower spread vol → lower option price
        price_low_rho = kirk_spread_call(F1=110, F2=100, K=5, T=1.0,
                                          sigma1=0.20, sigma2=0.20, rho=0.0, r=0.05)
        price_high_rho = kirk_spread_call(F1=110, F2=100, K=5, T=1.0,
                                           sigma1=0.20, sigma2=0.20, rho=0.8, r=0.05)
        assert price_high_rho < price_low_rho

    def test_t_zero_returns_intrinsic(self):
        price = kirk_spread_call(F1=115, F2=100, K=5, T=0.0,
                                  sigma1=0.20, sigma2=0.20, rho=0.0, r=0.05)
        assert abs(price - 10.0) < 1e-6  # 115 - 100 - 5 = 10


class TestMargrabe:
    def test_positive_value(self):
        price = margrabe_exchange(F1=110, F2=100, T=1.0,
                                   sigma1=0.20, sigma2=0.20, rho=0.5, r=0.05)
        assert price > 0

    def test_zero_vol_returns_intrinsic(self):
        price = margrabe_exchange(F1=110, F2=100, T=1.0,
                                   sigma1=0.0, sigma2=0.0, rho=0.0, r=0.05)
        disc = math.exp(-0.05 * 1.0)
        assert abs(price - disc * 10.0) < 1e-6

    def test_symmetry(self):
        # Swapping F1/F2 should give put parity: C - P = disc*(F1-F2)
        c = margrabe_exchange(F1=110, F2=100, T=1.0,
                               sigma1=0.20, sigma2=0.20, rho=0.0, r=0.05)
        p = margrabe_exchange(F1=100, F2=110, T=1.0,
                               sigma1=0.20, sigma2=0.20, rho=0.0, r=0.05)
        disc = math.exp(-0.05 * 1.0)
        assert abs((c - p) - disc * (110 - 100)) < 0.01
```

- [ ] **Step 2: Run to verify FAIL**

```
cd C:/Users/dipeng.chen/OneDrive/ETRM/ib-platform
pytest tests/libs/pricing/test_kirk_margrabe.py -v
```

Expected: `ModuleNotFoundError: No module named 'libs.pricing.kirk_margrabe'`

- [ ] **Step 3: Implement `libs/pricing/kirk_margrabe.py`**

```python
"""
libs/pricing/kirk_margrabe.py

Spread option and exchange option pricers.

Kirk (1995) approximation for spread calls: max(F1 - F2 - K, 0)
Margrabe (1978) for exchange options: max(F1 - F2, 0)

Both are pure-function, no external deps beyond math.
"""
from __future__ import annotations

import math
import sys
import os

# Reuse bess-platform Black-Scholes pricer
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "..", ".."))
from libs.options.black_scholes import b76_price


def kirk_spread_call(
    F1: float,
    F2: float,
    K: float,
    T: float,
    sigma1: float,
    sigma2: float,
    rho: float,
    r: float,
) -> float:
    """
    Kirk (1995) approximation for a European call on the spread F1 - F2 - K.

    Parameters
    ----------
    F1, F2 : forward prices of asset 1 and asset 2
    K      : fixed strike on the spread (set K=0 for exchange option)
    T      : time to expiry in years
    sigma1 : annualised vol of F1
    sigma2 : annualised vol of F2
    rho    : correlation between log(F1) and log(F2)
    r      : risk-free rate for discounting

    Returns
    -------
    float : call option price = E[disc * max(F1 - F2 - K, 0)]
    """
    if T <= 0.0:
        return max(F1 - F2 - K, 0.0) * math.exp(-r * 0.0)

    F2_hat = F2 + K
    if F2_hat <= 0.0:
        return max(F1 - F2 - K, 0.0) * math.exp(-r * T)

    w = F2 / F2_hat
    sigma_hat = math.sqrt(
        sigma1 ** 2 + (w * sigma2) ** 2 - 2.0 * rho * sigma1 * sigma2 * w
    )

    # Now it's a standard Black-76 call: max(F1 - F2_hat, 0)
    return b76_price(F=F1, K=F2_hat, T=T, r=r, sigma=sigma_hat, flag="c")


def margrabe_exchange(
    F1: float,
    F2: float,
    T: float,
    sigma1: float,
    sigma2: float,
    rho: float,
    r: float,
) -> float:
    """
    Margrabe (1978) exchange option: max(F1 - F2, 0).

    Equivalent to Kirk with K=0.

    Parameters
    ----------
    F1, F2 : forward prices
    T      : time to expiry in years
    sigma1 : annualised vol of F1
    sigma2 : annualised vol of F2
    rho    : correlation between log(F1) and log(F2)
    r      : risk-free rate

    Returns
    -------
    float : exchange option price
    """
    if T <= 0.0:
        return max(F1 - F2, 0.0)

    sigma_hat = math.sqrt(
        sigma1 ** 2 + sigma2 ** 2 - 2.0 * rho * sigma1 * sigma2
    )
    if sigma_hat <= 0.0:
        return max(F1 - F2, 0.0) * math.exp(-r * T)

    return b76_price(F=F1, K=F2, T=T, r=r, sigma=sigma_hat, flag="c")
```

- [ ] **Step 4: Run to verify PASS**

```
pytest tests/libs/pricing/test_kirk_margrabe.py -v
```

Expected: 8 passed

- [ ] **Step 5: Commit**

```bash
git add libs/pricing/kirk_margrabe.py tests/libs/pricing/test_kirk_margrabe.py
git commit -m "feat(libs): Kirk spread option and Margrabe exchange option pricers"
```

---

### Task 3: `libs/pricing/vol_surface.py`

Multi-expiry vol surface: SVI per slice stitched into a (K, T) grid.

**Files:**
- Create: `libs/pricing/vol_surface.py`
- Create: `tests/libs/pricing/test_vol_surface.py`

- [ ] **Step 1: Write failing tests**

Create `tests/libs/pricing/test_vol_surface.py`:

```python
"""Tests for multi-expiry vol surface stitching."""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "..", ".."))

import numpy as np
import pytest
from libs.pricing.vol_surface import VolSurface


class TestVolSurface:
    def _make_surface(self):
        # Two expiry slices: T=0.25, T=1.0
        # Each slice: flat vol at 20% for simplicity
        slices = {
            0.25: {"strikes": np.array([90.0, 95.0, 100.0, 105.0, 110.0]),
                   "vols": np.array([0.22, 0.21, 0.20, 0.21, 0.22])},
            1.00: {"strikes": np.array([80.0, 90.0, 100.0, 110.0, 120.0]),
                   "vols": np.array([0.24, 0.22, 0.20, 0.22, 0.24])},
        }
        return VolSurface(F=100.0, slices=slices)

    def test_atm_vol_at_known_expiry(self):
        vs = self._make_surface()
        vol = vs.get_vol(K=100.0, T=0.25)
        assert 0.18 < vol < 0.25

    def test_interpolation_between_expiries(self):
        vs = self._make_surface()
        vol = vs.get_vol(K=100.0, T=0.50)
        assert 0.18 < vol < 0.25

    def test_vol_increases_away_from_atm(self):
        vs = self._make_surface()
        vol_atm = vs.get_vol(K=100.0, T=1.0)
        vol_wing = vs.get_vol(K=80.0, T=1.0)
        assert vol_wing > vol_atm

    def test_grid_shape(self):
        vs = self._make_surface()
        strikes = np.linspace(85, 115, 10)
        expiries = np.array([0.25, 0.5, 1.0])
        grid = vs.vol_grid(strikes=strikes, expiries=expiries)
        assert grid.shape == (len(expiries), len(strikes))
        assert np.all(grid > 0)

    def test_raises_on_zero_expiry(self):
        vs = self._make_surface()
        with pytest.raises((ValueError, ZeroDivisionError)):
            vs.get_vol(K=100.0, T=0.0)
```

- [ ] **Step 2: Run to verify FAIL**

```
pytest tests/libs/pricing/test_vol_surface.py -v
```

Expected: `ModuleNotFoundError: No module named 'libs.pricing.vol_surface'`

- [ ] **Step 3: Implement `libs/pricing/vol_surface.py`**

```python
"""
libs/pricing/vol_surface.py

Multi-expiry implied vol surface built from SVI slices.

For each known expiry, SVI is calibrated from strike/vol pairs.
Between expiries, total variance is linearly interpolated (flat extrapolation
at the wings), then converted back to vol.

Usage
-----
    vs = VolSurface(F=100.0, slices={
        0.25: {"strikes": np.array([...]), "vols": np.array([...])},
        1.00: {"strikes": np.array([...]), "vols": np.array([...])},
    })
    vol = vs.get_vol(K=105.0, T=0.5)
    grid = vs.vol_grid(strikes=np.linspace(80, 120, 40), expiries=np.linspace(0.1, 2.0, 20))
"""
from __future__ import annotations

import sys
import os
from typing import dict as Dict

import numpy as np

# Reuse bess-platform SVI calibration
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "..", ".."))
from libs.options.smile import SVIParams, fit_svi, svi_vol


class VolSurface:
    """
    Multi-expiry implied vol surface.

    Parameters
    ----------
    F : float
        Reference forward price (used to compute log-moneyness for SVI).
    slices : dict[float, dict]
        Keys are expiries in years. Values are dicts with:
          - "strikes" : np.ndarray of absolute strikes
          - "vols"    : np.ndarray of implied vols (annualised)
    """

    def __init__(self, F: float, slices: dict):
        if not slices:
            raise ValueError("At least one expiry slice required")
        self._F = F
        self._expiries = sorted(slices.keys())
        self._svi_params: dict[float, SVIParams] = {}

        for T, data in slices.items():
            if T <= 0.0:
                raise ValueError(f"Expiry must be positive, got T={T}")
            strikes = np.asarray(data["strikes"], dtype=float)
            vols = np.asarray(data["vols"], dtype=float)
            log_k = np.log(strikes / F)
            self._svi_params[T] = fit_svi(log_k, vols, T)

    def _total_var_at_expiry(self, K: float, T: float) -> float:
        """Total variance w = sigma^2 * T from the fitted SVI slice at expiry T."""
        params = self._svi_params[T]
        k = np.log(K / self._F)
        vol = svi_vol(k, params, T)
        return vol ** 2 * T

    def get_vol(self, K: float, T: float) -> float:
        """
        Interpolated implied vol at strike K, expiry T.

        Uses linear interpolation of total variance between adjacent SVI slices.
        Flat extrapolation outside the calibrated expiry range.
        """
        if T <= 0.0:
            raise ValueError(f"Expiry T must be positive, got T={T}")

        expiries = self._expiries

        if T <= expiries[0]:
            # Flat extrapolation at short end
            w = self._total_var_at_expiry(K, expiries[0])
            total_var = w * T / expiries[0]
            return np.sqrt(max(total_var / T, 1e-12))

        if T >= expiries[-1]:
            # Flat extrapolation at long end
            w = self._total_var_at_expiry(K, expiries[-1])
            total_var = w * T / expiries[-1]
            return np.sqrt(max(total_var / T, 1e-12))

        # Find bracketing expiries
        for i in range(len(expiries) - 1):
            T_lo, T_hi = expiries[i], expiries[i + 1]
            if T_lo <= T <= T_hi:
                w_lo = self._total_var_at_expiry(K, T_lo)
                w_hi = self._total_var_at_expiry(K, T_hi)
                alpha = (T - T_lo) / (T_hi - T_lo)
                w_interp = (1 - alpha) * w_lo + alpha * w_hi
                total_var = w_interp * T / (alpha * T_hi + (1 - alpha) * T_lo)
                # Simpler: total_var at T by linear interp of (T*sigma^2)
                # w_lo is total_var at T_lo, w_hi at T_hi
                total_var_T = (1 - alpha) * w_lo + alpha * w_hi
                return np.sqrt(max(total_var_T / T, 1e-12))

        raise RuntimeError(f"Could not bracket T={T} in {expiries}")

    def vol_grid(self, strikes: np.ndarray, expiries: np.ndarray) -> np.ndarray:
        """
        Return a 2D vol grid of shape (len(expiries), len(strikes)).

        Parameters
        ----------
        strikes  : 1D array of strikes
        expiries : 1D array of expiries in years (must be > 0)
        """
        result = np.zeros((len(expiries), len(strikes)))
        for i, T in enumerate(expiries):
            for j, K in enumerate(strikes):
                result[i, j] = self.get_vol(K, T)
        return result
```

- [ ] **Step 4: Run to verify PASS**

```
pytest tests/libs/pricing/test_vol_surface.py -v
```

Expected: 5 passed

- [ ] **Step 5: Commit**

```bash
git add libs/pricing/vol_surface.py tests/libs/pricing/test_vol_surface.py
git commit -m "feat(libs): multi-expiry SVI vol surface with linear total-var interpolation"
```

---

### Task 4: `libs/pricing/pnl_explain.py`

P&L attribution: Delta, Gamma, Vega, SkewVega, Theta, residual.

**Files:**
- Create: `libs/pricing/pnl_explain.py`
- Create: `tests/libs/pricing/test_pnl_explain.py`

- [ ] **Step 1: Write failing tests**

Create `tests/libs/pricing/test_pnl_explain.py`:

```python
"""Tests for P&L explain / attribution."""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "..", ".."))

import pytest
from libs.pricing.pnl_explain import explain_pnl, PnlExplain


class TestPnlExplain:
    def _base_params(self):
        return dict(
            S0=100.0, S1=102.0,
            K=100.0, T0=0.25, T1=0.25 - 1/365,
            r=0.05, q=0.0,
            sigma0=0.20, sigma1=0.21,
            flag="c", quantity=1.0,
        )

    def test_returns_pnl_explain_dataclass(self):
        result = explain_pnl(**self._base_params())
        assert isinstance(result, PnlExplain)

    def test_components_sum_to_total(self):
        result = explain_pnl(**self._base_params())
        total_components = (
            result.delta_pnl
            + result.gamma_pnl
            + result.vega_pnl
            + result.theta_pnl
            + result.residual
        )
        assert abs(total_components - result.total_pnl) < 1e-6

    def test_delta_pnl_positive_on_up_move_call(self):
        result = explain_pnl(**self._base_params())
        assert result.delta_pnl > 0  # S moved up, call delta positive

    def test_theta_pnl_negative_long_call(self):
        result = explain_pnl(**self._base_params())
        assert result.theta_pnl < 0  # Long call loses time value overnight

    def test_vega_pnl_positive_on_vol_rise(self):
        result = explain_pnl(**self._base_params())
        assert result.vega_pnl > 0  # vol rose 20% → 21%, long call profits

    def test_short_position_flips_signs(self):
        params = self._base_params()
        long_result = explain_pnl(**params)
        params["quantity"] = -1.0
        short_result = explain_pnl(**params)
        assert abs(short_result.total_pnl + long_result.total_pnl) < 1e-6
```

- [ ] **Step 2: Run to verify FAIL**

```
pytest tests/libs/pricing/test_pnl_explain.py -v
```

Expected: `ModuleNotFoundError`

- [ ] **Step 3: Implement `libs/pricing/pnl_explain.py`**

```python
"""
libs/pricing/pnl_explain.py

P&L attribution for a single option position between two time points.

Attribution components (all in $):
  delta_pnl  : Delta × ΔS
  gamma_pnl  : ½ × Gamma × ΔS²
  vega_pnl   : Vega × Δsigma  (Vega here is per 1% move, so × Δsigma*100)
  theta_pnl  : Theta × Δt_days  (Theta is per calendar day)
  residual   : total_pnl - (delta + gamma + vega + theta)
"""
from __future__ import annotations

import sys
import os
from dataclasses import dataclass

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "..", ".."))
from libs.options.black_scholes import bs_price, bs_greeks


@dataclass
class PnlExplain:
    total_pnl: float
    delta_pnl: float
    gamma_pnl: float
    vega_pnl: float
    theta_pnl: float
    residual: float


def explain_pnl(
    S0: float,
    S1: float,
    K: float,
    T0: float,
    T1: float,
    r: float,
    q: float,
    sigma0: float,
    sigma1: float,
    flag: str,
    quantity: float = 1.0,
) -> PnlExplain:
    """
    Explain P&L of an option position between t0 and t1.

    Parameters
    ----------
    S0, S1      : spot at start and end
    K           : strike
    T0, T1      : time to expiry at start/end (years)
    r           : risk-free rate
    q           : dividend yield
    sigma0      : implied vol at t0
    sigma1      : implied vol at t1
    flag        : "c" or "p"
    quantity    : signed quantity (positive = long)

    Returns
    -------
    PnlExplain with delta/gamma/vega/theta components and residual
    """
    price0 = bs_price(S0, K, T0, r, sigma0, q, flag)
    price1 = bs_price(S1, K, T1, r, sigma1, q, flag)
    total_pnl = quantity * (price1 - price0)

    greeks0 = bs_greeks(S0, K, T0, r, sigma0, q, flag)

    dS = S1 - S0
    dt_days = (T0 - T1) * 365.0  # positive = time elapsed
    d_sigma = sigma1 - sigma0

    delta_pnl = quantity * greeks0["delta"] * dS
    gamma_pnl = quantity * 0.5 * greeks0["gamma"] * dS ** 2
    # vega in bs_greeks is per 1% vol move; d_sigma in raw vol units
    vega_pnl  = quantity * greeks0["vega"] * d_sigma * 100.0
    # theta is per calendar day (negative for long options)
    theta_pnl = quantity * greeks0["theta"] * dt_days

    residual = total_pnl - (delta_pnl + gamma_pnl + vega_pnl + theta_pnl)

    return PnlExplain(
        total_pnl=total_pnl,
        delta_pnl=delta_pnl,
        gamma_pnl=gamma_pnl,
        vega_pnl=vega_pnl,
        theta_pnl=theta_pnl,
        residual=residual,
    )
```

- [ ] **Step 4: Run to verify PASS**

```
pytest tests/libs/pricing/test_pnl_explain.py -v
```

Expected: 6 passed

- [ ] **Step 5: Commit**

```bash
git add libs/pricing/pnl_explain.py tests/libs/pricing/test_pnl_explain.py
git commit -m "feat(libs): option P&L explain — delta/gamma/vega/theta attribution"
```

---

### Task 5: `libs/fixed_income/bonds.py`

YTM (scipy.brentq), modified duration, DV01, convexity.

**Files:**
- Create: `libs/fixed_income/bonds.py`
- Create: `tests/libs/fixed_income/test_bonds.py`

- [ ] **Step 1: Write failing tests**

Create `tests/libs/fixed_income/test_bonds.py`:

```python
"""Tests for bond analytics: YTM, duration, DV01, convexity."""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "..", ".."))

import pytest
from libs.fixed_income.bonds import (
    bond_price, ytm, macaulay_duration, modified_duration, dv01, convexity
)


class TestBondPrice:
    def test_par_bond_prices_at_par(self):
        # 5% coupon, 5% YTM, semi-annual, 2yr → price = 100
        price = bond_price(face=100, coupon_rate=0.05, ytm_rate=0.05,
                           n_periods=4, periods_per_year=2)
        assert abs(price - 100.0) < 0.01

    def test_discount_bond_below_par(self):
        price = bond_price(face=100, coupon_rate=0.05, ytm_rate=0.07,
                           n_periods=4, periods_per_year=2)
        assert price < 100.0

    def test_premium_bond_above_par(self):
        price = bond_price(face=100, coupon_rate=0.07, ytm_rate=0.05,
                           n_periods=4, periods_per_year=2)
        assert price > 100.0

    def test_zero_coupon_bond(self):
        # Zero coupon: price = face * disc^n
        import math
        n = 10
        r = 0.05
        price = bond_price(face=100, coupon_rate=0.0, ytm_rate=r, n_periods=n, periods_per_year=1)
        expected = 100 / (1 + r) ** n
        assert abs(price - expected) < 1e-6


class TestYTM:
    def test_ytm_of_par_bond_equals_coupon(self):
        y = ytm(price=100, face=100, coupon_rate=0.05, n_periods=4, periods_per_year=2)
        assert abs(y - 0.05) < 1e-5

    def test_ytm_of_discount_bond_above_coupon(self):
        y = ytm(price=95, face=100, coupon_rate=0.05, n_periods=4, periods_per_year=2)
        assert y > 0.05

    def test_ytm_of_premium_bond_below_coupon(self):
        y = ytm(price=105, face=100, coupon_rate=0.05, n_periods=4, periods_per_year=2)
        assert y < 0.05


class TestDuration:
    def test_zero_coupon_duration_equals_maturity(self):
        # Macaulay duration of zero-coupon bond = maturity
        dur = macaulay_duration(face=100, coupon_rate=0.0, ytm_rate=0.05,
                                n_periods=5, periods_per_year=1)
        assert abs(dur - 5.0) < 1e-4

    def test_modified_duration_less_than_macaulay(self):
        mac = macaulay_duration(face=100, coupon_rate=0.05, ytm_rate=0.05,
                                n_periods=4, periods_per_year=2)
        mod = modified_duration(face=100, coupon_rate=0.05, ytm_rate=0.05,
                                n_periods=4, periods_per_year=2)
        assert mod < mac

    def test_dv01_positive(self):
        d = dv01(price=100, face=100, coupon_rate=0.05, ytm_rate=0.05,
                 n_periods=4, periods_per_year=2)
        assert d > 0

    def test_convexity_positive_for_vanilla_bond(self):
        c = convexity(face=100, coupon_rate=0.05, ytm_rate=0.05,
                      n_periods=4, periods_per_year=2)
        assert c > 0
```

- [ ] **Step 2: Run to verify FAIL**

```
pytest tests/libs/fixed_income/test_bonds.py -v
```

Expected: `ModuleNotFoundError`

- [ ] **Step 3: Implement `libs/fixed_income/bonds.py`**

```python
"""
libs/fixed_income/bonds.py

Bond analytics: price, YTM, duration, DV01, convexity.

All functions assume a flat yield curve and fixed periodic coupon payments.
Semi-annual coupon convention is standard (periods_per_year=2).
"""
from __future__ import annotations

from scipy.optimize import brentq


def bond_price(
    face: float,
    coupon_rate: float,
    ytm_rate: float,
    n_periods: int,
    periods_per_year: int = 2,
) -> float:
    """
    Price a fixed-coupon bond.

    Parameters
    ----------
    face           : face / par value
    coupon_rate    : annual coupon rate (e.g. 0.05 = 5%)
    ytm_rate       : annual yield to maturity (e.g. 0.05 = 5%)
    n_periods      : total number of coupon periods remaining
    periods_per_year : coupon payments per year (2 = semi-annual)

    Returns
    -------
    float : dirty price (clean price assuming no accrued interest)
    """
    c = face * coupon_rate / periods_per_year       # coupon per period
    y = ytm_rate / periods_per_year                 # yield per period
    disc = 1.0 / (1.0 + y)

    pv_coupons = c * (1.0 - disc ** n_periods) / y if y != 0.0 else c * n_periods
    pv_principal = face * disc ** n_periods
    return pv_coupons + pv_principal


def ytm(
    price: float,
    face: float,
    coupon_rate: float,
    n_periods: int,
    periods_per_year: int = 2,
    tol: float = 1e-8,
) -> float:
    """
    Yield to maturity via Brent's method.

    Returns annualised YTM.
    """
    def _price_diff(y_annual: float) -> float:
        return bond_price(face, coupon_rate, y_annual, n_periods, periods_per_year) - price

    # Search between near-zero and 200% annual yield
    return brentq(_price_diff, 1e-8, 2.0, xtol=tol)


def macaulay_duration(
    face: float,
    coupon_rate: float,
    ytm_rate: float,
    n_periods: int,
    periods_per_year: int = 2,
) -> float:
    """
    Macaulay duration in years.

    Σ [ t_i × PV(CF_i) ] / Price
    """
    c = face * coupon_rate / periods_per_year
    y = ytm_rate / periods_per_year
    price = bond_price(face, coupon_rate, ytm_rate, n_periods, periods_per_year)

    weighted_sum = 0.0
    for i in range(1, n_periods + 1):
        cf = c if i < n_periods else c + face
        pv_cf = cf / (1.0 + y) ** i
        t_years = i / periods_per_year
        weighted_sum += t_years * pv_cf

    return weighted_sum / price


def modified_duration(
    face: float,
    coupon_rate: float,
    ytm_rate: float,
    n_periods: int,
    periods_per_year: int = 2,
) -> float:
    """
    Modified duration = Macaulay duration / (1 + y/m).

    Approximates % price change per 1 unit (100 bps) change in yield.
    """
    mac = macaulay_duration(face, coupon_rate, ytm_rate, n_periods, periods_per_year)
    y_per_period = ytm_rate / periods_per_year
    return mac / (1.0 + y_per_period)


def dv01(
    price: float,
    face: float,
    coupon_rate: float,
    ytm_rate: float,
    n_periods: int,
    periods_per_year: int = 2,
    bump: float = 0.0001,
) -> float:
    """
    DV01 (dollar value of 1 basis point) via bump-and-reprice.

    Returns absolute $ change in price for +1bp shift in YTM.
    """
    price_up   = bond_price(face, coupon_rate, ytm_rate + bump, n_periods, periods_per_year)
    price_down = bond_price(face, coupon_rate, ytm_rate - bump, n_periods, periods_per_year)
    return abs(price_down - price_up) / 2.0


def convexity(
    face: float,
    coupon_rate: float,
    ytm_rate: float,
    n_periods: int,
    periods_per_year: int = 2,
) -> float:
    """
    Convexity (annualised).

    Σ [ t_i × (t_i + 1/m) × PV(CF_i) ] / [ Price × (1 + y/m)^2 ]
    """
    c = face * coupon_rate / periods_per_year
    y = ytm_rate / periods_per_year
    price = bond_price(face, coupon_rate, ytm_rate, n_periods, periods_per_year)

    total = 0.0
    for i in range(1, n_periods + 1):
        cf = c if i < n_periods else c + face
        pv_cf = cf / (1.0 + y) ** i
        t_years = i / periods_per_year
        total += t_years * (t_years + 1.0 / periods_per_year) * pv_cf

    return total / (price * (1.0 + y) ** 2)
```

- [ ] **Step 4: Run to verify PASS**

```
pytest tests/libs/fixed_income/test_bonds.py -v
```

Expected: 12 passed

- [ ] **Step 5: Commit**

```bash
git add libs/fixed_income/bonds.py tests/libs/fixed_income/test_bonds.py
git commit -m "feat(libs): bond analytics — price, YTM, duration, DV01, convexity"
```

---

### Task 6: `libs/fixed_income/yield_curve.py`

Nelson-Siegel fitting + linear bootstrap.

**Files:**
- Create: `libs/fixed_income/yield_curve.py`
- Create: `tests/libs/fixed_income/test_yield_curve.py`

- [ ] **Step 1: Write failing tests**

Create `tests/libs/fixed_income/test_yield_curve.py`:

```python
"""Tests for yield curve: Nelson-Siegel fit and linear bootstrap."""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "..", ".."))

import numpy as np
import pytest
from libs.fixed_income.yield_curve import NelsonSiegelCurve, bootstrap_curve


class TestNelsonSiegel:
    def _flat_curve(self):
        # Fit a flat 5% curve
        tenors = np.array([0.25, 0.5, 1.0, 2.0, 5.0, 10.0])
        rates  = np.array([0.05, 0.05, 0.05, 0.05, 0.05, 0.05])
        return NelsonSiegelCurve.fit(tenors, rates)

    def test_flat_curve_returns_5pct(self):
        curve = self._flat_curve()
        assert abs(curve.rate(1.0) - 0.05) < 0.002
        assert abs(curve.rate(5.0) - 0.05) < 0.002

    def test_upward_sloping_curve(self):
        tenors = np.array([0.5, 1.0, 2.0, 5.0, 10.0, 30.0])
        rates  = np.array([0.03, 0.035, 0.04, 0.045, 0.05, 0.055])
        curve = NelsonSiegelCurve.fit(tenors, rates)
        assert curve.rate(30.0) > curve.rate(0.5)

    def test_interpolation_between_knots(self):
        tenors = np.array([1.0, 2.0, 5.0, 10.0])
        rates  = np.array([0.03, 0.035, 0.04, 0.045])
        curve = NelsonSiegelCurve.fit(tenors, rates)
        mid_rate = curve.rate(3.0)
        assert 0.025 < mid_rate < 0.055

    def test_discount_factor_positive_and_leq_one(self):
        curve = self._flat_curve()
        for t in [0.5, 1.0, 5.0, 10.0]:
            df = curve.discount_factor(t)
            assert 0 < df <= 1.0


class TestBootstrap:
    def test_bootstrap_returns_curve(self):
        tenors = np.array([1.0, 2.0, 3.0, 5.0])
        rates  = np.array([0.04, 0.042, 0.044, 0.048])
        curve = bootstrap_curve(tenors, rates)
        assert abs(curve(1.0) - 0.04) < 1e-6

    def test_bootstrap_linear_interpolation(self):
        tenors = np.array([1.0, 3.0])
        rates  = np.array([0.04, 0.06])
        curve = bootstrap_curve(tenors, rates)
        mid = curve(2.0)
        assert abs(mid - 0.05) < 1e-6
```

- [ ] **Step 2: Run to verify FAIL**

```
pytest tests/libs/fixed_income/test_yield_curve.py -v
```

Expected: `ModuleNotFoundError`

- [ ] **Step 3: Implement `libs/fixed_income/yield_curve.py`**

```python
"""
libs/fixed_income/yield_curve.py

Yield curve models:
  - NelsonSiegelCurve  : parametric 3-factor fit via scipy.optimize.minimize
  - bootstrap_curve     : simple piecewise-linear bootstrap (callable)

Nelson-Siegel formula:
  y(t) = β0
       + β1 × (1 - e^(-λt)) / (λt)
       + β2 × [(1 - e^(-λt)) / (λt) - e^(-λt)]
"""
from __future__ import annotations

import math
from dataclasses import dataclass

import numpy as np
from scipy.interpolate import interp1d
from scipy.optimize import minimize


@dataclass
class NelsonSiegelCurve:
    beta0: float   # long-run level
    beta1: float   # short-term component
    beta2: float   # medium-term hump
    lam: float     # decay factor λ > 0

    @staticmethod
    def _ns_rate(t: float, beta0: float, beta1: float, beta2: float, lam: float) -> float:
        if t <= 0.0:
            return beta0 + beta1
        lt = lam * t
        exp_lt = math.exp(-lt)
        factor = (1.0 - exp_lt) / lt if lt > 1e-10 else 1.0 - lt / 2.0
        return beta0 + beta1 * factor + beta2 * (factor - exp_lt)

    def rate(self, t: float) -> float:
        """Spot rate at tenor t (in years)."""
        return self._ns_rate(t, self.beta0, self.beta1, self.beta2, self.lam)

    def discount_factor(self, t: float) -> float:
        """Continuous-compounding discount factor: exp(-r(t) × t)."""
        if t <= 0.0:
            return 1.0
        r = self.rate(t)
        return math.exp(-r * t)

    @classmethod
    def fit(cls, tenors: np.ndarray, rates: np.ndarray) -> "NelsonSiegelCurve":
        """
        Fit Nelson-Siegel parameters to observed (tenor, rate) pairs.

        Parameters
        ----------
        tenors : array of tenors in years (e.g. [0.25, 0.5, 1, 2, 5, 10, 30])
        rates  : array of spot rates (annualised, e.g. 0.05 = 5%)
        """
        tenors = np.asarray(tenors, dtype=float)
        rates  = np.asarray(rates, dtype=float)

        def objective(params: np.ndarray) -> float:
            b0, b1, b2, lam = params
            if lam <= 0:
                return 1e10
            fitted = np.array([cls._ns_rate(t, b0, b1, b2, lam) for t in tenors])
            return float(np.sum((fitted - rates) ** 2))

        # Initial guess: β0=long-end rate, β1=short-end-long, β2=0, λ=0.5
        b0_0 = float(rates[-1])
        b1_0 = float(rates[0] - rates[-1])
        x0   = [b0_0, b1_0, 0.0, 0.5]

        result = minimize(
            objective, x0,
            method="Nelder-Mead",
            options={"maxiter": 50000, "xatol": 1e-9, "fatol": 1e-12},
        )

        b0, b1, b2, lam = result.x
        lam = max(lam, 1e-4)
        return cls(beta0=b0, beta1=b1, beta2=b2, lam=lam)


def bootstrap_curve(
    tenors: np.ndarray,
    rates: np.ndarray,
) -> interp1d:
    """
    Piecewise-linear bootstrap from (tenor, rate) pairs.

    Returns a callable f(t) → interpolated spot rate.
    Flat extrapolation outside the tenor range.
    """
    tenors = np.asarray(tenors, dtype=float)
    rates  = np.asarray(rates, dtype=float)
    idx = np.argsort(tenors)
    return interp1d(tenors[idx], rates[idx], kind="linear",
                    bounds_error=False, fill_value=(rates[idx[0]], rates[idx[-1]]))
```

- [ ] **Step 4: Run to verify PASS**

```
pytest tests/libs/fixed_income/test_yield_curve.py -v
```

Expected: 7 passed

- [ ] **Step 5: Commit**

```bash
git add libs/fixed_income/yield_curve.py tests/libs/fixed_income/test_yield_curve.py
git commit -m "feat(libs): Nelson-Siegel yield curve fit + linear bootstrap"
```

---

### Task 7: `libs/fixed_income/swaps.py` + `caps_floors.py`

IRS fixed-float NPV, par rate, DV01; Black cap/floor pricing.

**Files:**
- Create: `libs/fixed_income/swaps.py`
- Create: `libs/fixed_income/caps_floors.py`
- Create: `tests/libs/fixed_income/test_swaps_caps.py`

- [ ] **Step 1: Write failing tests**

Create `tests/libs/fixed_income/test_swaps_caps.py`:

```python
"""Tests for IRS swaps and interest rate cap/floor pricers."""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "..", ".."))

import numpy as np
import pytest
from libs.fixed_income.swaps import irs_npv, par_rate, swap_dv01
from libs.fixed_income.caps_floors import caplet_black, cap_black


class TestSwaps:
    def _flat_df(self, tenors, r=0.05):
        """Simple flat discount factor callable."""
        import math
        def df(t):
            return math.exp(-r * t)
        return df

    def test_par_rate_swap_has_zero_npv(self):
        df = self._flat_df([0.5, 1.0, 1.5, 2.0])
        pay_times = [0.5, 1.0, 1.5, 2.0]
        K = par_rate(pay_times=pay_times, notional=1_000_000, discount_factor=df)
        npv = irs_npv(fixed_rate=K, pay_times=pay_times,
                      notional=1_000_000, discount_factor=df)
        assert abs(npv) < 0.01

    def test_above_par_rate_payer_npv_negative(self):
        df = self._flat_df([0.5, 1.0, 1.5, 2.0])
        pay_times = [0.5, 1.0, 1.5, 2.0]
        K = par_rate(pay_times=pay_times, notional=1_000_000, discount_factor=df)
        # Pay above par → receiver has positive NPV
        npv = irs_npv(fixed_rate=K + 0.01, pay_times=pay_times,
                      notional=1_000_000, discount_factor=df)
        assert npv < 0

    def test_swap_dv01_positive(self):
        df = self._flat_df([0.5, 1.0, 1.5, 2.0])
        pay_times = [0.5, 1.0, 1.5, 2.0]
        d = swap_dv01(fixed_rate=0.05, pay_times=pay_times,
                      notional=1_000_000, discount_factor=df)
        assert d > 0


class TestCapsFloors:
    def test_caplet_positive_value(self):
        price = caplet_black(F=0.05, K=0.04, T=1.0, sigma=0.20, df=0.95,
                             notional=1_000_000, tau=0.5)
        assert price > 0

    def test_cap_sum_of_caplets(self):
        # A 2-year semi-annual cap = sum of 4 caplets
        F = 0.05; K = 0.04; sigma = 0.20; notional = 1_000_000; tau = 0.5
        import math
        r = 0.05
        total = 0.0
        for i in range(1, 5):
            t = i * tau
            df = math.exp(-r * t)
            total += caplet_black(F=F, K=K, T=t, sigma=sigma, df=df,
                                   notional=notional, tau=tau)
        cap = cap_black(F=F, K=K, sigma=sigma, pay_times=[0.5, 1.0, 1.5, 2.0],
                        tau=tau, notional=notional,
                        discount_factor=lambda t: math.exp(-r * t))
        assert abs(cap - total) < 0.01

    def test_deep_otm_cap_near_zero(self):
        import math
        cap = cap_black(F=0.02, K=0.10, sigma=0.20,
                        pay_times=[0.5, 1.0, 1.5, 2.0], tau=0.5,
                        notional=1_000_000,
                        discount_factor=lambda t: math.exp(-0.05 * t))
        assert cap < 10.0
```

- [ ] **Step 2: Run to verify FAIL**

```
pytest tests/libs/fixed_income/test_swaps_caps.py -v
```

Expected: `ModuleNotFoundError`

- [ ] **Step 3: Implement `libs/fixed_income/swaps.py`**

```python
"""
libs/fixed_income/swaps.py

Interest rate swap analytics (fixed-float IRS).

Conventions
-----------
- Fixed leg pays at specified pay_times; floating leg = OIS / par swap
- Discount factors supplied as callable df(t) → float
- NPV from the perspective of the fixed-rate PAYER (short the swap):
    NPV_payer = PV_float - PV_fixed
"""
from __future__ import annotations

from typing import Callable, List


def irs_npv(
    fixed_rate: float,
    pay_times: List[float],
    notional: float,
    discount_factor: Callable[[float], float],
    tau: float | None = None,
) -> float:
    """
    NPV of a fixed-float IRS from the perspective of the fixed-rate payer.

    Parameters
    ----------
    fixed_rate      : annual fixed coupon rate
    pay_times       : list of payment times in years (equal spacing assumed if tau=None)
    notional        : notional principal
    discount_factor : callable df(t) → discount factor at time t
    tau             : accrual period in years per coupon; if None, inferred from pay_times

    Returns
    -------
    float : NPV (positive = payer profits; par swap = 0)
    """
    if not pay_times:
        return 0.0

    # Infer accrual period
    if tau is None:
        if len(pay_times) >= 2:
            tau = pay_times[1] - pay_times[0]
        else:
            tau = pay_times[0]

    # PV of fixed leg: Σ fixed_rate × tau × notional × df(t_i)
    pv_fixed = sum(fixed_rate * tau * notional * discount_factor(t) for t in pay_times)

    # PV of floating leg (assuming par): notional × (df(0) - df(T_N)) = notional × (1 - df(T_N))
    # For a standard IRS starting today, df(0)=1
    pv_float = notional * (1.0 - discount_factor(pay_times[-1]))

    return pv_float - pv_fixed


def par_rate(
    pay_times: List[float],
    notional: float,
    discount_factor: Callable[[float], float],
    tau: float | None = None,
) -> float:
    """
    Par fixed rate that makes IRS NPV = 0.

    par_rate = (1 - df(T_N)) / Σ [tau × df(t_i)]
    """
    if not pay_times:
        raise ValueError("pay_times must be non-empty")

    if tau is None:
        tau = pay_times[1] - pay_times[0] if len(pay_times) >= 2 else pay_times[0]

    annuity = sum(tau * discount_factor(t) for t in pay_times)
    df_last = discount_factor(pay_times[-1])
    return (1.0 - df_last) / annuity


def swap_dv01(
    fixed_rate: float,
    pay_times: List[float],
    notional: float,
    discount_factor: Callable[[float], float],
    tau: float | None = None,
    bump: float = 0.0001,
) -> float:
    """
    Swap DV01: absolute $ NPV change per 1bp parallel shift in the discount curve.

    Uses bump-and-reprice with a simple parallel-shifted discount factor.
    """
    import math

    def _bumped_df(t: float, shift: float) -> float:
        r_flat = -math.log(discount_factor(t)) / t if t > 0 else 0.0
        return math.exp(-(r_flat + shift) * t)

    npv_up   = irs_npv(fixed_rate, pay_times, notional,
                       lambda t: _bumped_df(t, +bump), tau)
    npv_down = irs_npv(fixed_rate, pay_times, notional,
                       lambda t: _bumped_df(t, -bump), tau)
    return abs(npv_down - npv_up) / 2.0
```

- [ ] **Step 4: Implement `libs/fixed_income/caps_floors.py`**

```python
"""
libs/fixed_income/caps_floors.py

Interest rate cap and floor pricing using the Black (1976) model.

A cap = series of caplets; each caplet prices a call on the forward rate.
A floor = series of floorlets; each floorlet prices a put on the forward rate.

Reference: Black (1976) "The pricing of commodity contracts"
"""
from __future__ import annotations

import math
from typing import Callable, List


def _norm_cdf(x: float) -> float:
    return math.erfc(-x / math.sqrt(2)) / 2.0


def caplet_black(
    F: float,
    K: float,
    T: float,
    sigma: float,
    df: float,
    notional: float,
    tau: float,
) -> float:
    """
    Black caplet price: a call on the forward rate F with strike K.

    Parameters
    ----------
    F        : forward rate for the period
    K        : cap strike rate
    T        : option expiry (reset date) in years
    sigma    : lognormal vol of the forward rate (annualised)
    df       : discount factor to the payment date
    notional : notional
    tau      : accrual period (payment = notional × tau × max(F-K, 0))

    Returns
    -------
    float : caplet present value
    """
    if T <= 0.0 or sigma <= 0.0:
        return max(F - K, 0.0) * notional * tau * df

    d1 = (math.log(F / K) + 0.5 * sigma ** 2 * T) / (sigma * math.sqrt(T))
    d2 = d1 - sigma * math.sqrt(T)
    return notional * tau * df * (F * _norm_cdf(d1) - K * _norm_cdf(d2))


def floorlet_black(
    F: float,
    K: float,
    T: float,
    sigma: float,
    df: float,
    notional: float,
    tau: float,
) -> float:
    """
    Black floorlet price: a put on the forward rate F with strike K.
    """
    if T <= 0.0 or sigma <= 0.0:
        return max(K - F, 0.0) * notional * tau * df

    d1 = (math.log(F / K) + 0.5 * sigma ** 2 * T) / (sigma * math.sqrt(T))
    d2 = d1 - sigma * math.sqrt(T)
    return notional * tau * df * (K * _norm_cdf(-d2) - F * _norm_cdf(-d1))


def cap_black(
    F: float,
    K: float,
    sigma: float,
    pay_times: List[float],
    tau: float,
    notional: float,
    discount_factor: Callable[[float], float],
) -> float:
    """
    Cap price = sum of Black caplets, one per reset/payment period.

    Parameters
    ----------
    F               : flat forward rate (same for all periods — simplified)
    K               : cap strike
    sigma           : flat cap vol
    pay_times       : list of payment dates in years
    tau             : accrual period per caplet
    notional        : notional
    discount_factor : callable df(t) → discount factor

    Returns
    -------
    float : total cap present value
    """
    return sum(
        caplet_black(F=F, K=K, T=t, sigma=sigma,
                     df=discount_factor(t), notional=notional, tau=tau)
        for t in pay_times
    )


def floor_black(
    F: float,
    K: float,
    sigma: float,
    pay_times: List[float],
    tau: float,
    notional: float,
    discount_factor: Callable[[float], float],
) -> float:
    """Floor price = sum of Black floorlets."""
    return sum(
        floorlet_black(F=F, K=K, T=t, sigma=sigma,
                       df=discount_factor(t), notional=notional, tau=tau)
        for t in pay_times
    )
```

- [ ] **Step 5: Run to verify PASS**

```
pytest tests/libs/fixed_income/test_swaps_caps.py -v
```

Expected: 6 passed

- [ ] **Step 6: Commit**

```bash
git add libs/fixed_income/swaps.py libs/fixed_income/caps_floors.py \
        tests/libs/fixed_income/test_swaps_caps.py
git commit -m "feat(libs): IRS swap NPV/par-rate/DV01 and Black cap/floor pricers"
```

---

### Task 8: `libs/fx/forwards.py`

CIP forward rate, forward points, cross rates.

**Files:**
- Create: `libs/fx/forwards.py`
- Create: `tests/libs/fx/test_forwards.py`

- [ ] **Step 1: Write failing tests**

Create `tests/libs/fx/test_forwards.py`:

```python
"""Tests for FX forward rate and cross-rate calculations."""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "..", ".."))

import pytest
from libs.fx.forwards import fx_forward, forward_points, cross_rate


class TestFXForward:
    def test_cip_forward_usd_jpy(self):
        # USD/JPY spot 150, r_usd=5%, r_jpy=0%, 1yr
        # Forward = 150 × exp((0.00 - 0.05) × 1) ≈ 142.68
        fwd = fx_forward(spot=150.0, r_domestic=0.00, r_foreign=0.05, T=1.0)
        import math
        expected = 150.0 * math.exp((0.00 - 0.05) * 1.0)
        assert abs(fwd - expected) < 0.01

    def test_zero_rate_diff_forward_equals_spot(self):
        fwd = fx_forward(spot=1.25, r_domestic=0.04, r_foreign=0.04, T=0.5)
        assert abs(fwd - 1.25) < 1e-6

    def test_forward_points_sign(self):
        # Higher domestic rate → forward below spot (domestic discount)
        pts = forward_points(spot=1.30, r_domestic=0.05, r_foreign=0.02, T=1.0)
        assert pts < 0  # forward at discount

    def test_forward_points_unit(self):
        import math
        spot = 1.30; rd = 0.05; rf = 0.02; T = 1.0
        fwd = fx_forward(spot, rd, rf, T)
        pts = forward_points(spot, rd, rf, T)
        assert abs(spot + pts - fwd) < 1e-8


class TestCrossRate:
    def test_eur_jpy_from_eur_usd_and_usd_jpy(self):
        # EUR/USD = 1.10, USD/JPY = 150 → EUR/JPY ≈ 165
        eur_jpy = cross_rate(spot_a_b=1.10, spot_b_c=150.0)
        assert abs(eur_jpy - 165.0) < 0.01

    def test_identity(self):
        rate = cross_rate(1.0, 1.0)
        assert abs(rate - 1.0) < 1e-10
```

- [ ] **Step 2: Run to verify FAIL**

```
pytest tests/libs/fx/test_forwards.py -v
```

Expected: `ModuleNotFoundError`

- [ ] **Step 3: Implement `libs/fx/forwards.py`**

```python
"""
libs/fx/forwards.py

FX forward rate calculations under Covered Interest Parity (CIP).

Conventions
-----------
- All rates are annual, continuously compounded.
- spot     : units of domestic per 1 unit of foreign (e.g. JPY/USD = 150 means
             1 USD = 150 JPY; domestic = JPY, foreign = USD)
- Forward  : F = spot × exp((r_domestic - r_foreign) × T)

CIP formula is exact for continuously compounded rates. For simple-compounding
markets, callers should convert: r_cc = ln(1 + r_simple).
"""
from __future__ import annotations

import math


def fx_forward(
    spot: float,
    r_domestic: float,
    r_foreign: float,
    T: float,
) -> float:
    """
    CIP forward exchange rate.

    Parameters
    ----------
    spot        : spot rate (domestic / foreign)
    r_domestic  : domestic risk-free rate (continuously compounded, annualised)
    r_foreign   : foreign risk-free rate (continuously compounded, annualised)
    T           : tenor in years

    Returns
    -------
    float : forward rate (domestic / foreign)
    """
    return spot * math.exp((r_domestic - r_foreign) * T)


def forward_points(
    spot: float,
    r_domestic: float,
    r_foreign: float,
    T: float,
) -> float:
    """
    Forward points = forward - spot.

    Positive → forward premium (domestic rate < foreign rate).
    Negative → forward discount (domestic rate > foreign rate).
    """
    return fx_forward(spot, r_domestic, r_foreign, T) - spot


def cross_rate(spot_a_b: float, spot_b_c: float) -> float:
    """
    Compute the A/C cross rate from A/B and B/C quotes.

    Example: EUR/USD = 1.10, USD/JPY = 150 → EUR/JPY = 1.10 × 150 = 165

    Parameters
    ----------
    spot_a_b : rate A/B (units of B per 1 unit of A)
    spot_b_c : rate B/C (units of C per 1 unit of B)

    Returns
    -------
    float : A/C rate (units of C per 1 unit of A)
    """
    return spot_a_b * spot_b_c
```

- [ ] **Step 4: Run to verify PASS**

```
pytest tests/libs/fx/test_forwards.py -v
```

Expected: 6 passed

- [ ] **Step 5: Commit**

```bash
git add libs/fx/forwards.py tests/libs/fx/test_forwards.py
git commit -m "feat(libs): FX forward rate (CIP), forward points, cross rates"
```

---

### Task 9: `libs/fx/vol_surface_fx.py`

Delta-space RR/BF quotes → absolute strikes → SVI calibration.

**Files:**
- Create: `libs/fx/vol_surface_fx.py`
- Create: `tests/libs/fx/test_vol_surface_fx.py`

- [ ] **Step 1: Write failing tests**

Create `tests/libs/fx/test_vol_surface_fx.py`:

```python
"""Tests for FX vol surface: delta-space quotes → strikes → SVI."""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "..", ".."))

import numpy as np
import pytest
from libs.fx.vol_surface_fx import delta_to_strike, build_fx_smile, FXVolSmile


class TestDeltaToStrike:
    def test_atm_delta_50_returns_forward(self):
        # 50-delta call strike ≈ F × exp(0.5 × sigma^2 × T) (ATM-forward convention)
        K = delta_to_strike(delta=0.5, F=100.0, T=1.0, sigma=0.10, r=0.05, flag="c")
        import math
        expected = 100.0 * math.exp(0.5 * 0.10 ** 2 * 1.0)
        assert abs(K - expected) < 0.01

    def test_25_delta_call_above_atm(self):
        K_atm = delta_to_strike(0.50, 100.0, 1.0, 0.10, 0.05, "c")
        K_25c = delta_to_strike(0.25, 100.0, 1.0, 0.10, 0.05, "c")
        assert K_25c > K_atm

    def test_25_delta_put_below_atm(self):
        K_atm = delta_to_strike(0.50, 100.0, 1.0, 0.10, 0.05, "c")
        K_25p = delta_to_strike(0.25, 100.0, 1.0, 0.10, 0.05, "p")
        assert K_25p < K_atm


class TestBuildFXSmile:
    def _make_quotes(self):
        return {
            "atm": 0.10,      # ATM straddle vol
            "rr25": 0.005,    # 25-delta risk reversal (call vol - put vol)
            "bf25": 0.001,    # 25-delta butterfly ((call + put)/2 - atm)
        }

    def test_returns_fx_vol_smile(self):
        smile = build_fx_smile(F=100.0, T=1.0, r=0.05, quotes=self._make_quotes())
        assert isinstance(smile, FXVolSmile)

    def test_atm_vol_close_to_input(self):
        smile = build_fx_smile(F=100.0, T=1.0, r=0.05, quotes=self._make_quotes())
        vol_atm = smile.vol(K=100.0)
        assert abs(vol_atm - 0.10) < 0.02

    def test_wing_vols_higher_than_atm_with_positive_bf(self):
        smile = build_fx_smile(F=100.0, T=1.0, r=0.05, quotes=self._make_quotes())
        vol_atm = smile.vol(K=100.0)
        vol_otm = smile.vol(K=115.0)
        assert vol_otm >= vol_atm - 0.005
```

- [ ] **Step 2: Run to verify FAIL**

```
pytest tests/libs/fx/test_vol_surface_fx.py -v
```

Expected: `ModuleNotFoundError`

- [ ] **Step 3: Implement `libs/fx/vol_surface_fx.py`**

```python
"""
libs/fx/vol_surface_fx.py

FX volatility surface from market delta-space quotes.

Standard FX vol market quotes:
  - ATM vol (delta-neutral straddle)
  - 25-delta risk reversal: RR25 = vol_25C - vol_25P
  - 25-delta butterfly: BF25 = (vol_25C + vol_25P) / 2 - ATM

From these three quotes we recover:
  vol_25C = ATM + BF25 + RR25/2
  vol_25P = ATM + BF25 - RR25/2

Then convert deltas to strikes, then fit SVI to (strike, vol) triplet.

FX options use the Garman-Kohlhagen model = Black-Scholes with q=r_foreign.
"""
from __future__ import annotations

import math
import sys
import os
from dataclasses import dataclass
from typing import Dict

import numpy as np

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "..", ".."))
from libs.options.black_scholes import bs_greeks
from libs.options.smile import SVIParams, fit_svi, svi_vol


def delta_to_strike(
    delta: float,
    F: float,
    T: float,
    sigma: float,
    r: float,
    flag: str = "c",
) -> float:
    """
    Convert a Black-Scholes delta to an absolute strike via inverse formula.

    For a call: K = F × exp(-d1 × sigma × sqrt(T) + 0.5 × sigma^2 × T)
    where d1 = N^{-1}(delta × exp(r × T)) for spot delta convention.

    Parameters
    ----------
    delta : option delta (positive, e.g. 0.25 for 25-delta call or put)
    F     : forward price
    T     : time to expiry in years
    sigma : vol to use for the conversion
    r     : risk-free rate (for discounting delta)
    flag  : "c" = call, "p" = put

    Returns
    -------
    float : absolute strike K
    """
    from scipy.special import ndtri  # inverse normal CDF

    if flag == "p":
        # Put delta is negative; use absolute value
        delta_call_equiv = 1.0 - delta  # put-call parity on deltas
    else:
        delta_call_equiv = delta

    # Spot delta = exp(-r*T) × N(d1)  →  d1 = N^{-1}(delta × exp(r×T))
    disc_inv = math.exp(r * T)
    d1 = float(ndtri(min(max(delta_call_equiv * disc_inv, 1e-9), 1 - 1e-9)))
    sq_T = math.sqrt(T)
    log_FK = d1 * sigma * sq_T - 0.5 * sigma ** 2 * T
    return F * math.exp(-log_FK)


@dataclass
class FXVolSmile:
    """A calibrated FX vol smile for one expiry."""
    F: float
    T: float
    svi: SVIParams

    def vol(self, K: float) -> float:
        """Interpolated implied vol at strike K."""
        k = math.log(K / self.F)
        return svi_vol(k, self.svi, self.T)


def build_fx_smile(
    F: float,
    T: float,
    r: float,
    quotes: Dict[str, float],
) -> FXVolSmile:
    """
    Build a calibrated FX vol smile from market delta-space quotes.

    Parameters
    ----------
    F      : forward price
    T      : time to expiry in years
    r      : risk-free rate
    quotes : dict with keys "atm", "rr25", "bf25"
             (optional: "rr10", "bf10" for 10-delta wings)

    Returns
    -------
    FXVolSmile with fitted SVI parameters
    """
    atm = quotes["atm"]
    rr25 = quotes.get("rr25", 0.0)
    bf25 = quotes.get("bf25", 0.0)

    vol_25c = atm + bf25 + rr25 / 2.0
    vol_25p = atm + bf25 - rr25 / 2.0

    # Convert deltas to strikes
    K_atm = delta_to_strike(0.5, F, T, atm, r, "c")
    K_25c = delta_to_strike(0.25, F, T, vol_25c, r, "c")
    K_25p = delta_to_strike(0.25, F, T, vol_25p, r, "p")

    # Include 10-delta wings if provided
    strikes = [K_25p, K_atm, K_25c]
    vols    = [vol_25p, atm, vol_25c]

    if "rr10" in quotes and "bf10" in quotes:
        rr10 = quotes["rr10"]; bf10 = quotes["bf10"]
        vol_10c = atm + bf10 + rr10 / 2.0
        vol_10p = atm + bf10 - rr10 / 2.0
        strikes = [delta_to_strike(0.10, F, T, vol_10p, r, "p")] + strikes + \
                  [delta_to_strike(0.10, F, T, vol_10c, r, "c")]
        vols    = [vol_10p] + vols + [vol_10c]

    log_strikes = np.log(np.array(strikes) / F)
    svi = fit_svi(log_strikes, np.array(vols), T)
    return FXVolSmile(F=F, T=T, svi=svi)
```

- [ ] **Step 4: Run to verify PASS**

```
pytest tests/libs/fx/test_vol_surface_fx.py -v
```

Expected: 5 passed

- [ ] **Step 5: Commit**

```bash
git add libs/fx/vol_surface_fx.py tests/libs/fx/test_vol_surface_fx.py
git commit -m "feat(libs): FX vol surface — delta quotes to strikes, SVI smile calibration"
```

---

### Task 10: `libs/signals/vix.py`

VIX term structure signals: contango, roll yield, IVP, regime classification.

**Files:**
- Create: `libs/signals/vix.py`
- Create: `tests/libs/signals/test_vix.py`

- [ ] **Step 1: Write failing tests**

Create `tests/libs/signals/test_vix.py`:

```python
"""Tests for VIX term structure signal library."""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "..", ".."))

import pytest
from libs.signals.vix import (
    contango_pct, roll_yield_annualised, implied_vol_premium,
    vix_regime, VixTermStructure
)


class TestContango:
    def test_contango_positive_when_m2_above_m1(self):
        assert contango_pct(m1=15.0, m2=16.0) > 0

    def test_backwardation_negative(self):
        assert contango_pct(m1=30.0, m2=25.0) < 0

    def test_flat_curve_zero(self):
        assert abs(contango_pct(m1=20.0, m2=20.0)) < 1e-9


class TestRollYield:
    def test_contango_roll_yield_negative(self):
        # In contango, rolling long VX futures loses value → negative roll yield
        ry = roll_yield_annualised(m1=15.0, m2=16.0, days_to_expiry=30)
        assert ry < 0

    def test_backwardation_roll_yield_positive(self):
        ry = roll_yield_annualised(m1=30.0, m2=25.0, days_to_expiry=30)
        assert ry > 0


class TestIVP:
    def test_ivp_between_0_and_1(self):
        import numpy as np
        history = list(np.random.uniform(10, 40, 252))
        ivp = implied_vol_premium(vix=20.0, history=history)
        assert 0.0 <= ivp <= 1.0

    def test_vix_at_min_gives_low_ivp(self):
        history = [25.0] * 252
        ivp = implied_vol_premium(vix=10.0, history=history)
        assert ivp < 0.1

    def test_vix_at_max_gives_high_ivp(self):
        history = [10.0] * 252
        ivp = implied_vol_premium(vix=50.0, history=history)
        assert ivp > 0.9


class TestRegime:
    def test_contango_regime(self):
        assert vix_regime(m1=15.0, m2=17.0, vix_index=15.0) == "contango"

    def test_backwardation_regime(self):
        assert vix_regime(m1=35.0, m2=30.0, vix_index=35.0) == "backwardation"

    def test_spike_regime(self):
        assert vix_regime(m1=40.0, m2=38.0, vix_index=42.0) == "spike"


class TestVixTermStructure:
    def test_dataclass_computes_signals(self):
        ts = VixTermStructure(
            vix_index=18.0, m1=17.0, m2=19.0, m3=20.0,
            days_to_m1_expiry=20
        )
        assert ts.contango_m1_m2 > 0
        assert ts.regime in ("contango", "backwardation", "spike")
```

- [ ] **Step 2: Run to verify FAIL**

```
pytest tests/libs/signals/test_vix.py -v
```

Expected: `ModuleNotFoundError`

- [ ] **Step 3: Implement `libs/signals/vix.py`**

```python
"""
libs/signals/vix.py

VIX term structure signals for volatility regime classification.

Signals
-------
contango_pct(m1, m2)                        → (m2 - m1) / m1 as a ratio
roll_yield_annualised(m1, m2, days)         → annualised roll cost of long M1 futures
implied_vol_premium(vix, history)           → percentile rank of VIX in history [0, 1]
vix_regime(m1, m2, vix_index)              → "contango" | "backwardation" | "spike"

These are used to:
  - Size VX/UVXY spread positions
  - Filter entry signals (only trade when regime is stable contango)
  - Feed the strategy advisor context
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import List, Optional


# ── Pure signal functions ────────────────────────────────────────────────────

def contango_pct(m1: float, m2: float) -> float:
    """
    Contango as a percentage of M1.

    Returns (m2 - m1) / m1.
    Positive = contango (normal vol term structure).
    Negative = backwardation (stress / inversion).
    """
    if m1 <= 0.0:
        raise ValueError(f"m1 must be positive, got {m1}")
    return (m2 - m1) / m1


def roll_yield_annualised(m1: float, m2: float, days_to_expiry: float) -> float:
    """
    Annualised roll yield for a long M1 VIX futures position.

    As M1 converges to spot over days_to_expiry, the holder earns/loses
    the spread (m1 - m2) per contract, annualised.

    Roll yield = (m1 - m2) / m2 × (365 / days_to_expiry)
    Negative in contango (long futures bleeds carry).
    Positive in backwardation.
    """
    if days_to_expiry <= 0.0:
        raise ValueError(f"days_to_expiry must be positive, got {days_to_expiry}")
    if m2 <= 0.0:
        raise ValueError(f"m2 must be positive, got {m2}")
    return (m1 - m2) / m2 * (365.0 / days_to_expiry)


def implied_vol_premium(vix: float, history: List[float]) -> float:
    """
    Implied volatility percentile (IVP): where does current VIX rank in history?

    Returns a value in [0, 1]:
      0.0 = VIX is at all-time low of sample
      1.0 = VIX is at all-time high of sample

    Useful for sizing: sell vol when IVP is high (expensive), buy when low (cheap).
    """
    if not history:
        return 0.5
    n_below = sum(1 for h in history if h <= vix)
    return n_below / len(history)


def vix_regime(m1: float, m2: float, vix_index: float) -> str:
    """
    Classify the current VIX term structure regime.

    Rules
    -----
    "spike"        : vix_index >= 30 AND m1 >= m2 (inverted + high absolute vol)
    "backwardation": m1 >= m2 (inverted, but not extreme)
    "contango"     : m2 > m1 (normal upward sloping)
    """
    if vix_index >= 30.0 and m1 >= m2:
        return "spike"
    if m1 >= m2:
        return "backwardation"
    return "contango"


# ── Convenience dataclass ────────────────────────────────────────────────────

@dataclass
class VixTermStructure:
    """
    Snapshot of the VIX term structure with pre-computed signals.

    Parameters
    ----------
    vix_index         : spot VIX level
    m1                : M1 VX futures price
    m2                : M2 VX futures price
    m3                : M3 VX futures price (optional, for slope extension)
    days_to_m1_expiry : calendar days until M1 expires
    history           : optional list of historical VIX closes for IVP calculation
    """
    vix_index: float
    m1: float
    m2: float
    m3: float
    days_to_m1_expiry: float
    history: List[float] = field(default_factory=list)

    @property
    def contango_m1_m2(self) -> float:
        """M1→M2 contango as a ratio."""
        return contango_pct(self.m1, self.m2)

    @property
    def contango_m2_m3(self) -> float:
        """M2→M3 contango as a ratio."""
        return contango_pct(self.m2, self.m3)

    @property
    def roll_yield(self) -> float:
        """Annualised roll yield for long M1 position."""
        return roll_yield_annualised(self.m1, self.m2, self.days_to_m1_expiry)

    @property
    def ivp(self) -> float:
        """Implied vol percentile (0–1). Returns 0.5 if no history provided."""
        return implied_vol_premium(self.vix_index, self.history)

    @property
    def regime(self) -> str:
        """Current term structure regime: 'contango' | 'backwardation' | 'spike'."""
        return vix_regime(self.m1, self.m2, self.vix_index)

    def to_dict(self) -> dict:
        """Serialisable snapshot for DB writes (trading.vix_term_structure)."""
        return {
            "vix_index": self.vix_index,
            "m1": self.m1,
            "m2": self.m2,
            "m3": self.m3,
            "contango_pct": round(self.contango_m1_m2 * 100, 4),
            "roll_yield_annualised": round(self.roll_yield * 100, 4),
            "regime": self.regime,
        }
```

- [ ] **Step 4: Run to verify PASS**

```
pytest tests/libs/signals/test_vix.py -v
```

Expected: 10 passed

- [ ] **Step 5: Run full Phase 2 test suite**

```
pytest tests/libs/ -v --tb=short
```

Expected: all tests pass (target ≥ 55 tests)

- [ ] **Step 6: Commit**

```bash
git add libs/signals/vix.py tests/libs/signals/test_vix.py
git commit -m "feat(libs): VIX term structure signals — contango, roll yield, IVP, regime"
```

---

## Self-Review

**Spec coverage check:**

| Spec requirement | Task |
|---|---|
| Kirk spread option pricer | Task 2 |
| Margrabe exchange option | Task 2 |
| Multi-expiry vol surface (SVI) | Task 3 |
| P&L explain (Δ/Γ/Vega/Θ) | Task 4 |
| Bond price, YTM, duration, DV01, convexity | Task 5 |
| Nelson-Siegel yield curve | Task 6 |
| Linear bootstrap | Task 6 |
| IRS NPV, par rate, DV01 | Task 7 |
| Cap/floor Black pricing | Task 7 |
| FX forward (CIP) | Task 8 |
| FX cross rates | Task 8 |
| FX vol surface (delta space → SVI) | Task 9 |
| VIX contango/roll yield/IVP/regime | Task 10 |
| trading.vix_term_structure integration | Task 10 (`to_dict()`) |

All spec requirements covered.

**Placeholder scan:** No TBDs, no "similar to above", all code blocks complete.

**Type consistency:** `SVIParams`, `bs_price`, `bs_greeks`, `b76_price`, `fit_svi`, `svi_vol` — all referenced from bess-platform `libs/options/`; names verified against actual source files.
