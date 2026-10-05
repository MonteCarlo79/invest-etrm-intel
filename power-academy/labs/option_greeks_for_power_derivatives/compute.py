"""Worked example for option_greeks_for_power_derivatives.

Numeric Greeks of a spark-spread (Margrabe) option: delta (both legs),
gamma, vega — by analytic formula cross-checked against finite differences
of the same pricer. Deterministic. Reuses margrabe from the valuation lab.
"""

import sys
from pathlib import Path

import numpy as np
from scipy.stats import norm

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from ..spread_option_pricing_models.compute import margrabe

F1, F2, S1, S2, RHO, T, DF = 80.0, 60.0, 0.50, 0.30, 0.40, 0.25, 0.99


def analytic_deltas(f1=F1, f2=F2, s1=S1, s2=S2, rho=RHO, t=T, df=DF):
    _, d1, d2 = margrabe(f1, f2, s1, s2, rho, t, df)
    return df * norm.cdf(d1), -df * norm.cdf(d2)


def analytic_gamma(f1=F1, f2=F2, s1=S1, s2=S2, rho=RHO, t=T, df=DF):
    """dDelta1/dF1 = DF * pdf(d1) / (F1 * sig * sqrt(t))."""
    import math
    sig = math.sqrt(s1 ** 2 + s2 ** 2 - 2 * rho * s1 * s2)
    _, d1, _ = margrabe(f1, f2, s1, s2, rho, t, df)
    return df * norm.pdf(d1) / (f1 * sig * math.sqrt(t))


def analytic_vega(f1=F1, f2=F2, s1=S1, s2=S2, rho=RHO, t=T, df=DF):
    """dV/d(sig_spread) = DF * F1 * pdf(d1) * sqrt(t)."""
    import math
    _, d1, _ = margrabe(f1, f2, s1, s2, rho, t, df)
    return df * f1 * norm.pdf(d1) * math.sqrt(t)


def fd(f, x, h, **kw):
    return (f(x + h, **kw) - f(x - h, **kw)) / (2 * h)


def _v(f1, f2, s1, s2):
    return margrabe(f1, f2, s1, s2, RHO, T, DF)[0]


def fd_vega(h=0.001):
    """dV/d(spread vol) via proportional scaling of both legs' vols."""
    import math
    sig_spread = math.sqrt(S1 ** 2 + S2 ** 2 - 2 * RHO * S1 * S2)

    def v(scale):
        return margrabe(F1, F2, S1 * scale, S2 * scale, RHO, T, DF)[0]
    return (v(1 + h) - v(1 - h)) / (2 * h * sig_spread)


def fd_gamma(h=0.01):
    return (fd(lambda x: _v(x + h / 2, F2, S1, S2), F1, h) -
            fd(lambda x: _v(x - h / 2, F2, S1, S2), F1, h)) / h


if __name__ == "__main__":
    da = analytic_deltas()
    v, _, _ = margrabe(F1, F2, S1, S2, RHO, T, DF)
    d1_fd = fd(lambda x: _v(x, F2, S1, S2), F1, 0.01)
    d2_fd = fd(lambda x: _v(F1, x, S1, S2), F2, 0.01)
    print(f"Margrabe value = {v:.3f}")
    print(f"delta1 analytic {da[0]:.4f} vs fd {d1_fd:.4f}")
    print(f"delta2 analytic {da[1]:.4f} vs fd {d2_fd:.4f}")
    print(f"gamma  analytic {analytic_gamma():.6f} vs fd {fd_gamma():.6f}")
    print(f"vega   analytic {analytic_vega():.3f} vs fd {fd_vega():.3f} (per unit spread vol)")
