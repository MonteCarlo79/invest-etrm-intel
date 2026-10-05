"""Worked example for real_options_framework_for_generation_assets.

Dixit-Pindyck perpetual build option (closed form) + CRR binomial tree
for the finite-horizon American version. Deterministic; no external data.
Units: MEUR, years.
"""

import math

I = 100.0          # build cost, MEUR
V0 = 100.0         # current project value, MEUR
SIGMA = 0.25
R = 0.05
RN_DRIFT = -0.04   # risk-neutral drift (r - payout yield)


def beta1(sigma=SIGMA, r=R, rn_drift=RN_DRIFT) -> float:
    a = 0.5 - rn_drift / sigma ** 2
    return a + math.sqrt(a ** 2 + 2 * r / sigma ** 2)


def wait_multiplier(sigma=SIGMA, r=R, rn_drift=RN_DRIFT) -> float:
    b = beta1(sigma, r, rn_drift)
    return b / (b - 1.0)


def build_trigger(cost: float = I, **kw) -> float:
    return wait_multiplier(**kw) * cost


def perpetual_value(v: float, cost: float = I, **kw) -> float:
    """Value of the perpetual option to build at project value v."""
    vs = build_trigger(cost, **kw)
    if v >= vs:
        return v - cost
    return (vs - cost) * (v / vs) ** beta1(**kw)


def tree_value(v: float = V0, cost: float = I, T: float = 3.0, n: int = 300,
               sigma=SIGMA, r=R, rn_drift=RN_DRIFT) -> float:
    """CRR binomial tree for the American option to build within T years."""
    dt = T / n
    u = math.exp(sigma * math.sqrt(dt))
    d = 1.0 / u
    p = (math.exp(rn_drift * dt) - d) / (u - d)
    if not 0.0 < p < 1.0:
        raise ValueError(f"tree probability out of bounds: p={p}")
    disc = math.exp(-r * dt)
    # terminal payoffs
    level = [max(v * u ** j * d ** (n - j) - cost, 0.0) for j in range(n + 1)]
    for step in range(n - 1, -1, -1):
        for j in range(step + 1):
            cont = disc * (p * level[j + 1] + (1 - p) * level[j])
            v_node = v * u ** j * d ** (step - j)
            level[j] = max(cont, v_node - cost)
    return level[0]


if __name__ == "__main__":
    b = beta1()
    print(f"beta1            = {b:.3f}")
    print(f"wait multiplier  = {wait_multiplier():.3f}")
    print(f"build trigger V* = {build_trigger():.1f} MEUR")
    print(f"perpetual option value at V=100: {perpetual_value(100):.2f} MEUR")
    for T in (0.5, 1.0, 3.0, 5.0):
        n = max(int(T * 200), 50)
        print(f"tree value T={T:.1f}y (n={n}): {tree_value(T=T, n=n):.2f} MEUR")
