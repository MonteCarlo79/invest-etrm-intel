"""Worked example for intrinsic_vs_extrinsic_value.

Four-hour toy: forward spreads, intrinsic, and expected value under
symmetric +/-20 uncertainty. Deterministic; no external data.
"""

SPREADS = [-10.0, 30.0, -5.0, 50.0]   # forward spark spreads, EUR/MWh
SHOCK = 20.0                           # symmetric 50/50 shock


def intrinsic(spreads) -> float:
    """Sum of positive forward spreads (switching model)."""
    return sum(max(s, 0.0) for s in spreads)


def expected_value(spreads, shock: float) -> float:
    """E[max(S+eps, 0)] with eps = +/- shock at 50/50."""
    return sum(0.5 * max(s + shock, 0.0) + 0.5 * max(s - shock, 0.0)
               for s in spreads)


def hourly_table(spreads, shock: float):
    rows = []
    for i, s in enumerate(spreads, 1):
        up = max(s + shock, 0.0)
        down = max(s - shock, 0.0)
        rows.append((i, s, 0.5 * (up + down), max(s, 0.0)))
    return rows


if __name__ == "__main__":
    v_int = intrinsic(SPREADS)
    v_tot = expected_value(SPREADS, SHOCK)
    for i, s, ev, iv in hourly_table(SPREADS, SHOCK):
        print(f"hour {i}: forward {s:+.0f}  intrinsic {iv:.1f}  E[value] {ev:.1f}")
    print(f"intrinsic = {v_int:.1f}  total = {v_tot:.1f}  extrinsic = {v_tot - v_int:.1f}")
