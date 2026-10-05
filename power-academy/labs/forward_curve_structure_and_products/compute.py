"""Worked example for forward_curve_structure_and_products.

Synthetic calendar (3 months x 10 days x 24h, weekdays only for peak),
base+peak quotes -> implied offpeak -> shaped hourly curve.
Every block must reprice its input quote. Deterministic.
"""

import numpy as np

BASE_Q = np.array([50.0, 55.0, 48.0])
PEAK_Q = np.array([70.0, 75.0, 65.0])
DAYS, PEAK_H = 10, range(8, 20)          # peak 08:00-19:59 = 12h/day (simplified)


def implied_offpeak(base=BASE_Q, peak=PEAK_Q):
    """F_op = 2*base - peak with a 12/12 hour split."""
    return 2.0 * base - peak


def is_peak_hour(month, day, hour):
    return hour in PEAK_H and (month * DAYS + day) % 7 not in (5, 6)   # no weekends


def build_curve(profile=None):
    """Hourly (3*DAYS*24,) curve repricing base and peak quotes."""
    n = 3 * DAYS * 24
    curve = np.zeros(n)
    profile = profile or {}
    for m in range(3):
        for d in range(DAYS):
            for h in range(24):
                i = m * DAYS * 24 + d * 24 + h
                w = profile.get(h, 1.0)
                if is_peak_hour(m, d, h):
                    curve[i] = PEAK_Q[m] * w
                else:
                    curve[i] = implied_offpeak()[m] * w
    return curve


def block_average(curve, month, kind):
    vals = [curve[m * DAYS * 24 + d * 24 + h]
            for m in [month] for d in range(DAYS) for h in range(24)
            if (kind == "peak") == is_peak_hour(m, d, h)]
    return float(np.mean(vals))


def normalised_profile(w):
    """Normalise an hour profile so its mean over peak hours is 1."""
    arr = np.array([w.get(h, 1.0) for h in PEAK_H])
    return {h: w.get(h, 1.0) / arr.mean() for h in range(24)}


if __name__ == "__main__":
    print("implied offpeak:", implied_offpeak())
    flat = build_curve()
    for m in range(3):
        print(f"month {m}: peak reprices {block_average(flat, m, 'peak'):.6f} "
          f"(quote {PEAK_Q[m]}), base { (block_average(flat, m, 'peak') + block_average(flat, m, 'offpeak')) / 2:.6f} (quote {BASE_Q[m]})")
    w = normalised_profile({8: 1.3, 9: 1.2, 12: 0.7, 13: 0.7, 18: 1.25, 19: 1.15})
    shaped = build_curve(profile=w)
    energy = sum(shaped[m * DAYS * 24 + d * 24 + h]
                 for m in range(3) for d in range(DAYS) for h in PEAK_H
                 if is_peak_hour(m, d, h))
    quote_energy = sum(PEAK_Q[m] * sum(is_peak_hour(m, d, h) for d in range(DAYS) for h in PEAK_H)
                       for m in range(3))
    print(f"shaped peak energy {energy:.2f} vs quote energy {quote_energy:.2f}")
