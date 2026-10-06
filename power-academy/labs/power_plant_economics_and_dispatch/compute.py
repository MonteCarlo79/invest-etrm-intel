"""Worked example for power_plant_economics_and_dispatch.

24h toy: 100 MW unit, K=55 EUR/MWh, start cost 5000 EUR.
Run/stop rule with and without start economics. Deterministic.
"""

MEL, K, START = 100.0, 55.0, 5000.0
PRICES = ([40.0] * 6 + [58.0] * 3 + [48.0] * 6 + [75.0] * 6 + [52.0] * 3)


def run_hours_no_constraints(prices=PRICES, k=K):
    return [p > k for p in prices]


def margin_no_constraints(prices=PRICES, k=K, mel=MEL):
    return sum(mel * max(p - k, 0.0) for p in prices)


def blocks(prices=PRICES):
    """Contiguous above-K blocks with their margins."""
    out, cur = [], None
    for i, p in enumerate(prices):
        if p > K:
            if cur is None:
                cur = [i, i, 0.0]
            cur[1] = i
            cur[2] += MEL * (p - K)
        elif cur is not None:
            out.append(tuple(cur))
            cur = None
    if cur is not None:
        out.append(tuple(cur))
    return out


def optimal_schedule(prices=PRICES, start_cost=START):
    """Chain blocks through a dip if the dip loss beats a second start."""
    bl = blocks(prices)
    if not bl:
        return [0] * len(prices), 0.0, 0
    sched = [0] * len(prices)

    def value(runs):
        margin = 0.0
        for (b, e) in runs:
            margin += sum(MEL * (p - K) for p in prices[b:e + 1]) - start_cost
        return margin

    options = [[]]
    for i in range(len(bl)):                       # each block alone
        options.append([(bl[i][0], bl[i][1])])
    for i in range(len(bl) - 1):                   # adjacent pairs chained through the dip
        options.append([(bl[i][0], bl[i + 1][1])])
    runs = max(options, key=value)
    for (b, e) in runs:
        for i in range(b, e + 1):
            sched[i] = 1
    return sched, value(runs), len(runs)


if __name__ == "__main__":
    bl = blocks()
    print("blocks (start_h, end_h, margin):", [(b, e, round(m)) for b, e, m in bl])
    sched, margin, nstarts = optimal_schedule()
    dip_loss = sum(MEL * (K - p) for p in PRICES[9:15])
    print(f"dip loss hours 9-14: {dip_loss:.0f} EUR  (vs second start {START:.0f})")
    print(f"run hours: {sum(sched)}  starts: {nstarts}  margin net of starts: {margin:.0f} EUR")
    extend = bl[0][2] - dip_loss
    print(f"extend evening run back through dip? extra margin {extend:.0f} EUR (negative -> no)")
