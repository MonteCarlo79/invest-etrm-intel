"""S5 forecast model — exporter volume decomposition, importer merit-order stack,
spread-regression overlay. Pure functions; no DB. All inputs are plain dicts/lists
assembled by the tab from the staging tables and knowledge files.

Units: GWh for volumes, 元/MWh for prices. Horizon = one month (next month).
"""
from __future__ import annotations


def monthly_agreement_volume(agreements: list[dict], send: str, recv: str | None = None) -> float:
    """Agreement baseline for the month = Σ annual_gwh/12 for matching pairs."""
    total = 0.0
    for a in agreements:
        if a["send_prov"] != send:
            continue
        if recv is not None and a["recv_prov"] != recv:
            continue
        if a.get("annual_gwh"):
            total += a["annual_gwh"] / 12.0
    return round(total, 1)


def obligation_push_volume(mlt_gap_gwh: float | None) -> float:
    """MLT-gap volume seeking export this month. The gap is a stock concept
    (required − within − exported, GWh over the obligation period); exporters
    close it over the year → monthly push = gap ÷ remaining months, min 0."""
    if mlt_gap_gwh is None or mlt_gap_gwh <= 0:
        return 0.0
    return mlt_gap_gwh


def opportunistic_volume(spread: float | None, hist_max_monthly: float,
                         sensitivity: float = 1.0) -> float:
    """Spread-driven opportunistic export: only when export beats staying home.
    Linear in spread up to the historical-max monthly cap; sensitivity scales it."""
    if spread is None or spread <= 0 or hist_max_monthly <= 0:
        return 0.0
    frac = min(1.0, spread / 100.0 * sensitivity)   # 100元/MWh spread → full hist-max
    return round(hist_max_monthly * frac, 1)


def exporter_forecast(agreement: float, hist_baseline: float, obligation: float,
                      opportunistic: float, channel_cap: float | None,
                      hist_max: float | None) -> dict:
    """Combine drivers and apply caps.
    Baseline = max(agreement monthly rate, historical avg monthly export) — the
    stronger of commitment vs pattern (never double-counted).
    Total = baseline + obligation + opportunistic (discretionary add-ons).
    Caps: channel capability first (physical), then historical-max × 1.2."""
    baseline = max(agreement, hist_baseline)
    total = baseline + obligation + opportunistic
    capped_by = None
    for cap, name in ((channel_cap, "channel"), (hist_max * 1.2 if hist_max else None, "history")):
        if cap is not None and total > cap:
            total, capped_by = cap, name
    return dict(baseline=round(baseline, 1), agreement=round(agreement, 1),
                hist_baseline=round(hist_baseline, 1), obligation=round(obligation, 1),
                opportunistic=round(opportunistic, 1), total=round(total, 1),
                capped_by=capped_by)


def landed_cost(send_price: float | None, fee: float | None) -> float | None:
    """Marginal landed cost = 上网VWAP + 通道费VWAP."""
    if send_price is None:
        return None
    return round(send_price + (fee or 0.0), 1)


def importer_stack(candidates: list[dict], demand_gwh: float) -> dict:
    """Merit-order stack: sort candidates by landed_cost ascending (None cost last),
    fill demand from the cheapest. Each candidate: {send, cost, volume}.
    ALL candidates appear in the stack (zero-allocated after demand fills) so the
    full supply curve renders; demand line cuts through it.
    Returns inflow allocation + marginal price + unfilled demand."""
    order = sorted(candidates, key=lambda c: (c["cost"] is None, c["cost"] or 0))
    alloc, remaining = [], demand_gwh
    for c in order:
        take = min(remaining, max(c["volume"], 0.0)) if remaining > 0 else 0.0
        alloc.append(dict(send=c["send"], cost=c["cost"], available=round(c["volume"], 1),
                          allocated=round(take, 1)))
        remaining -= take
    filled = [a for a in alloc if a["allocated"] > 0]
    marginal = filled[-1]["cost"] if filled else None
    return dict(stack=alloc, demand=demand_gwh,
                inflow=round(sum(a["allocated"] for a in alloc), 1),
                marginal_price=marginal, unfilled=round(max(remaining, 0.0), 1))


def spread_regression(points: list[tuple[float, float]]) -> dict:
    """OLS of monthly export volume on realized spread: vol = a + b·spread.
    points = [(spread, vol), ...]. Needs n≥4 for a defensible line.
    Returns slope/intercept/r² + forecast for a given spread."""
    n = len(points)
    if n < 4:
        return dict(ok=False, n=n, reason="insufficient history")
    xs = [p[0] for p in points]; ys = [p[1] for p in points]
    mx, my = sum(xs)/n, sum(ys)/n
    sxx = sum((x-mx)**2 for x in xs)
    if sxx == 0:
        return dict(ok=False, n=n, reason="no spread variance")
    sxy = sum((x-mx)*(y-my) for x, y in points)
    b = sxy / sxx
    a = my - b * mx
    yhat = [a + b*x for x in xs]
    ss_res = sum((y-yh)**2 for y, yh in zip(ys, yhat))
    ss_tot = sum((y-my)**2 for y in ys)
    r2 = 1 - ss_res/ss_tot if ss_tot > 0 else 0.0
    return dict(ok=True, n=n, slope=round(b, 3), intercept=round(a, 2), r2=round(r2, 2),
                forecast=lambda s: max(0.0, a + b*s))
