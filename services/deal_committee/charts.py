"""Matplotlib PNG charts for the DAF PDF. Axis labels in English (no CJK font in slim images)."""
from __future__ import annotations

from io import BytesIO

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np


def _png(fig) -> bytes:
    buf = BytesIO()
    fig.savefig(buf, format="png", dpi=150, bbox_inches="tight")
    plt.close(fig)
    return buf.getvalue()


def chart_monthly_price(monthly_price: list[tuple[str, float]]) -> bytes:
    if not monthly_price:
        raise ValueError("monthly_price is empty")
    months, vals = zip(*monthly_price)
    fig, ax = plt.subplots(figsize=(8, 3.2))
    ax.plot(months, vals, marker="o", color="#1f77b4", lw=1.8)
    ax.set_ylabel("¥/MWh")
    ax.set_title("Monthly Average DA Price — Last 12 Months")
    ax.grid(alpha=0.3)
    plt.setp(ax.get_xticklabels(), rotation=45, ha="right")
    return _png(fig)


def chart_revenue_distribution(revenue_paths) -> bytes:
    arr = np.asarray(revenue_paths, dtype=float)
    if arr.size == 0:
        raise ValueError("revenue_paths is empty")
    p10, p50, p90 = np.percentile(arr, [10, 50, 90])
    fig, ax = plt.subplots(figsize=(8, 3.2))
    ax.hist(arr / 1e6, bins=40, color="#2ca02c", alpha=0.75)
    for v, c, lbl in ((p10, "#d62728", "P10"), (p50, "#1f77b4", "P50"), (p90, "#9467bd", "P90")):
        ax.axvline(v / 1e6, color=c, ls="--", lw=1.5, label=f"{lbl} ¥{v/1e6:.1f}M")
    ax.set_xlabel("Annual Revenue (¥M)")
    ax.set_ylabel("Frequency")
    ax.set_title("Annual Revenue Distribution")
    ax.legend()
    return _png(fig)


def chart_irr_distribution(equity_irr_paths, hurdle_rate: float = 0.08) -> bytes:
    arr = np.asarray(equity_irr_paths, dtype=float)
    if arr.size == 0:
        raise ValueError("equity_irr_paths is empty")
    p10, p50, p90 = np.percentile(arr, [10, 50, 90])
    fig, ax = plt.subplots(figsize=(8, 3.2))
    ax.hist(arr * 100, bins=40, color="#ff7f0e", alpha=0.75)
    ax.axvline(hurdle_rate * 100, color="#d62728", lw=2, label=f"Hurdle {hurdle_rate:.0%}")
    for v, lbl in ((p10, "P10"), (p50, "P50"), (p90, "P90")):
        ax.axvline(v * 100, color="#1f77b4", ls="--", lw=1.2, label=f"{lbl} {v:.1%}")
    ax.set_xlabel("Equity IRR (%)")
    ax.set_ylabel("Frequency")
    ax.set_title("Equity IRR Distribution")
    ax.legend(fontsize=8)
    return _png(fig)
