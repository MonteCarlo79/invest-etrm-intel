"""Pure nodal-analysis functions for Mengxi BESS zone work.

All functions are DB-free and operate on 96-slot price vectors (numpy arrays,
may contain NaN for missing slots).
"""
from __future__ import annotations

from datetime import date

import numpy as np


def price_match_fraction(a: np.ndarray, b: np.ndarray, tol: float = 0.01) -> float:
    """Fraction of comparable slots where |a-b| <= tol. NaN slots are excluded."""
    a = np.asarray(a, dtype=float)
    b = np.asarray(b, dtype=float)
    valid = ~np.isnan(a) & ~np.isnan(b)
    n = int(valid.sum())
    if n == 0:
        return 0.0
    return float((np.abs(a - b) <= tol)[valid].sum() / n)


def detect_split_days(
    asset: dict[date, np.ndarray],
    parent: dict[date, np.ndarray],
    tol: float = 0.01,
    min_match: float = 0.99,
) -> list[date]:
    """Days where the asset's price diverges from the parent bus price
    (match fraction below min_match). Days missing on either side are skipped."""
    splits = []
    for d in sorted(asset):
        if d not in parent:
            continue
        if price_match_fraction(asset[d], parent[d], tol=tol) < min_match:
            splits.append(d)
    return splits


def _decimals(tol: float) -> int:
    return max(0, int(round(-np.log10(tol))))


def cluster_day_prices(
    matrix: dict[str, np.ndarray], tol: float = 0.01
) -> list[list[str]]:
    """Group nodes by identical rounded price vector. Returns clusters
    (lists of node names) sorted by size, largest first."""
    dec = _decimals(tol)
    groups: dict[tuple, list[str]] = {}
    for node, vec in matrix.items():
        r = np.round(np.asarray(vec, dtype=float), dec)
        key = tuple(np.where(np.isnan(r), -9e9, r))
        groups.setdefault(key, []).append(node)
    return sorted(groups.values(), key=len, reverse=True)


def assign_asset_clusters(
    matrix: dict[str, np.ndarray],
    assets: dict[str, np.ndarray],
    tol: float = 0.01,
    min_match: float = 0.99,
) -> dict[str, list[str]]:
    """For each asset price vector, the nodes whose vector matches it
    (fraction >= min_match). Empty list if no node matches."""
    out: dict[str, list[str]] = {}
    for name, vec in assets.items():
        out[name] = [node for node, nvec in matrix.items()
                     if price_match_fraction(vec, nvec, tol=tol) >= min_match]
    return out
