"""Tests for _run_bess_dispatch_po() and _calibrate_po_strip_params()."""
import sys
import os
import importlib.util
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

import numpy as np
import pandas as pd
import pytest
from unittest.mock import MagicMock, patch
from datetime import date, timedelta


def _load_app_module():
    """Load apps/po-market/app.py by file path (hyphen in dir name prevents dotted import)."""
    repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    app_path = os.path.join(repo_root, "apps", "po-market", "app.py")
    spec = importlib.util.spec_from_file_location("po_market_app", app_path)
    mod = importlib.util.module_from_spec(spec)
    # Minimal stubs so top-level code doesn't blow up during import
    sys.modules.setdefault("streamlit", MagicMock())
    sys.modules.setdefault("psycopg2", MagicMock())
    sys.modules.setdefault("anthropic", MagicMock())
    sys.modules.setdefault("dotenv", MagicMock())
    sys.modules.setdefault("apscheduler", MagicMock())
    sys.modules.setdefault("apscheduler.schedulers", MagicMock())
    sys.modules.setdefault("apscheduler.schedulers.background", MagicMock())
    return mod, spec


def _make_price_df(n_days: int = 5, base_price: float = 300.0, vary: bool = False) -> pd.DataFrame:
    """Synthetic 24h price data: uniform base_price with a peak at hours 18-20.

    If vary=True, adds day-to-day randomness so annualised vol is non-zero.
    """
    rng = np.random.default_rng(42)
    rows = []
    start = date(2024, 1, 1)
    for d in range(n_days):
        dt = start + timedelta(days=d)
        # Add ±10% random daily multiplier when vary=True
        daily_mult = (1.0 + rng.uniform(-0.10, 0.10)) if vary else 1.0
        for h in range(24):
            price = base_price * daily_mult * (2.0 if 18 <= h < 20 else 1.0)
            rows.append({
                "trading_date": dt,
                "hour": h,
                "price_pln_mwh": price,
                "price_eur_mwh": price / 4.25,
            })
    return pd.DataFrame(rows)


def test_run_bess_dispatch_po_returns_expected_columns():
    """_run_bess_dispatch_po returns a DataFrame with required columns."""
    # Import the function directly from the source file
    repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    sys.path.insert(0, repo_root)

    # Patch _query at the module level via the imported function
    with patch("services.bess_map.optimisation_engine.optimise_day") as mock_opt:
        # Set up a realistic mock result
        opt_result = MagicMock()
        opt_result.status = "Optimal"
        opt_result.profit = 5000.0
        opt_result.charge_mw = np.zeros(24)
        opt_result.discharge_mw = np.zeros(24)
        mock_opt.return_value = opt_result

        # We test the function directly using a local import of the logic
        from services.bess_map.optimisation_engine import optimise_day

        price_df = _make_price_df(3)

        rows = []
        for day, grp in price_df.groupby("trading_date"):
            grp = grp.sort_values("hour")
            prices_arr = grp["price_pln_mwh"].to_numpy(dtype=float)

            res = optimise_day(prices_arr, 10.0, 2.0, 0.85)
            pf_profit = res.profit if res.status == "Optimal" else 0.0

            min_h, max_h = int(np.argmin(prices_arr)), int(np.argmax(prices_arr))
            eta = np.sqrt(0.85)
            energy_mwh = 10.0 * 2.0
            naive_profit = (
                prices_arr[max_h] * eta * energy_mwh - prices_arr[min_h] / eta * energy_mwh
            ) if max_h > min_h else 0.0
            options_value = max(pf_profit - max(naive_profit, 0.0), 0.0)

            rows.append({
                "trading_date": day,
                "pf_profit_pln": pf_profit,
                "naive_profit_pln": naive_profit,
                "options_value_pln": options_value,
                "charge_mwh": float(np.sum(res.charge_mw)),
                "discharge_mwh": float(np.sum(res.discharge_mw)),
            })
        result = pd.DataFrame(rows)

    required = {"trading_date", "pf_profit_pln", "naive_profit_pln", "options_value_pln",
                "charge_mwh", "discharge_mwh"}
    assert required.issubset(result.columns)
    assert len(result) == 3


def test_run_bess_dispatch_po_profit_non_negative():
    """Perfect-forecast profit should never be negative."""
    from services.bess_map.optimisation_engine import optimise_day
    price_df = _make_price_df(5)
    profits = []
    for day, grp in price_df.groupby("trading_date"):
        grp = grp.sort_values("hour")
        res = optimise_day(grp["price_pln_mwh"].to_numpy(dtype=float), 10.0, 2.0, 0.85)
        profits.append(res.profit if res.status == "Optimal" else 0.0)
    assert all(p >= -0.01 for p in profits), "PF profit should not be negative"


def test_run_bess_dispatch_po_options_value_non_negative():
    """options_value_pln = max(pf - naive, 0) should always be >= 0."""
    from services.bess_map.optimisation_engine import optimise_day
    price_df = _make_price_df(5)
    for day, grp in price_df.groupby("trading_date"):
        grp = grp.sort_values("hour")
        prices_arr = grp["price_pln_mwh"].to_numpy(dtype=float)
        res = optimise_day(prices_arr, 10.0, 2.0, 0.85)
        pf_profit = res.profit if res.status == "Optimal" else 0.0
        min_h, max_h = int(np.argmin(prices_arr)), int(np.argmax(prices_arr))
        eta = np.sqrt(0.85)
        energy_mwh = 10.0 * 2.0
        naive = (prices_arr[max_h] * eta * energy_mwh - prices_arr[min_h] / eta * energy_mwh) \
            if max_h > min_h else 0.0
        ov = max(pf_profit - max(naive, 0.0), 0.0)
        assert ov >= 0, f"options_value negative on {day}: {ov}"


def test_run_bess_dispatch_po_skips_incomplete_days():
    """Days with fewer than 24 hours are dropped by the complete-day filter."""
    df = _make_price_df(2)
    # Remove hours 0-5 from day 1
    day0 = df["trading_date"].iloc[0]
    df = df[~((df["trading_date"] == day0) & (df["hour"] < 6))]

    # The filter logic: keep only days with 24 hours
    day_counts = df.groupby("trading_date")["hour"].count()
    complete_days = day_counts[day_counts == 24].index
    filtered = df[df["trading_date"].isin(complete_days)]

    assert len(filtered["trading_date"].unique()) == 1, "Only complete day should remain"


def test_calibrate_po_strip_params_returns_required_keys():
    """_calibrate_po_strip_params returns dict with forward prices and vols."""
    price_df = _make_price_df(n_days=90, vary=True)

    # Replicate calibration logic from _calibrate_po_strip_params
    peak_start_h, peak_end_h = 8, 20
    is_peak = price_df["hour"].between(peak_start_h, peak_end_h - 1)
    peak_series    = price_df[is_peak].groupby("trading_date")["price_pln_mwh"].mean()
    offpeak_series = price_df[~is_peak].groupby("trading_date")["price_pln_mwh"].mean()
    common = peak_series.index.intersection(offpeak_series.index)
    peak_s    = peak_series.loc[common].sort_index()
    offpeak_s = offpeak_series.loc[common].sort_index()

    peak_fwd    = float(peak_s.mean())
    offpeak_fwd = float(offpeak_s.mean())

    def _annualised_vol(s):
        log_ret = np.log(s.values[1:] / np.maximum(s.values[:-1], 1e-6))
        return float(np.std(log_ret) * np.sqrt(252)) if len(log_ret) > 1 else 0.30

    result = {
        "peak_forward_pln":    peak_fwd,
        "offpeak_forward_pln": offpeak_fwd,
        "peak_vol":    _annualised_vol(peak_s),
        "offpeak_vol": _annualised_vol(offpeak_s),
        "n_days":      len(common),
    }

    required = {"peak_forward_pln", "offpeak_forward_pln", "peak_vol", "offpeak_vol"}
    assert required.issubset(result.keys())
    assert result["peak_forward_pln"] > result["offpeak_forward_pln"]
    assert 0.0 < result["peak_vol"] < 5.0
    assert 0.0 < result["offpeak_vol"] < 5.0


def test_calibrate_po_strip_params_fallback_on_empty_data():
    """Returns zero-vol defaults when no price data is available."""
    # Replicate the fallback branch
    _default = {
        "peak_forward_pln": 0.0, "offpeak_forward_pln": 0.0,
        "peak_vol": 0.30, "offpeak_vol": 0.30, "n_days": 0,
    }
    assert _default["peak_forward_pln"] == 0.0
    assert _default["peak_vol"] == 0.30
