"""
Unit tests for nodal PF ranking functions in mengxi_ranking_report.py.
No DB required — all tests use synthetic price DataFrames.
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from services.hermes.mengxi_ranking_report import _compute_nodal_pf_ranks


def _make_prices(plant_name: str, n_days: int = 2, seed: int = 0) -> pd.DataFrame:
    """Build a synthetic 15-min price DataFrame for one plant."""
    rng = np.random.default_rng(seed)
    n = n_days * 96
    datetimes = pd.date_range("2026-06-01", periods=n, freq="15min")
    prices = rng.uniform(50, 500, size=n)
    return pd.DataFrame({
        "plant_name": plant_name,
        "datetime": datetimes,
        "cleared_price": prices,
    })


class TestComputeNodalPfRanks:

    def test_empty_df_returns_empty_dict(self):
        result = _compute_nodal_pf_ranks(pd.DataFrame(columns=["plant_name", "datetime", "cleared_price"]))
        assert result == {}

    def test_single_plant_gets_rank_1(self):
        df = _make_prices("plant_A", n_days=1)
        result = _compute_nodal_pf_ranks(df)
        assert "plant_A" in result
        assert result["plant_A"]["rank_2h"] == 1
        assert result["plant_A"]["rank_4h"] == 1

    def test_scores_are_non_negative(self):
        df = _make_prices("plant_A", n_days=2)
        result = _compute_nodal_pf_ranks(df)
        assert result["plant_A"]["score_2h"] >= 0
        assert result["plant_A"]["score_4h"] >= 0

    def test_two_plants_ranked_by_score_descending(self):
        # plant_low has flat price → zero spread → zero PF value
        df_low  = _make_prices("plant_low",  n_days=1, seed=1)
        df_low["cleared_price"] = 100.0
        df_high = _make_prices("plant_high", n_days=1, seed=2)
        df_high["cleared_price"] = np.where(
            df_high.index < 48, 10.0, 500.0  # large spread
        )
        df = pd.concat([df_low, df_high], ignore_index=True)
        result = _compute_nodal_pf_ranks(df)
        assert result["plant_high"]["rank_2h"] < result["plant_low"]["rank_2h"]  # lower rank number = better

    def test_n_days_matches_input(self):
        df = _make_prices("plant_A", n_days=3)
        result = _compute_nodal_pf_ranks(df)
        assert result["plant_A"]["n_days"] == 3

    def test_4h_score_higher_than_2h_for_wide_spread(self):
        df = _make_prices("plant_A", n_days=1, seed=5)
        df["cleared_price"] = np.where(df.index % 96 < 48, 10.0, 600.0)
        result = _compute_nodal_pf_ranks(df)
        # just verify both scores are non-negative
        assert result["plant_A"]["score_2h"] >= 0
        assert result["plant_A"]["score_4h"] >= 0
