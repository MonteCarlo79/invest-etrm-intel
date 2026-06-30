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
            df_high["datetime"].dt.hour < 12, 10.0, 500.0  # morning low, afternoon high
        )
        df = pd.concat([df_low, df_high], ignore_index=True)
        result = _compute_nodal_pf_ranks(df)
        assert result["plant_high"]["rank_2h"] < result["plant_low"]["rank_2h"]  # lower rank number = better

    def test_n_days_matches_input(self):
        df = _make_prices("plant_A", n_days=3)
        result = _compute_nodal_pf_ranks(df)
        assert result["plant_A"]["n_days"] == 3

    def test_scores_non_negative_with_bimodal_prices(self):
        # Bimodal prices (low morning, high afternoon) — both 2h and 4h PF scores must be >= 0
        df = _make_prices("plant_A", n_days=1, seed=5)
        df["cleared_price"] = np.where(df.index % 96 < 48, 10.0, 600.0)
        result = _compute_nodal_pf_ranks(df)
        assert result["plant_A"]["score_2h"] >= 0
        assert result["plant_A"]["score_4h"] >= 0


from services.hermes.mengxi_ranking_report import _enrich_and_rank


def _raw_df() -> pd.DataFrame:
    return pd.DataFrame({
        "plant_name":    ["plant_A", "plant_B"],
        "discharge_rev": [50000.0,   30000.0],
        "charge_cost":   [10000.0,   8000.0],
        "discharge_mwh": [500.0,     300.0],
        "days":          [1,         1],
        "comp_yuan":     [175000.0,  105000.0],
        "max_energy":    [25.0,      25.0],
    })


def _plant_list() -> list[dict]:
    return [
        {"plant_name": "plant_A", "owner": "owner_X", "mw": 100},
        {"plant_name": "plant_B", "owner": "owner_Y", "mw": 100},
    ]


class TestEnrichAndRankNodalColumns:

    def test_nodal_rank_columns_present_when_ranks_provided(self):
        nodal_ranks = {
            "plant_A": {"rank_2h": 2, "rank_4h": 3, "score_2h": 10.0, "score_4h": 8.0, "n_days": 1},
            "plant_B": {"rank_2h": 1, "rank_4h": 1, "score_2h": 20.0, "score_4h": 18.0, "n_days": 1},
        }
        df = _enrich_and_rank(_raw_df(), _plant_list(), nodal_ranks=nodal_ranks)
        assert "nodal_rank_2h" in df.columns
        assert "nodal_rank_4h" in df.columns

    def test_nodal_rank_none_when_not_provided(self):
        df = _enrich_and_rank(_raw_df(), _plant_list(), nodal_ranks=None)
        assert df["nodal_rank_2h"].isna().all()
        assert df["nodal_rank_4h"].isna().all()

    def test_nodal_rank_values_match_input(self):
        nodal_ranks = {
            "plant_A": {"rank_2h": 5, "rank_4h": 7, "score_2h": 10.0, "score_4h": 8.0, "n_days": 1},
        }
        df = _enrich_and_rank(_raw_df(), _plant_list(), nodal_ranks=nodal_ranks)
        row_a = df[df["plant_name"] == "plant_A"].iloc[0]
        assert row_a["nodal_rank_2h"] == 5
        assert row_a["nodal_rank_4h"] == 7

    def test_missing_plant_nodal_rank_is_nan(self):
        # plant_B not in nodal_ranks → its nodal ranks should be NaN
        nodal_ranks = {
            "plant_A": {"rank_2h": 1, "rank_4h": 1, "score_2h": 10.0, "score_4h": 8.0, "n_days": 1},
        }
        df = _enrich_and_rank(_raw_df(), _plant_list(), nodal_ranks=nodal_ranks)
        row_b = df[df["plant_name"] == "plant_B"].iloc[0]
        assert pd.isna(row_b["nodal_rank_2h"])
        assert pd.isna(row_b["nodal_rank_4h"])
