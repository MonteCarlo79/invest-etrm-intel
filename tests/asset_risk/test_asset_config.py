"""Tests for invoice-folder normalization in tab_asset_config."""
import pandas as pd
import pytest

from apps.asset_risk.tab_asset_config import _norm_folder


@pytest.mark.parametrize("val", [None, float("nan"), pd.NA, "", "   ", "nan", "NaN", "None", "NONE"])
def test_norm_folder_empty_variants(val):
    assert _norm_folder(val) is None


@pytest.mark.parametrize("val,expected", [
    ("B-8 内蒙杭锦旗", "B-8 内蒙杭锦旗"),
    ("  B-6 内蒙苏右  ", "B-6 内蒙苏右"),
])
def test_norm_folder_real_values(val, expected):
    assert _norm_folder(val) == expected
