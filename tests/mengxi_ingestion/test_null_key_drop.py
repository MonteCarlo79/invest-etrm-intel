"""Tests for null-conflict-key row dropping in the mengxi Excel loaders.

Covers the 2026-09-10 md_id_cleared_energy freeze: 上湾电厂 rows with empty
时刻 produced null datetime keys, and the staging COPY's NotNullViolation
aborted the whole sheet-day (table stuck at 2026-08-28 while siblings advanced).
Both loader implementations (vendored pipeline + in-app upload path) must drop
such rows with a warning instead of failing the load.
"""
import pathlib
import sys

import pandas as pd
import pytest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[2]
                      / "bess-marketdata-ingestion" / "providers" / "mengxi"))

import load_excel_to_marketdata as vendored  # noqa: E402
from services.mengxi_ingestion import loader as app_loader  # noqa: E402

_KEYS = ["data_date", "datetime", "plant_name", "dispatch_unit_name"]


def _df() -> pd.DataFrame:
    return pd.DataFrame({
        "data_date": ["2026-08-26", "2026-08-26", "2026-08-26"],
        "datetime": [pd.Timestamp("2026-08-26 00:15"), pd.NaT, pd.Timestamp("2026-08-26 01:00")],
        "plant_name": ["好电厂", "上湾电厂", "好电厂"],
        "dispatch_unit_name": ["1#G", "1#G", "1#G"],
        "energy_mwh": [100.0, 98.8, 120.0],
    })


@pytest.mark.parametrize("drop", [
    pytest.param(lambda df, keys: vendored.drop_null_key_rows(df, keys, "md_id_cleared_energy"), id="vendored"),
    pytest.param(lambda df, keys: app_loader._drop_null_key_rows(df, keys, "md_id_cleared_energy"), id="app-loader"),
])
def test_nat_datetime_row_dropped_others_kept(drop, capsys):
    out = drop(_df(), _KEYS)
    assert len(out) == 2
    assert "上湾电厂" not in set(out["plant_name"])
    assert "DROP NULL-KEY" in capsys.readouterr().out


@pytest.mark.parametrize("drop", [
    pytest.param(lambda df, keys: vendored.drop_null_key_rows(df, keys, "md_id_cleared_energy"), id="vendored"),
    pytest.param(lambda df, keys: app_loader._drop_null_key_rows(df, keys, "md_id_cleared_energy"), id="app-loader"),
])
def test_empty_string_key_dropped(drop):
    df = _df()
    df["dispatch_unit_name"] = df["dispatch_unit_name"].astype(object)
    df.loc[0, "dispatch_unit_name"] = "  "
    out = drop(df, _KEYS)
    assert len(out) == 1  # row 0 ('' key) and row 1 (NaT) both dropped


@pytest.mark.parametrize("drop", [
    pytest.param(lambda df, keys: vendored.drop_null_key_rows(df, keys, "md_id_cleared_energy"), id="vendored"),
    pytest.param(lambda df, keys: app_loader._drop_null_key_rows(df, keys, "md_id_cleared_energy"), id="app-loader"),
])
def test_clean_df_unchanged(drop):
    df = _df().drop(index=1)
    out = drop(df, _KEYS)
    assert len(out) == 2


@pytest.mark.parametrize("drop", [
    pytest.param(lambda df, keys: vendored.drop_null_key_rows(df, keys, "md_id_cleared_energy"), id="vendored"),
    pytest.param(lambda df, keys: app_loader._drop_null_key_rows(df, keys, "md_id_cleared_energy"), id="app-loader"),
])
def test_all_bad_rows_returns_empty(drop):
    df = _df().iloc[[1]]
    out = drop(df, _KEYS)
    assert out.empty
