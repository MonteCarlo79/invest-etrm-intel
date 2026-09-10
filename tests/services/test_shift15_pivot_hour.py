"""Tests for _shift15_pivot_hour hour-label normalization (all three copies).

Covers the 2025-10-25→2026-09-10 wide-table write failure: NaT timestamps made
pivot hour labels float (0.0), producing "Hour_0.0" columns that don't exist in
the wide tables (Hour_00..Hour_23) — UndefinedColumn aborted nightly writes.
"""
import re

import pandas as pd
import pytest

from services.data_ingestion import column_to_matrix_all as ctm
from services.loader import province_misc_to_db_v2 as pmisc
from services.common import focused_assets_data as fad

FUNCS = [
    pytest.param(ctm._shift15_pivot_hour, id="column_to_matrix_all"),
    pytest.param(pmisc._shift15_pivot_hour, id="province_misc_to_db_v2"),
    pytest.param(fad._shift15_pivot_hour, id="focused_assets_data"),
]


def _long(with_nat: bool = False) -> pd.DataFrame:
    rows = [
        {"time": pd.Timestamp("2026-09-05 00:15"), "metric": "actual", "price": 100.0},
        {"time": pd.Timestamp("2026-09-05 00:30"), "metric": "actual", "price": 140.0},
        {"time": pd.Timestamp("2026-09-05 01:15"), "metric": "actual", "price": 200.0},
    ]
    if with_nat:
        rows.append({"time": pd.NaT, "metric": "actual", "price": 999.0})
    return pd.DataFrame(rows)


_HOUR_LABEL = re.compile(r"^Hour_\d{2}$")


@pytest.mark.parametrize("fn", FUNCS)
def test_hour_labels_are_two_digit_ints_even_with_nat(fn):
    mat = fn(_long(with_nat=True))
    hour_cols = [c for c in mat.columns if c.startswith("Hour")]
    assert hour_cols, "expected Hour_* columns"
    for c in hour_cols:
        assert _HOUR_LABEL.match(c), f"bad hour label {c!r} (float-label regression)"
    assert "Hour_0.0" not in mat.columns


@pytest.mark.parametrize("fn", FUNCS)
def test_nat_rows_dropped_not_averaged(fn):
    mat = fn(_long(with_nat=True))
    row = mat[(mat["metric"] == "actual")]
    # 999.0 from the NaT row must not appear in any hourly cell
    hour_cols = [c for c in mat.columns if c.startswith("Hour")]
    assert not (row[hour_cols] == 999.0).any().any()


@pytest.mark.parametrize("fn", FUNCS)
def test_shift15_hour_assignment(fn):
    mat = fn(_long())
    row = mat[mat["metric"] == "actual"].iloc[0]
    # 00:15 shifts to hour 0 (mean of 100,140), 01:15 shifts to hour 1
    assert row["Hour_00"] == pytest.approx(120.0)
    assert row["Hour_01"] == pytest.approx(200.0)
