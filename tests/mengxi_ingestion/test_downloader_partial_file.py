"""Downloader keeps a day when any core sheet has data (2026-11-03: empty
实时节点电价 but complete DA/ID clearing was rejected wholesale)."""
import pathlib
import sys

import pandas as pd

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[2]
                      / "bess-marketdata-ingestion" / "providers" / "mengxi"))

import batch_downloader as bd  # noqa: E402


def _write(path, rows_by_sheet):
    with pd.ExcelWriter(path) as w:
        for sheet, n in rows_by_sheet.items():
            pd.DataFrame({"a": range(n)}).to_excel(w, sheet_name=sheet, index=False)


def test_rt_sheet_empty_but_clearing_present_is_kept(tmp_path):
    f = tmp_path / "data_2026-11-03.xlsx"
    _write(f, {"实时节点电价": 0, "日内各时段出清电量": 120, "日前各时段出清电量": 120})
    assert bd.excel_contains_data(f) is True


def test_unpublished_template_rejected(tmp_path):
    f = tmp_path / "data_2026-11-08.xlsx"
    _write(f, {"实时节点电价": 0, "日内各时段出清电量": 0, "日前各时段出清电量": 0})
    assert bd.excel_contains_data(f) is False


def test_full_file_kept(tmp_path):
    f = tmp_path / "data_2026-11-04.xlsx"
    _write(f, {"实时节点电价": 200, "日内各时段出清电量": 200, "日前各时段出清电量": 200})
    assert bd.excel_contains_data(f) is True
