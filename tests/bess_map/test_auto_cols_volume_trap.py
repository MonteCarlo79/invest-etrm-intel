# tests/bess_map/test_auto_cols_volume_trap.py
"""Regression: _guess_cols_from_header must not pick 出清电量 (volume) columns
as rt/da price. The 2026-09 LingFeng layout for 河北南网 added
'河北南网现货价格（元/MWh）-实时出清电量', whose name contains the '价格'
prefix substring — the longest-name tiebreak then preferred the VOLUME column
over '-实时价格', poisoning spot_prices_hourly.rt_price with MWh volumes."""
import sys
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "services" / "bess_map"))
from run_all_provinces import _guess_cols_from_header  # noqa: E402

# Header of the actual 2026-09-29 河北南网 download (price-related columns)
_NEW_LAYOUT_COLS = [
    "日期", "时点",
    "河北南网现货价格（元/MWh）-实时价格",
    "河北南网现货价格（元/MWh）-日前价格",
    "河北南网中长期分时均价（元/MWh）",
    "河北南网现货价格（元/MWh）-实时出清电量",
    "河北南网现货价格（元/MWh）-日前出清电量",
    "竞价空间（MW）-实时",
]


def _mock_header(cols):
    df0 = pd.DataFrame(columns=cols)
    return patch("pandas.read_excel", return_value=df0)


def test_rt_pick_ignores_volume_column():
    with _mock_header(_NEW_LAYOUT_COLS):
        rt, _ = _guess_cols_from_header(Path("河北南网.xlsx"), "河北南网")
    assert "电量" not in rt, f"picked volume column as rt: {rt}"
    assert rt == "河北南网现货价格（元/MWh）-实时价格"


def test_da_pick_ignores_volume_column():
    with _mock_header(_NEW_LAYOUT_COLS):
        _, da = _guess_cols_from_header(Path("河北南网.xlsx"), "河北南网")
    assert "电量" not in da, f"picked volume column as da: {da}"
    assert da == "河北南网现货价格（元/MWh）-日前价格"


def test_old_layout_still_picks_corrected_columns():
    """The Feb-2026 layout with 修正后 prefixes must keep working."""
    old_cols = [
        "日期", "时点",
        "河北南网现货价格（元/MWh）-修正后实时价格",
        "河北南网现货价格（元/MWh）-修正后日前价格",
        "河北南网中长期分时均价（元/MWh）",
        "省内负荷（MW）-实时",
    ]
    with _mock_header(old_cols):
        rt, da = _guess_cols_from_header(Path("河北南网.xlsx"), "河北南网")
    assert rt == "河北南网现货价格（元/MWh）-修正后实时价格"
    assert da == "河北南网现货价格（元/MWh）-修正后日前价格"


def test_volume_only_file_raises_not_silent():
    """If ONLY volume columns exist, better to fail loud than ingest volumes."""
    cols = ["日期", "时点", "河北南网现货价格（元/MWh）-实时出清电量"]
    with _mock_header(cols):
        with pytest.raises(KeyError):
            _guess_cols_from_header(Path("河北南网.xlsx"), "河北南网")
