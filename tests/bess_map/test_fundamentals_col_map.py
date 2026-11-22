"""Every detected fundamentals column must land in a real DB column.

Regression: ("renewable", "D-1") mapped to renewable_total_d1_mw, which exists
neither in the DDL nor in the INSERT column list, so renewable D-1/D-2/D-3 were
silently never written.
"""
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from services.bess_map import run_fundamentals_ingest as m  # noqa: E402


def _ddl_cols() -> set[str]:
    return set(re.findall(r"^\s+(\w+_mw)\s+DOUBLE PRECISION", m._DDL, flags=re.M))


def test_full_map_targets_exist_in_ddl():
    missing = set(m._FULL_MAP.values()) - _ddl_cols()
    assert not missing, f"detected columns with no DDL column: {sorted(missing)}"


def test_full_map_targets_are_inserted():
    missing = set(m._FULL_MAP.values()) - set(m._ALL_DB_COLS)
    assert not missing, f"detected columns never inserted: {sorted(missing)}"


def test_renewable_mapping():
    assert m._FULL_MAP[("renewable", "实时")] == "renewable_total_mw"
    assert m._FULL_MAP[("renewable", "D-1")] == "renewable_d1_mw"
    assert m._FULL_MAP[("renewable", "D-3")] == "renewable_d3_mw"


# Real header row of a 山东 export downloaded 2026-11-22 (2026-09+ LingFeng layout).
_SHANDONG_HEADERS = [
    "日期", "时点", "山东全省现货价格（元/MWh）-实时价格", "山东全省现货价格（元/MWh）-日前价格",
    "山东全省现货价格（元/MWh）-中长期结算点电价",
    "省内负荷（MW）-实时", "省内负荷（MW）-电网D-1预测", "省内负荷（MW）-电网D-2预测", "省内负荷（MW）-临时数据",
    "联络线-实时", "联络线-电网D-1预测", "联络线-电网D-2预测",
    "新能源（总出力）-实时", "新能源（总出力）-电网D-1预测", "新能源（总出力）-电网D-2预测", "新能源（总出力）-临时数据",
    "竞价空间（MW）-实时", "竞价空间（MW）-电网D-1预测", "竞价空间（MW）-电网D-2预测",
    "省内负荷&联络线-实时", "省内负荷&联络线-电网D-1预测", "省内负荷&联络线-电网D-2预测",
    "新能源（风电）-实时", "新能源（风电）-电网D-1预测", "新能源（风电）-电网D-2预测", "新能源（风电）-临时数据",
    "新能源（光伏）-实时", "新能源（光伏）-电网D-1预测", "新能源（光伏）-电网D-2预测", "新能源（光伏）-临时数据",
    "备用信息（MW）-实际上旋备用", "备用信息（MW）-实际下旋备用",
    "省内负荷-新能源-实时", "省内负荷-新能源-电网D-1预测", "省内负荷-新能源-电网D-2预测", "省内负荷-新能源-临时数据",
    "水电（含抽蓄）-实时", "水电（含抽蓄）-电网D-1预测", "水电（含抽蓄）-电网D-2预测", "水电（含抽蓄）-临时数据",
]


def test_bracketed_renewable_total_headers_detected(tmp_path):
    import pandas as pd
    f = tmp_path / "山东.xlsx"
    pd.DataFrame(columns=_SHANDONG_HEADERS).to_excel(f, index=False)
    cm = m._detect_fundamentals_cols(f)
    assert cm["renewable_total_mw"] == "新能源（总出力）-实时"
    assert cm["renewable_d1_mw"] == "新能源（总出力）-电网D-1预测"
    assert cm["renewable_d2_mw"] == "新能源（总出力）-电网D-2预测"
    # unchanged mappings
    assert cm["load_d1_mw"] == "省内负荷（MW）-电网D-1预测"
    assert cm["wind_d1_mw"] == "新能源（风电）-电网D-1预测"
    assert cm["solar_mw"] == "新能源（光伏）-实时"
    assert cm["bidding_space_d1_mw"] == "竞价空间（MW）-电网D-1预测"
    assert not any("价格" in v for v in cm.values())
