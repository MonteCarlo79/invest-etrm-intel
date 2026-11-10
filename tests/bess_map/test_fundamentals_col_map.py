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
