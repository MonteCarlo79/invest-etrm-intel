# tests/bess_map/test_folder_scan_guard.py
"""Folder-scan guard: --indir ingestion must skip files whose stem is not a
known province (2026-09-29 运行数据披露 incident — the LingFeng export for
河北南网 lands as 运行数据披露-_<dates>.xlsx, a UI section name, and the scan
ingested it as a province into spot_prices_hourly)."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "services" / "bess_map"))
import run_all_provinces as rap  # noqa: E402


def test_guard_set_covers_all_markets():
    expected = {
        "河南", "新疆", "吉林", "海南", "湖北", "四川", "黑龙江", "福建",
        "浙江", "江苏", "广西", "安徽", "陕西", "贵州", "云南", "广东",
        "蒙东", "湖南", "宁夏", "辽宁", "河北南网", "甘肃", "蒙西", "山东",
        "山西", "冀北", "青海", "江西",
    }
    assert expected <= rap._KNOWN_PROVINCES


def test_section_names_rejected():
    assert "运行数据披露" not in rap._KNOWN_PROVINCES
    assert "数据咨询" not in rap._KNOWN_PROVINCES


def test_stem_variants_resolve_to_known():
    """Every legitimate filename shape resolves INTO the guard set."""
    for stem, want in [("4.山西", "山西"), ("1 蒙西", "蒙西"), ("01_甘肃", "甘肃"),
                       ("2-广东", "广东"), ("河北南网", "河北南网"), ("蒙东", "蒙东"),
                       ("山东_市场供需数据_2026-01-01_2026-01-30", "山东"),
                       ("河北南网市场供需数据_2025-01-01_2025-01-30", "河北南网"),
                       ("甘肃西河", "甘肃西河")]:
        prov = rap._resolve_province(rap._clean_province_from_stem(stem))
        assert prov == want, f"{stem} → {prov}, want {want}"


def test_section_stem_rejected():
    prov = rap._resolve_province(
        rap._clean_province_from_stem("运行数据披露-_2025-01-01_2025-01-30"))
    assert prov is None


def test_loop_skip_logic_in_source():
    src = Path(rap.__file__).read_text(encoding="utf-8")
    assert "[GUARD]" in src and "_KNOWN_PROVINCES" in src
