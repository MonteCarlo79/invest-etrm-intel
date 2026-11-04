# tests/province_fundamentals/test_registry.py
"""Registry dispatch tests — province detection from path, format sniff, []+log
fallback for anything unknown (intake falls back to vision per spec)."""
from datetime import date
from pathlib import Path

import openpyxl
import pytest

from services.province_fundamentals import registry
from services.province_fundamentals.models import FundRow

FIXTURE = (Path(__file__).parent / "fixtures"
           / "安徽电力市场信息披露月报数据-截至2026年5月.xlsx")
FIXTURE_B = (Path(__file__).parent / "fixtures" / "2-安徽装机-7月.xlsx")
FIXTURE_JIAOJIE = (Path(__file__).parent / "fixtures"
                   / "2025年1-12月各省分类型利用小时.xlsx")


def test_known_formats_lists_format_a():
    assert "format_a" in registry.known_formats()
    assert "format_b" in registry.known_formats()


def test_parse_file_dispatches_anhui_fixture():
    """省份从文件名嗅探 ("安徽…月报数据….xlsx"), 格式由 parser.sniff 确认."""
    rows = registry.parse_file(FIXTURE)
    assert rows, "fixture must parse non-empty"
    assert {r.province for r in rows} == {"安徽"}
    by = {(r.month, r.metric): r.value for r in rows}
    assert by[(date(2025, 1, 1), "cap_total")] == pytest.approx(6831.49)


def test_parse_file_dispatches_anhui_format_b_fixture():
    """同省多格式: 装机-月工作簿 (格式 B) 不得落到格式 A — sniff 依次探, 首个命中者解析."""
    rows = registry.parse_file(FIXTURE_B)
    assert rows, "format B fixture must parse non-empty"
    assert {r.province for r in rows} == {"安徽"}
    by = {(r.month, r.metric): r.value for r in rows}
    assert by[(date(2025, 1, 1), "cap_total_full")] == pytest.approx(12635.64)
    assert (date(2025, 1, 1), "cap_total") not in by   # 未误派格式 A


def test_parse_file_unknown_format_returns_empty_and_logs(tmp_path, caplog):
    """已注册省份 (安徽在文件名里) 但工作簿结构不命中 → [] + log, 不抛."""
    wb = openpyxl.Workbook()
    wb.active.title = "sheet1"
    wb.active.append(["x"])
    p = tmp_path / "安徽-乱七八糟.xlsx"
    wb.save(p)
    with caplog.at_level("INFO", logger="services.province_fundamentals.registry"):
        assert registry.parse_file(p) == []
    assert any("安徽" in r.getMessage() for r in caplog.records)


def test_parse_file_unregistered_province_returns_empty(tmp_path):
    """格式再像, 省份未注册也不派发."""
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "装机及发电情况"
    ws.append(["月份", "省调口径装机总容量(万千瓦)"])
    ws.append(["2025-01", 1.0])
    p = tmp_path / "火星省电力市场信息披露月报数据.xlsx"
    wb.save(p)
    assert registry.parse_file(p) == []


def test_parse_file_missing_file_returns_empty(tmp_path):
    assert registry.parse_file(tmp_path / "安徽-不存在.xlsx") == []


def test_parse_file_non_workbook_returns_empty(tmp_path):
    p = tmp_path / "安徽月报.txt"
    p.write_text("not a workbook", encoding="utf-8")
    assert registry.parse_file(p) == []


def test_register_custom_parser(tmp_path, monkeypatch):
    """register(province, parser) — parser 鸭子类型: .parse/.sniff/.FORMAT_NAME."""
    monkeypatch.setattr(registry, "_REGISTRY", dict(registry._REGISTRY))

    class FakeParser:
        FORMAT_NAME = "format_test"

        @staticmethod
        def sniff(path):
            return True

        @staticmethod
        def parse(path, province):
            return [FundRow(province=province, month=date(2025, 1, 1),
                            period="month", section="capacity",
                            metric="cap_total", value=1.0,
                            unit="万千瓦", source="fake")]

    registry.register("测试省", FakeParser)
    assert "format_test" in registry.known_formats()
    rows = registry.parse_file(tmp_path / "测试省-月报.xlsx")
    assert [r.value for r in rows] == [1.0]
    assert rows[0].province == "测试省"


def test_detect_province_longest_name_wins(monkeypatch):
    """河北南网 路径不得被 河北 截胡 (若两者都注册)."""
    monkeypatch.setattr(registry, "_REGISTRY", dict(registry._REGISTRY))

    class _P:
        FORMAT_NAME = "format_test"

        @staticmethod
        def sniff(path):
            return True

        @staticmethod
        def parse(path, province):
            return [FundRow(province=province, month=date(2025, 1, 1),
                            period="month", section="capacity",
                            metric="cap_total", value=1.0,
                            unit="万千瓦", source="fake")]

    registry.register("河北", _P)
    registry.register("河北南网", _P)
    rows = registry.parse_file(Path("/x/河北南网月报数据.xlsx"))
    assert rows[0].province == "河北南网"


# ── dispatch() + jiaojie 全国册注册 (task 5) ────────────────────────────────────

def test_dispatch_returns_format_name_and_rows():
    """dispatch = parse_file + 命中格式名 (runner dry-run 判定要打印格式)."""
    fmt, rows = registry.dispatch(FIXTURE)
    assert fmt == "format_a"
    assert rows, "fixture must parse non-empty"


def test_dispatch_unknown_returns_none_format(tmp_path):
    wb = openpyxl.Workbook()
    wb.active.append(["x"])
    p = tmp_path / "乱七八糟.xlsx"
    wb.save(p)
    fmt, rows = registry.dispatch(p)
    assert fmt is None
    assert rows == []


def test_jiaojie_registered_as_national_parser():
    """交接表全国册: 路径无省份名, 仍须命中 (task-5 brief: 注册进 registry)."""
    assert "jiaojie_2025" in registry.known_formats()
    fmt, rows = registry.dispatch(FIXTURE_JIAOJIE)
    assert fmt == "jiaojie_2025"
    provs = {r.province for r in rows}
    assert "山东" in provs and "安徽" in provs


def test_jiaojie_sniff_rejects_province_workbook():
    """全国册 sniff 不得误吃省月报 (format A fixture 无交接表三 sheet)."""
    from services.province_fundamentals.parsers import jiaojie_2025
    assert not jiaojie_2025.sniff(FIXTURE)
    assert jiaojie_2025.sniff(FIXTURE_JIAOJIE)


# ── dispatch_all (2026-11-04, wave-2 fix): aggregate workbooks carry multiple ──
# content types; first-hit-wins starved interprov parsers of 聚合册.

def test_dispatch_all_returns_every_matching_parser():
    from services.province_fundamentals import registry
    f = Path("tests/province_fundamentals/fixtures/蒙东-电力交易市场信息披露数据_截至2026年5月.xlsx")
    all_hits = registry.dispatch_all(f)
    fmts = [fmt for fmt, _ in all_hits]
    # the aggregate workbook must hit BOTH its disclosure parser AND interprov2
    assert any("month_rows" in x or "format" in x for x in fmts), fmts
    assert any("interprov" in x for x in fmts), fmts
    # first-hit dispatch still returns just one
    one_fmt, _ = registry.dispatch(f)
    assert one_fmt == fmts[0]


def test_dispatch_all_unknown_file_returns_empty():
    from services.province_fundamentals import registry
    assert registry.dispatch_all(Path("/tmp/不存在的文件.xlsx")) == []
