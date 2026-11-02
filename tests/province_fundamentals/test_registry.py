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


def test_known_formats_lists_format_a():
    assert "format_a" in registry.known_formats()


def test_parse_file_dispatches_anhui_fixture():
    """省份从文件名嗅探 ("安徽…月报数据….xlsx"), 格式由 parser.sniff 确认."""
    rows = registry.parse_file(FIXTURE)
    assert rows, "fixture must parse non-empty"
    assert {r.province for r in rows} == {"安徽"}
    by = {(r.month, r.metric): r.value for r in rows}
    assert by[(date(2025, 1, 1), "cap_total")] == pytest.approx(6831.49)


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
