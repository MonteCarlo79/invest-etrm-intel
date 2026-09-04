"""Tests for capcomp table parser + new classifier rules."""
import pytest

from services.settlement_ingest.parser_voucher import parse_capcomp_table_text
from services.settlement_ingest.scanner import classify_pdf

TABLE_TEXT_STANDARD = """
2025年9月储能容量补偿费用统计表
序号 公司名称 机组名称 补偿费用 容量补偿费用
一、储能电站享受容量补偿费用明细
4 苏尼特右旗景蓝新能源有限公司 景蓝乌尔图储能电站 7,542,360.25 7,542,360.25
6 乌海市远鸿富景新能源科技有限公司 富景五虎山储能电站 7,125,694.16 7,125,694.16
"""

TABLE_TEXT_DASH = """
2025年12月储能容量补偿费用统计表
一、储能电站享受容量补偿费用明细
4 苏尼特右旗景蓝新能源有限公司 景蓝乌尔图储能电站 - 6,046,116.33 6,046,116.33
6 乌海市远鸿富景新能源科技有限公司 富景五虎山储能电站 - 6,421,466.08 6,421,466.08
"""


class TestCapcompTableParser:
    def test_standard_row(self):
        assert parse_capcomp_table_text(TABLE_TEXT_STANDARD, "富景五虎山储能电站") == pytest.approx(7125694.16)
        assert parse_capcomp_table_text(TABLE_TEXT_STANDARD, "景蓝乌尔图储能电站") == pytest.approx(7542360.25)

    def test_dash_layout(self):
        assert parse_capcomp_table_text(TABLE_TEXT_DASH, "富景五虎山储能电站") == pytest.approx(6421466.08)

    def test_station_absent(self):
        assert parse_capcomp_table_text(TABLE_TEXT_STANDARD, "不存在储能电站") is None


class TestClassifierRules:
    def test_capcomp_table_classification(self):
        assert classify_pdf("2025年9月储能容量补偿费用统计表（发电厂）.pdf") == "capcomp_table"
        assert classify_pdf("2025年8月储能容量补偿费用统计表.pdf") == "capcomp_table"

    def test_fadianchang_jiesuan_is_voucher(self):
        assert classify_pdf("发电厂结算2025-05-01.pdf") == "voucher"

    def test_grid_bills_unaffected(self):
        assert classify_pdf("2025年3月 【B-8-上】富景五虎山储能电站上网电费结算单-3月结算单.pdf") == "discharge"
        assert classify_pdf("杭锦旗1月份电费清单.pdf") == "charge"
