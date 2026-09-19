"""Tests for month extraction boundary guard + 2025 voucher naming variants."""
from services.settlement_ingest.scanner import extract_month_from_filename, classify_pdf


class TestMonthExtractionBoundaries:
    def test_serial_number_prefix_not_a_date(self):
        # DC047300001001-2025-09-01: "1001-20" must NOT match; real date wins
        assert extract_month_from_filename("DC047300001001-2025-09-01（发电侧）.pdf") == "2025-09-01"
        assert extract_month_from_filename("YH0473004941005125-2025-11-01（用户侧）.pdf") == "2025-11-01"

    def test_invalid_month_rejected(self):
        # month 20 would overflow Postgres date; must not be returned
        assert extract_month_from_filename("1001-20-01.pdf") is None
        assert extract_month_from_filename("2025-13结算单.pdf") is None

    def test_normal_patterns_still_work(self):
        assert extract_month_from_filename("2026-01电费结算单.pdf") == "2026-01-01"
        assert extract_month_from_filename("2026年1月上网电费结算单.pdf") == "2026-01-01"
        assert extract_month_from_filename("发电厂结算2025-05-01.pdf") == "2025-05-01"
        assert extract_month_from_filename("乌海富景五虎山储能电站2025.01电费清单.pdf") == "2025-01-01"
        assert extract_month_from_filename("杭锦旗1月份电费清单.pdf") == "NEED_YEAR-01-01"

    def test_two_digit_year(self):
        # 零碳46 2024 uploads use 2-digit years (24年 = 2024, 25年 = 2025)
        assert extract_month_from_filename("【W-B-2-上】悦盛药草站24年5月下网电费结算单.pdf") == "2024-05-01"
        assert extract_month_from_filename("【W-B-2-下】悦盛药草站25年11月下网电费结算单.pdf") == "2025-11-01"
        # 4-digit wins when both present; the (?<!\d) guard blocks the 24年 inside 2024年
        assert extract_month_from_filename("2024年5月下网电费结算单.pdf") == "2024-05-01"


class TestVoucherNamingVariants:
    def test_side_marker_names_are_vouchers(self):
        assert classify_pdf("2025年10月发电侧结算(1).pdf") == "voucher"
        assert classify_pdf("2025年10月用户侧结算(1).pdf") == "voucher"
        assert classify_pdf("发电侧2025-06-01.pdf") == "voucher"
        assert classify_pdf("DC047300001001-2025-09-01（发电侧）.pdf") == "voucher"
        assert classify_pdf("YH0473004941005125-2025-09-01（用户侧）.pdf") == "voucher"
        assert classify_pdf("富景五虎山储能电站2025年12月电费结算清单（发电侧）(1).pdf") == "voucher"

    def test_grid_bills_unaffected(self):
        # 上网/下网 bills without side markers keep their classifications
        assert classify_pdf("2025年3月 【B-8-上】富景五虎山储能电站上网电费结算单-3月结算单.pdf") == "discharge"
        assert classify_pdf("2025年3月 【B-8-下】富景五虎山储能电站下网结算3月结算.pdf") == "charge"

    def test_quantity_statements_skipped(self):
        # 电量结算单 = 抄表 meter-reading attachment, no monetary rows (融水 2026-01/05)
        assert classify_pdf("W-19-上融水景岳2026年1月上网电量结算单.pdf") == "skip"
        assert classify_pdf("W-19-上融水景岳2026年5月上网电量结算单（盖章版）.pdf") == "skip"
        # the money bill keeps its discharge classification
        assert classify_pdf("W-19-上融水景岳2026年1月上网电费结算单.pdf") == "discharge"
