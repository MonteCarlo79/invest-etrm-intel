"""Tests for voucher (结算凭证) classification and skip routing."""
from services.settlement_ingest.scanner import classify_pdf


def test_voucher_filenames():
    assert classify_pdf("2026年3月发电侧结算凭证.pdf") == "voucher"
    assert classify_pdf("2026年3月用户侧结算凭证.pdf") == "voucher"
    assert classify_pdf("乌海市远鸿富景新能源科技有限公司8月份结算凭证（发电侧）.pdf") == "voucher"
    assert classify_pdf("2026年5月用户侧结算凭证(2).pdf") == "voucher"


def test_existing_classifications_unchanged():
    assert classify_pdf("上网，乌海2026-03电费结算单.pdf") == "discharge"
    assert classify_pdf("2026年1月供电局电费账单.pdf") == "unknown"  # no keyword → unknown (text path handles)
    assert classify_pdf("杭锦旗1月份电费清单.pdf") == "charge"
    assert classify_pdf("某发票.pdf") == "skip"
    assert classify_pdf("2026年6月电费核查清单.pdf") == "skip"  # verification slip, not a bill
    assert classify_pdf("2026年5月电费核查清单(2).pdf") == "skip"
