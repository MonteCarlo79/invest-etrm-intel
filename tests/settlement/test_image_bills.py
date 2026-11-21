"""Image bills (PNG/JPG) enter the PDF ladder as a one-page scanned PDF."""
import pytest

pymupdf = pytest.importorskip("pymupdf")

from libs.asset_risk.settlement.ocr import IMAGE_EXTS, image_to_pdf
from services.settlement_ingest.scanner import iter_bill_files


def _png() -> bytes:
    pix = pymupdf.Pixmap(pymupdf.csRGB, pymupdf.IRect(0, 0, 40, 20), 0)
    pix.clear_with(255)
    return pix.tobytes("png")


def test_image_to_pdf_is_one_scanned_page(tmp_path):
    pdf = image_to_pdf(_png(), "png")
    doc = pymupdf.open(stream=pdf, filetype="pdf")
    assert doc.page_count == 1 and doc[0].get_text().strip() == ""
    assert {"png", "jpg", "jpeg"} <= IMAGE_EXTS


def test_scanner_picks_up_images_case_insensitively(tmp_path):
    for n in ("a.pdf", "b.PNG", "c.jpg", "d.jpeg", "e.xlsx", "~f.png"):
        (tmp_path / n).write_bytes(b"x")
    assert [p.name for p in iter_bill_files(tmp_path)] == ["a.pdf", "b.PNG", "c.jpg", "d.jpeg"]


def test_camscanner_stamp_alone_counts_as_scanned(tmp_path):
    from libs.asset_risk.settlement.router import read_bill
    doc = pymupdf.open()
    page = doc.new_page()
    page.insert_image(pymupdf.Rect(0, 0, 40, 20), stream=_png())
    page.insert_text((50, 700), "扫描全能王 创建", fontname="china-s")
    pdf = tmp_path / "cam.pdf"
    doc.save(pdf)
    assert read_bill(str(pdf), "x.pdf").is_scanned
