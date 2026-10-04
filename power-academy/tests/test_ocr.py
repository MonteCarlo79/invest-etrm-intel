import pymupdf
import pytest

from academy.extract import extract_source
from academy.models import SourceEntry

Vision = pytest.importorskip("Vision")

TEXT = ("Spark spread option valuation uses forward prices of power and gas. "
        "The holder receives the positive part of the spread each hour. ") * 3


def _scanned_pdf(path):
    """Render text to a bitmap and embed only the bitmap: a PDF with no text layer."""
    src = pymupdf.open()
    page = src.new_page()
    page.insert_textbox(pymupdf.Rect(40, 40, 555, 800), TEXT, fontsize=16)
    png = page.get_pixmap(dpi=150).tobytes("png")
    out = pymupdf.open()
    p = out.new_page()
    p.insert_image(p.rect, stream=png)
    out.save(path)


def _entry(path):
    return SourceEntry(id="scan", path=str(path), title="scan", type="pdf",
                       source_class="library", cleared=False, license_risk="high")


def test_scanned_pdf_is_no_text_without_ocr_and_ok_with_ocr(tmp_path):
    pdf = tmp_path / "scan.pdf"
    _scanned_pdf(pdf)
    assert extract_source(_entry(pdf), tmp_path / "c1")["status"] == "no_text"
    r = extract_source(_entry(pdf), tmp_path / "c2", ocr=True)
    assert r["status"] == "ok"
    body = (tmp_path / "c2" / "scan.txt").read_text(encoding="utf-8").lower()
    assert "spark spread" in body and "forward prices" in body


def test_ocr_leaves_blank_pdf_as_no_text(tmp_path):
    d = pymupdf.open()
    d.new_page()
    d.save(tmp_path / "blank.pdf")
    e = SourceEntry(id="blank", path=str(tmp_path / "blank.pdf"), title="b", type="pdf",
                    source_class="library", cleared=False, license_risk="high")
    assert extract_source(e, tmp_path / "c", ocr=True)["status"] == "no_text"
