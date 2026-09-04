import io

import pytest

from services.deal_committee.intake_parser import MAX_CHARS, extract_text


def test_txt_roundtrip():
    text = extract_text("蒙西 100MW/200MWh 储能项目,总投资 12 亿元。".encode("utf-8"), "deal.txt")
    assert "蒙西" in text
    assert "100MW" in text


def test_docx_roundtrip():
    import docx
    doc = docx.Document()
    doc.add_paragraph("山东 200MW 风电项目建议书")
    buf = io.BytesIO()
    doc.save(buf)
    text = extract_text(buf.getvalue(), "proposal.docx")
    assert "山东" in text


def test_unsupported_extension_raises():
    with pytest.raises(ValueError, match="不支持"):
        extract_text(b"MZ", "archive.zip")


def test_empty_content_raises():
    with pytest.raises(ValueError, match="提取"):
        extract_text(b"   ", "empty.txt")


def test_truncates_to_max_chars():
    text = extract_text(("长" * (MAX_CHARS + 5000)).encode("utf-8"), "long.txt")
    assert len(text) == MAX_CHARS
