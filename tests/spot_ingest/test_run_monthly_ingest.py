from pathlib import Path
from unittest.mock import patch

from services.spot_ingest.run_monthly_ingest import find_monthly_pdfs


def test_find_monthly_pdfs_filters(tmp_path: Path):
    (tmp_path / "电力现货市场价格与运行月报（2026年6月）.pdf").write_bytes(b"x")
    (tmp_path / "电力现货市场价格与运行日报2026-06-01.pdf").write_bytes(b"x")
    (tmp_path / "山东电力交易中心2026年6月月报.pdf").write_bytes(b"x")
    (tmp_path / "notes.txt").write_bytes(b"x")
    found = find_monthly_pdfs(tmp_path)
    assert [p.name for p in found] == ["电力现货市场价格与运行月报（2026年6月）.pdf"]


def test_find_monthly_pdfs_skips_yearless(tmp_path: Path):
    (tmp_path / "电力现货市场价格与运行月报（6月）.pdf").write_bytes(b"x")
    assert find_monthly_pdfs(tmp_path) == []
