# tests/ancillary_revenue/test_extract_ancillary.py
"""新疆 monthly 调频补偿 extraction tests (real PDFs under
data/exchange-monthly-reports/新疆月报/ — skipped if data dir absent)."""
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "services" / "ancillary_revenue"))
from extract_ancillary import _month_from_name, extract_xinjiang_monthly  # noqa: E402

_DATA = Path(__file__).resolve().parents[2] / "data" / "exchange-monthly-reports" / "新疆月报"


def test_month_from_name():
    assert _month_from_name("2026年6月省内辅助服务市场运行情况.pdf") == "2026-06-01"
    assert _month_from_name("新疆-2026-07-report.pdf") == "2026-07-01"
    assert _month_from_name("no-date.pdf") is None


@pytest.mark.skipif(not _DATA.is_dir(), reason="exchange reports data dir not present")
def test_june_2026_extraction():
    r = extract_xinjiang_monthly(_DATA / "2026年6月省内辅助服务市场运行情况.pdf")
    assert r is not None
    assert r.province == "新疆" and r.month == "2026-06-01"
    assert r.metric == "调频补偿费用_独立储能"
    assert r.amount_yuan == pytest.approx(534.99e4)   # 534.99 万元


@pytest.mark.skipif(not _DATA.is_dir(), reason="exchange reports data dir not present")
def test_jan_2026_no_bess_figure():
    assert extract_xinjiang_monthly(_DATA / "2026年1月省内辅助服务市场运行情况.pdf") is None


def test_upsert_skips_confirmed_and_superseded():
    """Dedup: confirmed/superseded (province, month, metric) must not be
    re-upserted by a later scan (2026-09-30 新疆 dupe incident)."""
    from unittest.mock import MagicMock, patch
    from extract_ancillary import AncillaryRow, upsert_rows

    rows = [
        AncillaryRow("新疆", "2026-06-01", "调频补偿费用_独立储能", 1.0, "scan.pdf"),
        AncillaryRow("新疆", "2026-07-01", "调频补偿费用_独立储能", 2.0, "scan.pdf"),
        AncillaryRow("广东", "2026-06-01", "调频补偿费用_独立储能", 3.0, "scan.pdf"),
    ]
    # confirmed exists for 新疆-06, superseded for 新疆-07, nothing for 广东-06
    existing = {("新疆", "2026-06-01"), ("新疆", "2026-07-01")}

    cur = MagicMock()
    def fake_fetchone():
        call = cur.execute.call_args_list[-1].args[1]
        return (1,) if (call[0], call[1]) in existing else None
    cur.fetchone.side_effect = fake_fetchone
    conn = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cur

    with patch("psycopg2.connect", return_value=conn):
        n = upsert_rows(rows, "postgresql://x")

    assert n == 1          # only 广东-06 upserted
    upsert_calls = [c for c in cur.execute.call_args_list
                    if "INSERT INTO" in c.args[0]]
    assert len(upsert_calls) == 1
    assert upsert_calls[0].args[1][0] == "广东"
