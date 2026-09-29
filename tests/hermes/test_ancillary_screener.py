# tests/hermes/test_ancillary_screener.py
"""调频收入 screener: extraction→row conversion + sanity guards."""
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from services.hermes.ancillary_screener import _to_ancillary_rows  # noqa: E402


def test_valid_extraction_converts():
    data = {
        "rows": [
            {"month": "2026-06-01", "amount_wanyuan": 534.99,
             "metric": "调频补偿费用_独立储能"},
            {"month": "2026-07", "amount_wanyuan": 567.91,
             "metric": "调频补偿费用_独立储能"},
        ],
        "confidence": "high",
        "_source_hint": "新疆月报.pdf",
    }
    rows = _to_ancillary_rows("新疆", data)
    assert len(rows) == 2
    assert rows[0].amount_yuan == pytest.approx(534.99e4)
    assert rows[0].month == "2026-06-01"
    assert rows[1].month == "2026-07-01"     # day filled
    assert rows[0].province == "新疆"


def test_rejects_garbage():
    data = {"rows": [
        {"month": "not-a-month", "amount_wanyuan": 100, "metric": "x"},
        {"month": "2026-06-01", "amount_wanyuan": "abc", "metric": "x"},
        {"month": "2026-06-01", "amount_wanyuan": -5, "metric": "x"},
        {"month": "2026-06-01", "amount_wanyuan": 2e6, "metric": "x"},  # implausible
    ]}
    assert _to_ancillary_rows("新疆", data) == []


def test_empty_rows_and_none():
    assert _to_ancillary_rows("新疆", {"rows": []}) == []
    assert _to_ancillary_rows("新疆", None) == []
    assert _to_ancillary_rows("新疆", {}) == []


def test_review_helpers_sql():
    """Review helpers issue the expected statements against a mock engine."""
    from services.ancillary_revenue.extract_ancillary import (
        confirm_ancillary, dismiss_ancillary, insert_manual_ancillary,
    )
    eng = MagicMock()
    ctx = eng.begin.return_value.__enter__.return_value

    confirm_ancillary(eng, 7)
    assert "status='confirmed'" in ctx.execute.call_args.args[0].text

    dismiss_ancillary(eng, 7)
    assert "status='superseded'" in ctx.execute.call_args.args[0].text

    insert_manual_ancillary(eng, "新疆", "2026-08-01", "调频补偿费用_独立储能",
                            500.0e4, "source note")
    sql = ctx.execute.call_args.args[0].text
    params = ctx.execute.call_args.args[1]
    assert "ON CONFLICT" in sql
    assert params["p"] == "新疆" and params["amt"] == 500.0e4


def test_mgmt_section_and_i18n_wired():
    src = Path("apps/bess-map/app.py").read_text(encoding="utf-8")
    assert "insert_manual_ancillary" in src
    assert "list_pending_ancillary" in src
    assert "fr_manual_form" in src
    for key in ('"fr_mgmt_title"', '"fr_form_amount"', '"fr_pending_caption"'):
        assert src.count(key) >= 2, f"{key} missing from one locale dict"


def test_hermes_cron_and_command_wired():
    src = Path("services/hermes/app.py").read_text(encoding="utf-8")
    assert "screen_ancillary_revenue" in src
    assert "day=5, hour=13, minute=0" in src
    assert "frrev" in src and "调频扫描" in src
