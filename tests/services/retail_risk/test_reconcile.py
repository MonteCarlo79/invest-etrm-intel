# tests/services/retail_risk/test_reconcile.py
import datetime
import pandas as pd
import pytest
from services.retail_risk import reconcile as rc


def test_spot_vwap_simple_mean():
    df = pd.DataFrame({"rt_price": [300.0, 400.0, 500.0]})
    assert rc.spot_vwap_series(df) == pytest.approx(400.0)


def test_expected_spot_cost():
    assert rc.expected_spot_cost(1000.0, 350.0) == 350000.0


def test_judge_status():
    L = rc.ReconLine
    matched = [L("midlong", 100.0, 100.5, 0.5, ""), L("spot", 50.0, 50.0, 0.0, ""),
               L("total", 180.0, 180.4, 0.4, ""), L("residual", 30.0, 30.1, 0.1, "fees")]
    assert rc.judge(matched, tol=1.0) == "matched"
    over = [L("midlong", 100.0, 105.0, 5.0, ""), L("spot", 50.0, 50.0, 0.0, ""),
            L("total", 180.0, 185.0, 5.0, ""), L("residual", 30.0, 30.0, 0.0, "fees")]
    assert rc.judge(over, tol=1.0) == "flagged"
    # explained: midlong/spot + residual tie within tol even when total doesn't
    explained = [L("midlong", 100.0, 100.0, 0.0, ""), L("spot", 50.0, 50.0, 0.0, ""),
                 L("total", 180.0, 200.0, 20.0, ""), L("residual", 30.0, 30.0, 0.0, "fees")]
    assert rc.judge(explained, tol=1.0) == "explained"


def test_tolerance():
    assert rc.tolerance_for(2_000_000.0, 0.005, 5000.0) == 10000.0
    assert rc.tolerance_for(500_000.0, 0.005, 5000.0) == 5000.0


def test_settle_volume_shortest_code_wins():
    items = pd.DataFrame([
        {"volume_mwh": 21906.46, "category": "other", "notes": "01"},
        {"volume_mwh": 17770.0, "category": "midlong_energy", "notes": "0101"},
        {"volume_mwh": 4136.46, "category": "spot_energy", "notes": "0102"},
    ])
    assert rc.settle_volume(items) == 21906.46      # top line, not the double-counted sum


def test_settle_volume_fallback_spot_sum():
    items = pd.DataFrame([
        {"volume_mwh": 7546.9, "category": "spot_energy", "notes": "RT"},
        {"volume_mwh": 7662.2, "category": "spot_energy", "notes": "RT"},
    ])
    assert rc.settle_volume(items) == pytest.approx(15209.1)


def test_invoice_by_category_hierarchy_aware():
    """Hierarchical subject lines: amounts use the MIN-code-length line per
    category (top line), fees ignore sub-lines and the 2-digit header rows."""
    items = pd.DataFrame([
        {"category": "other", "amount_cny": 7263741.37, "volume_mwh": 21906.46, "notes": "01"},
        {"category": "midlong_energy", "amount_cny": 6000657.47, "volume_mwh": 17770.0, "notes": "0101"},
        {"category": "midlong_energy", "amount_cny": 6086813.27, "volume_mwh": 16351.05, "notes": "010102"},
        {"category": "midlong_energy", "amount_cny": 6081078.57, "volume_mwh": 16338.0, "notes": "0101020302"},
        {"category": "spot_energy", "amount_cny": 1263083.90, "volume_mwh": 4136.46, "notes": "0102"},
        {"category": "spot_energy", "amount_cny": 2312853.23, "volume_mwh": 6721.70, "notes": "0102020301"},
        {"category": "imbalance", "amount_cny": 10383.71, "volume_mwh": None, "notes": "0202030002"},
        {"category": "market_redistribution", "amount_cny": 176.41, "volume_mwh": None, "notes": "0202"},
        {"category": "rule_charges", "amount_cny": 193111.28, "volume_mwh": None, "notes": "0211030001"},
    ])
    by_cat = rc.invoice_by_category_frame(items)
    assert by_cat["midlong_energy"] == 6000657.47       # top line only, NOT 46M
    assert by_cat["spot_energy"] == 1263083.90
    assert by_cat["market_redistribution"] == 176.41
    assert by_cat["imbalance"] == 10383.71
    assert "other" not in by_cat                         # 2-digit header excluded


def test_invoice_by_category_db_notes_format():
    """C3 regression: notes stored by the loader are '<code> | <label>' — code
    extraction must work on that format (label-first would defeat it)."""
    items = pd.DataFrame([
        {"category": "other", "amount_cny": 7263741.37, "volume_mwh": 21906.46,
         "notes": "01 | 电量清分"},
        {"category": "midlong_energy", "amount_cny": 6000657.47, "volume_mwh": 17770.0,
         "notes": "0101 | 中长期交易"},
        {"category": "midlong_energy", "amount_cny": 6086813.27, "volume_mwh": 16351.05,
         "notes": "010102 | 电力直接交易"},
        {"category": "spot_energy", "amount_cny": 1263083.90, "volume_mwh": 4136.46,
         "notes": "0102 | 现货交易"},
    ])
    by_cat = rc.invoice_by_category_frame(items)
    assert by_cat["midlong_energy"] == 6000657.47       # NOT the double count
    assert rc.settle_volume(items) == 21906.46


def test_invoice_totals_no_join_fanout():
    """C2 regression: settlement total must come from rm_settlements alone —
    joining items multiplies the printed total by the item count."""
    from unittest.mock import patch, MagicMock
    settlements_df = pd.DataFrame([{"total": 175251.68, "doc_vol": None}])
    items_df = pd.DataFrame([
        {"volume_mwh": 21906.46, "category": "other", "notes": "01 | 电量清分"},
        {"volume_mwh": 17770.0, "category": "midlong_energy", "notes": "0101 | 中长期交易"},
    ])
    queries = []
    def fake_read_sql(q, conn, params=None):
        queries.append(str(q))
        return settlements_df if len(queries) == 1 else items_df
    conn = MagicMock()
    with patch("services.retail_risk.reconcile.pd.read_sql", side_effect=fake_read_sql):
        out = rc._invoice_totals(conn, 42, datetime.date(2026, 3, 1))
    assert out["total"] == 175251.68                    # not x2 (2 items)
    assert out["settled_vol"] == 21906.46
    assert "JOIN" not in queries[0].upper()             # total query must not join
