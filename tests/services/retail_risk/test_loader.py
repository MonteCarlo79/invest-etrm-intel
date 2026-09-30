# tests/services/retail_risk/test_loader.py
import datetime
from unittest.mock import MagicMock
import pandas as pd
import pytest
from services.retail_risk import loader, schemas


def _mock_conn(book_id=42):
    conn = MagicMock()
    conn.execute.return_value.scalar.return_value = book_id
    return conn


def test_rollup_vwap_signed():
    df = pd.DataFrame([
        ["2026-03-01", 8, "monthly_auction", "forward", "buy", 10.0, 400.0, None, "月度竞价", "f1"],
        ["2026-03-01", 9, "monthly_auction", "forward", "buy", 30.0, 300.0, None, "月度竞价", "f1"],
        ["2026-03-01", 10, "monthly_auction", "forward", "buy", 20.0, -50.0, None, "合同转让", "f2"],
    ], columns=schemas.TRADES_COLS)
    rolled = loader.rollup_trades_day(df)
    row = rolled[(rolled.channel == "monthly_auction")].iloc[0]
    # (10*400 + 30*300 - 20*50) / 60 = 12000/60 = 200.0  (negative price preserved)
    assert row["volume_mwh"] == 60.0
    assert row["price_cny_mwh"] == pytest.approx(200.0)


def test_rollup_status_open_closed():
    today = datetime.date.today()
    past = today - datetime.timedelta(days=5)
    future = today + datetime.timedelta(days=5)
    df = pd.DataFrame([
        [past.isoformat(), 8, "annual", "bilateral", "buy", 1.0, 350.0, None, "年度双边", "f"],
        [future.isoformat(), 8, "annual", "bilateral", "buy", 1.0, 350.0, None, "年度双边", "f"],
    ], columns=schemas.TRADES_COLS)
    rolled = loader.rollup_trades_day(df)
    assert rolled.loc[rolled.delivery_date == past, "status"].iloc[0] == "closed"
    assert rolled.loc[rolled.delivery_date == future, "status"].iloc[0] == "open"


def test_write_trades_deletes_batch_first():
    conn = _mock_conn()
    df = pd.DataFrame([["2026-03-01", 8, "monthly_auction", "forward", "buy",
                        1.0, 400.0, None, "月度竞价", "f"]], columns=schemas.TRADES_COLS)
    loader.write_trades(conn, 42, df, "jinan_202603_trades", province="冀南")
    statements = [str(c.args[0]) for c in conn.execute.call_args_list]
    assert any("DELETE FROM marketdata.rm_positions" in s and "upload_batch_id" in s for s in statements)
    assert any("INSERT INTO marketdata.rm_positions" in s for s in statements)


def test_write_invoice_dedup_by_hash():
    conn = _mock_conn()
    # first execute (hash check) returns an existing id -> skip
    conn.execute.return_value.scalar.return_value = 7
    doc = schemas.InvoiceDoc(settlement_month=datetime.date(2026, 3, 1), items=[], total_amount_cny=1.0)
    assert loader.write_invoice(conn, 42, doc, "f.pdf", "deadbeef") is None


def _invoice_conn():
    """dup check -> None, settlement INSERT -> sid 99."""
    conn = MagicMock()
    conn.execute.return_value.scalar.side_effect = [None, 99] + [99] * 50
    return conn


def _settlement_status(conn):
    for c in conn.execute.call_args_list:
        if "INSERT INTO marketdata.rm_settlements" in str(c.args[0]):
            return c.args[1]["st"]
    return None


def test_invoice_flagged_when_toplines_inconsistent():
    """01-line amount must equal midlong+spot top lines (>1% -> flagged)."""
    conn = _invoice_conn()
    doc = schemas.InvoiceDoc(
        settlement_month=datetime.date(2026, 3, 1),
        items=[
            {"category": "other", "label_cn": "电量清分", "amount_cny": 7263741.37,
             "volume_mwh": 21906.46, "notes": "01"},
            {"category": "midlong_energy", "label_cn": "中长期交易", "amount_cny": 6000657.47,
             "volume_mwh": 17770.0, "notes": "0101"},
            {"category": "spot_energy", "label_cn": "现货交易", "amount_cny": 1000000.00,
             "volume_mwh": 3000.0, "notes": "0102"},   # 6.0M + 1.0M ≠ 7.26M -> flagged
        ],
        total_amount_cny=175251.68)
    loader.write_invoice(conn, 42, doc, "f.pdf", "h1")
    assert _settlement_status(conn) == "flagged"


def test_invoice_processed_when_toplines_tie():
    conn = _invoice_conn()
    doc = schemas.InvoiceDoc(
        settlement_month=datetime.date(2026, 3, 1),
        items=[
            {"category": "other", "label_cn": "电量清分", "amount_cny": 7263741.37,
             "volume_mwh": 21906.46, "notes": "01"},
            {"category": "midlong_energy", "label_cn": "中长期交易", "amount_cny": 6000657.47,
             "volume_mwh": 17770.0, "notes": "0101"},
            {"category": "spot_energy", "label_cn": "现货交易", "amount_cny": 1263083.90,
             "volume_mwh": 4136.46, "notes": "0102"},
        ],
        total_amount_cny=175251.68)
    loader.write_invoice(conn, 42, doc, "f.pdf", "h2")
    assert _settlement_status(conn) == "processed"
