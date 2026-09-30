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
    assert any("DELETE FROM marketdata.rm_positions" in s for s in statements)
    assert any("INSERT INTO marketdata.rm_positions" in s for s in statements)


def test_write_trades_full_book_reload_idempotent():
    """I7: re-runs must not duplicate — positions AND volumes delete per-BOOK
    (full reload), not per run-dated batch."""
    conn = _mock_conn()
    df = pd.DataFrame([["2026-03-01", 8, "monthly_auction", "forward", "buy",
                        1.0, 400.0, None, "月度竞价", "f"]], columns=schemas.TRADES_COLS)
    loader.write_trades(conn, 42, df, "冀南_trades", province="冀南")
    deletes = [c for c in conn.execute.call_args_list
               if "DELETE FROM marketdata.rm_positions" in str(c.args[0])]
    assert deletes and deletes[0].args[1] == {"b": 42}          # whole book, not a batch

    conn2 = _mock_conn()
    vol = pd.DataFrame([{"delivery_date": datetime.date(2026, 3, 1), "hour": 8,
                         "channel": "annual", "volume_mwh": 1.0, "vwap_cny_mwh": 350.0,
                         "nominated_mwh": None, "settled_mwh": None, "estimated": False}])
    loader.write_volumes(conn2, 42, vol, "冀南_volumes")
    vdeletes = [c for c in conn2.execute.call_args_list
                if "DELETE FROM marketdata.rm_position_volumes" in str(c.args[0])]
    assert vdeletes and vdeletes[0].args[1] == {"b": 42}


def test_write_invoice_replace_on_corrected_file():
    """I8: same book+month, different hash -> old settlement + items deleted,
    new one inserted (corrected invoices supersede)."""
    conn = MagicMock()
    # hash-dup check -> None; existing-month check -> id 7; insert -> sid 99
    conn.execute.return_value.scalar.side_effect = [None, 7, 99] + [99] * 50
    doc = schemas.InvoiceDoc(
        settlement_month=datetime.date(2026, 3, 1),
        items=[{"category": "spot_energy", "label_cn": "现货交易",
                "amount_cny": 1.0, "volume_mwh": 1.0, "notes": "0102"}],
        total_amount_cny=1.0)
    sid = loader.write_invoice(conn, 42, doc, "f.pdf", "newhash")
    assert sid == 99
    stmts = [str(c.args[0]) for c in conn.execute.call_args_list]
    assert any("DELETE FROM marketdata.rm_settlement_items" in s for s in stmts)
    assert any("DELETE FROM marketdata.rm_settlements" in s for s in stmts)


def test_write_curves_batches_params():
    """Curves write in chunked executemany batches (list of dicts per call),
    not 8,760 single-row round trips — home-network RDS drops long transactions."""
    conn = _mock_conn()
    df = pd.DataFrame([
        ["山东", "spot_base", "2026-01-01", 0, 0.30, datetime.date(2026, 9, 30)],
        ["山东", "spot_base", "2026-01-01", 1, 0.31, datetime.date(2026, 9, 30)],
        ["山东", "spot_base", "2026-01-01", 2, 0.32, datetime.date(2026, 9, 30)],
    ], columns=schemas.CURVES_COLS)
    loader.write_curves(conn, df)
    assert conn.execute.call_count == 1                      # one batched call, not three
    args = conn.execute.call_args_list[0].args
    assert isinstance(args[1], list) and len(args[1]) == 3   # list of param dicts
    assert args[1][0]["dh"] == 0 and args[1][2]["dh"] == 2


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


def test_invoice_notes_written_code_first():
    """C3: notes must be '<code> | <label>' — reconcile's ^(\\d+) extraction
    depends on the leading code."""
    conn = _invoice_conn()
    doc = schemas.InvoiceDoc(
        settlement_month=datetime.date(2026, 3, 1),
        items=[{"category": "midlong_energy", "label_cn": "中长期交易",
                "amount_cny": 6000657.47, "volume_mwh": 17770.0, "notes": "0101"}],
        total_amount_cny=None)
    loader.write_invoice(conn, 42, doc, "f.pdf", "h3")
    item_inserts = [c for c in conn.execute.call_args_list
                    if "INSERT INTO marketdata.rm_settlement_items" in str(c.args[0])]
    assert item_inserts and item_inserts[0].args[1]["notes"] == "0101 | 中长期交易"


def test_excel_invoice_flagged_when_items_sum_off():
    """I6: 山东 7021 (excel) has no subject hierarchy — the cross-check is
    Σ items vs the printed 合计 within 1%."""
    conn = _invoice_conn()
    doc = schemas.InvoiceDoc(
        settlement_month=datetime.date(2026, 3, 1),
        items=[{"category": "spot_energy", "label_cn": "实时电能量电费",
                "amount_cny": 100.0, "volume_mwh": 1.0, "notes": "RT"}],
        total_amount_cny=6176047.51, total_kind="spot_subtotal")
    loader.write_invoice(conn, 42, doc, "7021-f.xlsx", "h4")
    assert _settlement_status(conn) == "flagged"


def test_excel_invoice_processed_when_items_sum_ties():
    conn = _invoice_conn()
    doc = schemas.InvoiceDoc(
        settlement_month=datetime.date(2026, 3, 1),
        items=[{"category": "spot_energy", "label_cn": "实时电能量电费",
                "amount_cny": 6176047.51, "volume_mwh": 15209.1, "notes": "RT"}],
        total_amount_cny=6176047.51, total_kind="spot_subtotal")
    loader.write_invoice(conn, 42, doc, "7021-f.xlsx", "h5")
    assert _settlement_status(conn) == "processed"


def test_pdf_invoice_flagged_when_midlong_or_spot_missing():
    """I6: 安徽统推 (01 line carries no amount) — presence check: at least one
    midlong AND one spot item, else flagged."""
    conn = _invoice_conn()
    doc = schemas.InvoiceDoc(
        settlement_month=datetime.date(2026, 3, 1),
        items=[{"category": "midlong_energy", "label_cn": "中长期交易",
                "amount_cny": 13255508.85, "volume_mwh": 38101.3, "notes": "01010201"}],
        total_amount_cny=462471.22)
    loader.write_invoice(conn, 42, doc, "f.pdf", "h6")
    assert _settlement_status(conn) == "flagged"
