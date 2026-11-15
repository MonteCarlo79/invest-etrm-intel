"""Tests for content-hash dedup in tab_settlement uploads."""
import pytest
from sqlalchemy import create_engine, event, text
from sqlalchemy.pool import StaticPool

from apps.asset_risk.tab_settlement import _content_sha256, _already_ingested


def test_content_sha256_deterministic():
    assert _content_sha256(b"invoice") == _content_sha256(b"invoice")
    assert _content_sha256(b"invoice") != _content_sha256(b"invoice2")
    assert len(_content_sha256(b"x")) == 64


class _FakeConn:
    def __init__(self, found):
        self._found = found
        self.last_params = None

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def execute(self, query, params):
        self.last_params = params
        found = self._found

        class _Result:
            def first(self):
                return (1,) if found else None

        return _Result()


class _FakeEngine:
    def __init__(self, found):
        self.conn = _FakeConn(found)

    def connect(self):
        return self.conn


def test_already_ingested_true_when_hash_at_same_month():
    eng = _FakeEngine(found=True)
    assert _already_ingested(eng, 3, "2026-01-01", "abc123") is True
    assert eng.conn.last_params == {"bid": 3, "month": "2026-01-01", "h": "abc123"}


def test_already_ingested_false_when_absent():
    eng = _FakeEngine(found=False)
    assert _already_ingested(eng, 3, "2026-01-01", "abc123") is False


@pytest.fixture
def sqlite_engine():
    eng = create_engine("sqlite://", poolclass=StaticPool, connect_args={"check_same_thread": False})

    @event.listens_for(eng, "connect")
    def _attach(dbapi_conn, _):
        dbapi_conn.execute("ATTACH DATABASE ':memory:' AS marketdata")

    with eng.begin() as conn:
        conn.execute(text(
            "CREATE TABLE marketdata.rm_settlements (id INTEGER PRIMARY KEY AUTOINCREMENT, "
            "book_id INT, settlement_month TEXT, file_name TEXT, file_type TEXT, status TEXT, raw_data TEXT)"))
        conn.execute(text(
            "CREATE TABLE marketdata.rm_settlement_items (id INTEGER PRIMARY KEY AUTOINCREMENT, "
            "settlement_id INT, category TEXT, delivery_date TEXT, volume_mwh REAL, price_cny_kwh REAL, "
            "amount_cny REAL, amount_receivable_cny REAL, amount_settled_cny REAL, amount_diff_cny REAL, "
            "counterparty TEXT, peak_period TEXT, notes TEXT)"))
    return eng


def test_insert_settlement_pdf_and_excel_items(sqlite_engine):
    from libs.asset_risk.settlement.ledger import insert_settlement
    pdf_items = [{"category": "discharge_energy", "amount_cny": 100.0, "peak_period": "peak", "notes": "n"}]
    xl_items = [{"category": "capacity_compensation", "amount_cny": 50.0, "delivery_date": "2026-03-01",
                 "amount_receivable_cny": 60.0, "amount_settled_cny": 50.0, "amount_diff_cny": 10.0,
                 "counterparty": "exchange"}]
    with sqlite_engine.begin() as conn:
        sid1 = insert_settlement(conn, 7, "2026-03-01", "a.pdf", "pdf", "h1", pdf_items)
        sid2 = insert_settlement(conn, 7, "2026-03-01", "b.xlsx", "excel", "h2", xl_items)
    with sqlite_engine.connect() as conn:
        rows = conn.execute(text("SELECT settlement_id, category, amount_cny, peak_period, amount_diff_cny "
                                 "FROM marketdata.rm_settlement_items ORDER BY settlement_id")).fetchall()
    assert rows[0][:4] == (sid1, "discharge_energy", 100.0, "peak") and rows[0][4] is None
    assert rows[1][:3] == (sid2, "capacity_compensation", 50.0) and rows[1][4] == 10.0


def test_overwrite_leaves_single_header_and_no_orphans(sqlite_engine):
    from libs.asset_risk.settlement.ledger import insert_settlement, _delete_book_month_file
    old = [{"category": "discharge_energy", "amount_cny": 100.0}]
    new = [{"category": "discharge_energy", "amount_cny": 120.0}]
    other = [{"category": "discharge_energy", "amount_cny": 5.0}]
    with sqlite_engine.begin() as conn:
        insert_settlement(conn, 7, "2026-03-01", "a.pdf", "pdf", "hold", old)
        insert_settlement(conn, 7, "2026-03-01", "other.pdf", "pdf", "hother", other)
    with sqlite_engine.begin() as conn:
        _delete_book_month_file(conn, 7, "2026-03-01", "a.pdf")
        new_id = insert_settlement(conn, 7, "2026-03-01", "a.pdf", "pdf", "hnew", new)
    with sqlite_engine.connect() as conn:
        heads = conn.execute(text(
            "SELECT id FROM marketdata.rm_settlements WHERE file_name = 'a.pdf'")).fetchall()
        orphans = conn.execute(text(
            "SELECT COUNT(*) FROM marketdata.rm_settlements s WHERE NOT EXISTS "
            "(SELECT 1 FROM marketdata.rm_settlement_items i WHERE i.settlement_id = s.id)")).scalar()
        items = conn.execute(text(
            "SELECT amount_cny FROM marketdata.rm_settlement_items WHERE settlement_id = :s"),
            {"s": new_id}).fetchall()
        n_other = conn.execute(text(
            "SELECT COUNT(*) FROM marketdata.rm_settlements WHERE file_name = 'other.pdf'")).scalar()
    assert [h[0] for h in heads] == [new_id]
    assert orphans == 0
    assert items == [(120.0,)]
    assert n_other == 1


def test_already_ingested_false_for_old_hash_after_overwrite(sqlite_engine):
    from libs.asset_risk.settlement.ledger import insert_settlement, _delete_book_month_file
    with sqlite_engine.begin() as conn:
        insert_settlement(conn, 7, "2026-03-01", "a.pdf", "pdf", "hold",
                          [{"category": "discharge_energy", "amount_cny": 1.0}])
    assert _already_ingested(sqlite_engine, 7, "2026-03-01", "hold") is True
    with sqlite_engine.begin() as conn:
        _delete_book_month_file(conn, 7, "2026-03-01", "a.pdf")
        insert_settlement(conn, 7, "2026-03-01", "a.pdf", "pdf", "hnew",
                          [{"category": "discharge_energy", "amount_cny": 2.0}])
    assert _already_ingested(sqlite_engine, 7, "2026-03-01", "hold") is False
    assert _already_ingested(sqlite_engine, 7, "2026-03-01", "hnew") is True
