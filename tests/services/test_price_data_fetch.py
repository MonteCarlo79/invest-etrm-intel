"""fetch_price_history degenerate-column fallback (LingFeng zero-fill pattern)."""
import pandas as pd
import pytest

from services.deal_engine import price_data


def _hours_df(values):
    return pd.DataFrame({"datetime": pd.date_range("2026-01-01", periods=len(values), freq="h"),
                         "price": values})


@pytest.fixture
def fake_db(monkeypatch):
    """Patches the DB layer; records which price columns were queried."""
    calls = []

    class _Conn:
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

    class _Engine:
        def connect(self):
            return _Conn()

    def fake_read_sql(sql, conn, params=None):
        col = "rt_price" if "rt_price" in str(sql) else "da_price"
        calls.append(col)
        values = fake_db.data[col]
        return _hours_df(values)

    fake_db.data = {}
    monkeypatch.setattr(price_data, "get_engine", lambda: _Engine())
    monkeypatch.setattr(price_data.pd, "read_sql", fake_read_sql)
    fake_db.calls = calls
    return fake_db


REAL = [300.0 + (h % 24) for h in range(200)]
ZEROS = [0.0] * 200


def test_da_all_zero_falls_back_to_rt(fake_db):
    fake_db.data = {"da_price": ZEROS, "rt_price": REAL}
    out = price_data.fetch_price_history("蒙西", "2026-01-01", "2026-02-01", price_col="da_price")
    assert fake_db.calls == ["da_price", "rt_price"]
    assert out == REAL


def test_rt_all_zero_falls_back_to_da(fake_db):
    fake_db.data = {"rt_price": ZEROS, "da_price": REAL}
    out = price_data.fetch_price_history("蒙东", "2026-01-01", "2026-02-01", price_col="rt_price")
    assert fake_db.calls == ["rt_price", "da_price"]
    assert out == REAL


def test_both_all_zero_raises_clear_error(fake_db):
    fake_db.data = {"da_price": ZEROS, "rt_price": ZEROS}
    with pytest.raises(ValueError, match="蒙西"):
        price_data.fetch_price_history("蒙西", "2026-01-01", "2026-02-01")


def test_real_da_no_fallback(fake_db):
    fake_db.data = {"da_price": REAL, "rt_price": [1.0] * 200}
    out = price_data.fetch_price_history("山东", "2026-01-01", "2026-02-01", price_col="da_price")
    assert fake_db.calls == ["da_price"]
    assert out == REAL
