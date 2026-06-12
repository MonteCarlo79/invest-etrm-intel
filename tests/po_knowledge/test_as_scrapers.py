"""Tests for Polish AS market scrapers and revenue estimator."""
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from unittest.mock import MagicMock
import pytest
from datetime import date, timedelta


def _recent_monday(weeks_ago: int = 1) -> str:
    """Return an ISO date string for Monday N weeks ago (always within any weeks_back window)."""
    today = date.today()
    last_monday = today - timedelta(days=today.weekday())
    return (last_monday - timedelta(weeks=weeks_ago)).isoformat()


def _make_cmbp_tp_response(n_days: int = 14, fcr_g: float = 200.0, fcr_d: float = 180.0,
                            afrr_g: float = 300.0, afrr_d: float = 280.0) -> dict:
    """Build a mock cmbp-tp API response with n_days of hourly data (recent dates)."""
    today = date.today()
    start = today - timedelta(days=n_days)
    records = []
    for i in range(n_days):
        bd = (start + timedelta(days=i)).isoformat()
        for h in range(24):
            records.append({
                "business_date": bd,
                "dtime": f"{bd} {h+1:02d}:00:00",
                "fcr_g": fcr_g,
                "fcr_d": fcr_d,
                "afrr_g": afrr_g,
                "afrr_d": afrr_d,
                "mfrrd_g": 200.0,
                "mfrrd_d": 190.0,
                "rr_g": 50.0,
                "rr_d": None,
                "onmb": f"{h:02d} - {h+1:02d}",
            })
    return {"value": records}


def _make_conn(fcr_rows=None, afrr_rows=None, cap_rows=None):
    """Build a mock psycopg2 connection with preset fetchone results."""
    conn = MagicMock()
    cur = MagicMock()
    conn.cursor.return_value.__enter__ = lambda s: cur
    conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

    fcr_rows  = fcr_rows  or []
    afrr_rows = afrr_rows or []
    cap_rows  = cap_rows  or []

    call_count = [0]

    def side_effect():
        call_count[0] += 1
        if call_count[0] == 1:   # FCR avg query
            return fcr_rows[0] if fcr_rows else None
        elif call_count[0] == 2:  # aFRR avg query
            return afrr_rows[0] if afrr_rows else None
        else:                     # capacity market latest
            return cap_rows[0] if cap_rows else None
    cur.fetchone.side_effect = side_effect
    return conn


def test_get_as_revenue_estimate_returns_zeros_when_no_data():
    from services.po_knowledge.entso_scraper import get_as_revenue_estimate
    conn = _make_conn()
    result = get_as_revenue_estimate(conn, power_mw=100.0, fcr_pct=30.0, afrr_pct=30.0)
    assert result["fcr_pln_yr"] == 0.0
    assert result["afrr_pln_yr"] == 0.0
    assert result["capacity_pln_yr"] == 0.0
    assert result["total_pln_yr"] == 0.0


def test_get_as_revenue_estimate_computes_correctly():
    from services.po_knowledge.entso_scraper import get_as_revenue_estimate
    # FCR: avg 12000 PLN/MW/week, aFRR: avg 15000 PLN/MW/week, RM: 200000 PLN/MW/yr
    conn = _make_conn(
        fcr_rows=[(12000.0, 10)],   # (avg_price, weeks_count)
        afrr_rows=[(15000.0, 8)],
        cap_rows=[(200000.0,)],
    )
    result = get_as_revenue_estimate(conn, power_mw=100.0, fcr_pct=30.0, afrr_pct=30.0)
    fcr_mw  = 100.0 * 0.30
    afrr_mw = 100.0 * 0.30
    assert result["fcr_pln_yr"]      == pytest.approx(12000.0 * fcr_mw * 52)
    assert result["afrr_pln_yr"]     == pytest.approx(15000.0 * afrr_mw * 52)
    assert result["capacity_pln_yr"] == pytest.approx(200000.0 * 100.0)
    assert result["total_pln_yr"]    == pytest.approx(
        result["fcr_pln_yr"] + result["afrr_pln_yr"] + result["capacity_pln_yr"]
    )
    assert result["fcr_weeks"] == 10
    assert result["afrr_weeks"] == 8


def test_scrape_po_fcr_prices_inserts_rows(requests_mock):
    """scrape_po_fcr_prices uses cmbp-tp endpoint and aggregates FCR prices by ISO week."""
    from services.po_knowledge.entso_scraper import scrape_po_fcr_prices

    requests_mock.get(
        "https://api.raporty.pse.pl/api/cmbp-tp",
        json=_make_cmbp_tp_response(n_days=14, fcr_g=200.0, fcr_d=180.0),
    )

    conn = MagicMock()
    cur = MagicMock()
    conn.cursor.return_value.__enter__ = lambda s: cur
    conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    cur.rowcount = 1

    n = scrape_po_fcr_prices(conn, weeks_back=4)
    assert n >= 0
    assert cur.execute.called
    # Verify weekly price: mean(200, 180) = 190 PLN/MW/h × 168 = 31920 PLN/MW/week
    args = cur.execute.call_args_list[0][0]
    assert args[1][1] == pytest.approx(190.0 * 168, rel=0.01)


def test_scrape_po_afrr_prices_inserts_rows(requests_mock):
    """scrape_po_afrr_prices uses cmbp-tp endpoint and aggregates aFRR prices by ISO week."""
    from services.po_knowledge.entso_scraper import scrape_po_afrr_prices

    requests_mock.get(
        "https://api.raporty.pse.pl/api/cmbp-tp",
        json=_make_cmbp_tp_response(n_days=14, afrr_g=300.0, afrr_d=280.0),
    )

    conn = MagicMock()
    cur = MagicMock()
    conn.cursor.return_value.__enter__ = lambda s: cur
    conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    cur.rowcount = 1

    n = scrape_po_afrr_prices(conn, weeks_back=4)
    assert n >= 0
    assert cur.execute.called
    # Verify weekly price: mean(300, 280) = 290 PLN/MW/h × 168 = 48720 PLN/MW/week
    args = cur.execute.call_args_list[0][0]
    assert args[1][1] == pytest.approx(290.0 * 168, rel=0.01)


def test_scrape_po_fcr_prices_handles_api_error(requests_mock):
    """If PSE cmbp-tp API returns 500, function returns 0 without raising."""
    from services.po_knowledge.entso_scraper import scrape_po_fcr_prices

    requests_mock.get("https://api.raporty.pse.pl/api/cmbp-tp", status_code=500)

    conn = MagicMock()
    cur = MagicMock()
    conn.cursor.return_value.__enter__ = lambda s: cur
    conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

    n = scrape_po_fcr_prices(conn, weeks_back=4)
    assert n == 0


def test_scrape_po_capacity_market_inserts_rows(requests_mock):
    from services.po_knowledge.entso_scraper import scrape_po_capacity_market

    html = """
    <html><body>
    <table>
      <thead><tr><th>Rok dostaw</th><th>Data aukcji</th><th>Cena (PLN/MW/rok)</th><th>Wolumen (MW)</th></tr></thead>
      <tbody>
        <tr><td>2026</td><td>2023-12-15</td><td>220 000</td><td>5 200</td></tr>
        <tr><td>2025</td><td>2022-12-16</td><td>198 000</td><td>4 800</td></tr>
      </tbody>
    </table>
    </body></html>
    """
    requests_mock.get("https://tge.pl/rynek-mocy/wyniki-aukcji", text=html)

    conn = MagicMock()
    cur = MagicMock()
    conn.cursor.return_value.__enter__ = lambda s: cur
    conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    cur.rowcount = 1

    n = scrape_po_capacity_market(conn)
    assert n >= 0
    assert cur.execute.called


def test_scrape_po_capacity_market_handles_http_error(requests_mock):
    from services.po_knowledge.entso_scraper import scrape_po_capacity_market

    requests_mock.get("https://tge.pl/rynek-mocy/wyniki-aukcji", status_code=404)

    conn = MagicMock()
    n = scrape_po_capacity_market(conn)
    assert n == 0
