"""WESM LWAP parser: 5-minute interval-ending rows must become 24 hourly means per region."""
import os
import sys
from datetime import date, datetime, timedelta
from unittest.mock import MagicMock

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from services.ph_knowledge.wesm_scraper import WESMPriceScraper

D = date(2026, 11, 22)


def _csv(price_fn, regions=("CLUZ", "CMIN", "CVIS", "SYSTEM")):
    """One trading day as IEMOP publishes it: 288 interval-ENDING stamps, 00:05 .. next-day 00:00."""
    lines = ["RUN_TIME,MKT_TYPE,TIME_INTERVAL,REGION_NAME,LWAP,"]
    start = datetime(D.year, D.month, D.day)
    for i in range(1, 289):
        end = start + timedelta(minutes=5 * i)
        stamp = f"{end.month:02d}/{end.day:02d}/{end.year} {end.strftime('%I:%M:%S %p')}"
        for r in regions:
            lines.append(f"11/22/2026,DIPC,{stamp},{r},{price_fn(i, r)},")
    return "\n".join(lines) + "\n\n"


def test_hourly_mean_of_twelve_intervals():
    # price = interval index (1..288) * 1000 PHP/MWh → hour h mean = (12h + 6.5) PHP/kWh
    recs = WESMPriceScraper()._parse_lwap_csv(_csv(lambda i, r: i * 1000.0), D, "LWAP_orig")
    luz = {r["hour"]: r for r in recs if r["region"] == "Luzon"}
    assert sorted(luz) == list(range(24))
    for h in range(24):
        assert luz[h]["price_php_kwh"] == 12 * h + 6.5
        assert luz[h]["trading_date"] == D
        assert luz[h]["price_type"] == "LWAP_orig"


def test_midnight_interval_belongs_to_hour_23_not_hour_0():
    # Only the last interval (ending next-day 00:00) is non-zero.
    recs = WESMPriceScraper()._parse_lwap_csv(
        _csv(lambda i, r: 12000.0 if i == 288 else 0.0), D, "LWAP_orig")
    luz = {r["hour"]: r["price_php_kwh"] for r in recs if r["region"] == "Luzon"}
    assert luz[23] == 1.0
    assert luz[0] == 0.0


def test_three_regions_system_dropped():
    recs = WESMPriceScraper()._parse_lwap_csv(_csv(lambda i, r: 5000.0), D, "LWAP_orig")
    assert len(recs) == 72
    assert {r["region"] for r in recs} == {"Luzon", "Visayas", "Mindanao"}


def test_garbage_and_short_rows_ignored():
    text = _csv(lambda i, r: 5000.0) + "Total rows: 1152\n,,,\n"
    recs = WESMPriceScraper()._parse_lwap_csv(text, D, "LWAP_orig")
    assert len(recs) == 72
    assert WESMPriceScraper()._parse_lwap_csv("not,a,price,file\n1,2,3,4\n", D) == []


def _conn():
    conn = MagicMock()
    cur = conn.cursor.return_value.__enter__.return_value
    cur.rowcount = 1
    return conn, cur


REC = [{"trading_date": D, "hour": 0, "interval_no": 0, "region": "Luzon",
        "node": "CLUZ", "price_php_kwh": 5.0, "price_type": "LWAP_orig"}]


def test_store_default_never_overwrites():
    conn, cur = _conn()
    WESMPriceScraper().store_prices(conn, REC)
    sql = cur.execute.call_args[0][0]
    assert "DO NOTHING" in sql and "DO UPDATE" not in sql


def test_store_overwrite_updates_only_changed_prices():
    conn, cur = _conn()
    WESMPriceScraper().store_prices(conn, REC, overwrite=True)
    sql = cur.execute.call_args[0][0]
    assert "DO UPDATE" in sql and "IS DISTINCT FROM" in sql
