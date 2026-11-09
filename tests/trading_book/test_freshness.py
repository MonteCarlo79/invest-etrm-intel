"""Per-layer book freshness (libs.trading_book.freshness) — fakes only."""
from datetime import date, timedelta

import pandas as pd
import pytest

from libs.trading_book.asset_book import AssetBook
from libs.trading_book.freshness import book_freshness, classify
from libs.trading_book.registry import get_station

TODAY = date(2026, 11, 9)


def test_classify_ok_stale_missing():
    assert classify("x", TODAY - timedelta(days=2), TODAY, 2).status == "ok"
    stale = classify("x", TODAY - timedelta(days=3), TODAY, 2)
    assert stale.status == "stale" and stale.lag_days == 3
    # D+1 artifact: negative lag is fine
    assert classify("x", TODAY + timedelta(days=1), TODAY, 0).status == "ok"
    missing = classify("x", None, TODAY, 2)
    assert missing.status == "missing" and missing.lag_days is None


def _capture(monkeypatch, latest):
    calls = []

    def fake(sql, engine, params=None):
        calls.append((str(sql), params))
        return pd.DataFrame({"latest": [latest]})

    monkeypatch.setattr(pd, "read_sql", fake)
    return calls


def test_full_book_checks_all_six_layers(monkeypatch):
    _capture(monkeypatch, TODAY)
    rows = book_freshness(None, get_station("suyou"), TODAY)
    assert [r.layer for r in rows] == ["forecast", "strategy", "cleared",
                                       "actual", "attribution", "settled"]
    assert all(r.status == "ok" for r in rows)


def test_tiers_limit_layers(monkeypatch):
    _capture(monkeypatch, TODAY)
    assert [r.layer for r in book_freshness(None, get_station("wuhai"), TODAY)] \
        == ["attribution", "settled"]
    assert book_freshness(None, get_station("wuchuan"), TODAY) == []


def test_keys_follow_reader_conventions(monkeypatch):
    calls = _capture(monkeypatch, TODAY)
    book_freshness(None, get_station("bameng"), TODAY)
    by_table = {sql.split("FROM")[1].split()[0]: p["k"] for sql, p in calls}
    # exact name, never LIKE: the wildcard scan timed out on 36M rows (v54)
    assert by_table["marketdata.md_id_cleared_energy"] == "景怡查干哈达储能电站"
    assert all("LIKE :k" not in sql for sql, _ in calls)
    assert by_table["reports.bess_asset_daily_attribution"] == "hetao"
    assert by_table["reports.book_settled_daily"] == "bameng"
    assert by_table["marketdata.nodal_strategy_daily"] == "景怡查干哈达储能电站"


def test_timestamp_latest_becomes_date(monkeypatch):
    _capture(monkeypatch, pd.Timestamp(f"{TODAY} 23:45"))
    rows = book_freshness(None, get_station("suyou"), TODAY)
    assert rows[2].layer == "cleared" and rows[2].latest == TODAY


def test_null_max_is_missing(monkeypatch):
    _capture(monkeypatch, None)
    rows = book_freshness(None, get_station("suyou"), TODAY)
    assert all(r.status == "missing" for r in rows)


def test_query_failure_is_unavailable_not_raised(monkeypatch):
    def boom(sql, engine, params=None):
        if "book_forecast_daily" in str(sql):
            raise RuntimeError('relation "reports.book_forecast_daily" does not exist')
        return pd.DataFrame({"latest": [TODAY]})

    monkeypatch.setattr(pd, "read_sql", boom)
    rows = AssetBook(None, get_station("suyou")).freshness(TODAY)
    assert rows[0].status == "unavailable" and "does not exist" in rows[0].note
    assert all(r.status == "ok" for r in rows[1:])
