"""Tests for the monthly 代购价格 scrape cadence."""
import sys
import types

import pytest

from services.hermes import news_screener as ns


def test_cadence_of_defaults_daily():
    assert ns._cadence_of({"name": "x"}) == "daily"
    assert ns._cadence_of({"name": "x", "scrape_config": None}) == "daily"
    assert ns._cadence_of({"name": "x", "scrape_config": {}}) == "daily"
    assert ns._cadence_of({"name": "x", "scrape_config": {"cadence": "monthly"}}) == "monthly"


def _fake_conn(rowcount):
    class _Cur:
        def execute(self, *a):
            pass

        def fetchone(self):
            return (rowcount,)

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

    class _Conn:
        def cursor(self):
            return _Cur()

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

    return _Conn()


def test_monthly_done_threshold(monkeypatch):
    monkeypatch.setattr(ns.psycopg2, "connect", lambda *a, **k: _fake_conn(10))
    assert ns._monthly_sysopfee_done("pg", "2026-12-01") is True

    monkeypatch.setattr(ns.psycopg2, "connect", lambda *a, **k: _fake_conn(3))
    assert ns._monthly_sysopfee_done("pg", "2026-12-01") is False


def test_monthly_wrapper_skips_when_populated(monkeypatch):
    monkeypatch.setattr(ns, "_monthly_sysopfee_done", lambda pg, m: True)
    called = []
    monkeypatch.setattr(ns, "screen_news_sources",
                        lambda *a, **k: called.append(k))
    out = ns.screen_price_sources_monthly("pg", "k", None, "")
    assert out["skipped"] == "already_populated"
    assert called == []


def test_monthly_wrapper_scrapes_when_missing(monkeypatch):
    monkeypatch.setattr(ns, "_monthly_sysopfee_done", lambda pg, m: False)
    seen = {}
    monkeypatch.setattr(ns, "screen_news_sources",
                        lambda pg, key, feishu, oid, cadence="daily": seen.update(cadence=cadence) or {"ok": 1})
    out = ns.screen_price_sources_monthly("pg", "k", None, "")
    assert seen["cadence"] == "monthly"
    assert out == {"ok": 1}
