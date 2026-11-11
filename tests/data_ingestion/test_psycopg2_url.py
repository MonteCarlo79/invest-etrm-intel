"""Bare postgres URLs must pin psycopg2 — the image has no psycopg v3
(tt_api/enos_market collectors crashed daily 2026-09-27 → 2026-11-11)."""
from services.data_ingestion.shared.db import psycopg2_url


def test_bare_urls_pinned():
    assert psycopg2_url("postgresql://u:p@h:5432/d") == "postgresql+psycopg2://u:p@h:5432/d"
    assert psycopg2_url("postgres://u:p@h/d") == "postgresql+psycopg2://u:p@h/d"


def test_explicit_driver_untouched():
    for u in ("postgresql+psycopg2://u@h/d", "postgresql+psycopg://u@h/d", "sqlite://"):
        assert psycopg2_url(u) == u


def test_engine_uses_psycopg2(monkeypatch):
    from services.data_ingestion.shared import db
    monkeypatch.setenv("PGURL", "postgresql://u:p@localhost:5432/d")
    assert db.get_engine().dialect.driver == "psycopg2"
