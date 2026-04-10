"""Pytest configuration for data_ingestion smoke tests."""
import os
import pytest


def pytest_configure(config):
    """Skip all tests if PGURL is not set (CI without DB access)."""
    pass


@pytest.fixture(scope="session")
def engine():
    """Return a SQLAlchemy engine using PGURL env var."""
    pgurl = os.environ.get("PGURL")
    if not pgurl:
        pytest.skip("PGURL not set — skipping DB tests")
    from sqlalchemy import create_engine
    return create_engine(pgurl, pool_pre_ping=True)
