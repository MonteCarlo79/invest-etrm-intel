"""
services/monitoring/tests/conftest.py

Shared fixtures for monitoring service tests.
Integration tests require PGURL env var; they are skipped automatically when absent.
"""
from __future__ import annotations

import os

import pytest


@pytest.fixture(scope="session")
def db_engine():
    """
    SQLAlchemy engine pointed at the test database via PGURL.
    Skips the test session if PGURL is not set.
    """
    pgurl = os.getenv("PGURL")
    if not pgurl:
        pytest.skip("PGURL not set — skipping integration tests")
    from sqlalchemy import create_engine
    return create_engine(pgurl)
