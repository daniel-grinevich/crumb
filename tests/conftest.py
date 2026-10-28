"""Shared fixtures.

Integration tests use throwaway databases (crumb_test_wh, crumb_test_navi) on
the compose warehouse server (:5440) and are skipped if it isn't reachable.
"""

import datetime as dt
import os
from collections.abc import Iterator
from typing import Any

import psycopg
import pytest
from psycopg import sql

from crumb import warehouse as wh
from crumb.config import get_settings
from crumb.db import connect
from crumb.seed.generate import generate
from crumb.seed.generate import load
from crumb.temporal.activities import all_raw_tables

ADMIN_DSN = os.environ.get("CRUMB_TEST_ADMIN_DSN", "postgresql://crumb:crumb@localhost:5440/crumb")
TEST_SALT = "test-salt-0123456789abcdef"
TEST_API_KEY = "test-api-key"
# Fixed "now" so seeded data is deterministic across runs.
SEED_NOW = dt.datetime(2026, 10, 1, 12, 0, tzinfo=dt.UTC)


def _dsn(db: str) -> str:
    base, _, _ = ADMIN_DSN.rpartition("/")
    return f"{base}/{db}"


def _recreate(db: str) -> None:
    with psycopg.connect(ADMIN_DSN, autocommit=True) as conn:
        ident = sql.Identifier(db)
        conn.execute(sql.SQL("drop database if exists {} with (force)").format(ident))
        conn.execute(sql.SQL("create database {}").format(ident))


@pytest.fixture(scope="session")
def pg_available() -> None:
    try:
        psycopg.connect(ADMIN_DSN, connect_timeout=2).close()
    except psycopg.OperationalError:
        pytest.skip("warehouse Postgres on :5440 not reachable (run `just up`)")


@pytest.fixture(scope="session")
def navi_dsn(pg_available: None) -> str:
    _recreate("crumb_test_navi")
    dsn = _dsn("crumb_test_navi")
    with connect(dsn, autocommit=True) as conn:
        load(conn, generate(days=10, users=40, seed=3, now=SEED_NOW))
    return dsn


@pytest.fixture
def warehouse_dsn(pg_available: None) -> Iterator[str]:
    """A fresh, bootstrapped warehouse per test."""
    _recreate("crumb_test_wh")
    dsn = _dsn("crumb_test_wh")
    with connect(dsn, autocommit=True) as conn:
        wh.bootstrap(conn, all_raw_tables())
    yield dsn


@pytest.fixture
def settings_env(
    monkeypatch: pytest.MonkeyPatch, warehouse_dsn: str, navi_dsn: str
) -> Iterator[None]:
    """Point get_settings() (used by activities and the API) at the test DBs."""
    for key in list(os.environ):
        if key.startswith("CRUMB_") or key in ("STRIPE_API_KEY", "STRIPE_SECRET_KEY", "PII_SALT"):
            monkeypatch.delenv(key, raising=False)
    monkeypatch.chdir(os.path.dirname(__file__))  # don't read the repo's .env
    monkeypatch.setenv("CRUMB_WAREHOUSE_DSN", warehouse_dsn)
    monkeypatch.setenv("CRUMB_NAVI_DSN", navi_dsn)
    monkeypatch.setenv("CRUMB_PII_SALT", TEST_SALT)
    monkeypatch.setenv("CRUMB_API_KEY", TEST_API_KEY)
    monkeypatch.setenv("CRUMB_FRESHNESS_MAX_AGE_HOURS", "1")
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


@pytest.fixture
def wh_conn(warehouse_dsn: str) -> Iterator[psycopg.Connection[dict[str, Any]]]:
    with connect(warehouse_dsn, autocommit=True) as conn:
        yield conn
