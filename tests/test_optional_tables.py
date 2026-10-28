"""Older Navi deployments lack newer tables/columns: optional ones are skipped
and recorded, required ones still fail."""

import dataclasses
import datetime as dt
from collections.abc import Iterator
from typing import Any

import psycopg
import pytest
from fastapi.testclient import TestClient
from psycopg import sql
from temporalio.testing import ActivityEnvironment

from crumb.api.app import create_app
from crumb.config import get_settings
from crumb.db import connect
from crumb.seed.generate import generate
from crumb.seed.generate import load
from crumb.sources import navi
from crumb.temporal import activities as act
from crumb.temporal.types import ExtractTableInput
from crumb.temporal.types import StepInput
from crumb.transform import affected_models
from crumb.transform import parse_model
from tests.conftest import SEED_NOW
from tests.conftest import TEST_API_KEY
from tests.conftest import TEST_SALT
from tests.conftest import _dsn
from tests.conftest import _recreate

# Roughly navi as deployed on staging (Sept 20): no device event log, no
# per-level award history, no refund columns, no test-order flag.
MISSING_TABLES = ("devices_deviceevent", "awards_userawardlevel")
MISSING_COLUMNS = {
    "payments_payment": ("amount_refunded", "stripe_refund_id"),
    "orders_order": ("is_test",),
    "devices_raspberrypi": ("is_simulated",),
}

Conn = psycopg.Connection[dict[str, Any]]


def test_required_tables_are_the_core_ones() -> None:
    required = {t for t, spec in navi.TABLES.items() if spec.required}
    assert required == {"orders_order", "orders_orderitem", "payments_payment", "devices_naviport"}


def test_affected_models_is_transitive() -> None:
    models = [
        parse_model("staging", "s", "select * from raw.a"),
        parse_model("staging", "t", "select * from raw.b"),
        parse_model("marts", "m", "select * from staging.s"),
        parse_model("marts", "n", "select * from marts.m join staging.t using (x)"),
    ]
    assert affected_models(models, {"a"}) == {
        "staging.s": ["a"],
        "marts.m": ["a"],
        "marts.n": ["a"],
    }
    assert affected_models(models, set()) == {}


@pytest.fixture(scope="module")
def old_navi_dsn(pg_available: None) -> str:
    _recreate("crumb_test_navi_old")
    dsn = _dsn("crumb_test_navi_old")
    with connect(dsn, autocommit=True) as conn:
        load(conn, generate(days=10, users=40, seed=3, now=SEED_NOW))
        for table in MISSING_TABLES:
            conn.execute(sql.SQL("drop table {} cascade").format(sql.Identifier(table)))
        for table, cols in MISSING_COLUMNS.items():
            for col in cols:
                conn.execute(
                    sql.SQL("alter table {} drop column {}").format(
                        sql.Identifier(table), sql.Identifier(col)
                    )
                )
    return dsn


@pytest.fixture
def old_navi_env(
    settings_env: None, monkeypatch: pytest.MonkeyPatch, old_navi_dsn: str
) -> Iterator[None]:
    monkeypatch.setenv("CRUMB_NAVI_DSN", old_navi_dsn)
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


def extract_all() -> list[dict[str, Any]]:
    env = ActivityEnvironment()
    return [env.run(act.extract_navi_table, ExtractTableInput("run-old", t)) for t in navi.TABLES]


@pytest.mark.integration
@pytest.mark.usefixtures("old_navi_env")
def test_old_navi_runs_end_to_end(wh_conn: Conn) -> None:
    results = {r["table"]: r for r in extract_all()}
    for table in MISSING_TABLES:
        assert results[table]["status"] == navi.TABLE_MISSING
        assert results[table]["rows"] == 0
    assert results["orders_order"]["status"] == "ok"
    assert results["orders_order"]["rows"] > 0
    assert results["payments_payment"]["missing_columns"] == ["amount_refunded", "stripe_refund_id"]

    statuses = {
        r["table_name"]: r["status"]
        for r in wh_conn.execute("select table_name, status from meta.watermarks").fetchall()
    }
    assert {t for t, s in statuses.items() if s != "ok"} == set(MISSING_TABLES)

    env = ActivityEnvironment()
    built = env.run(act.run_transform, StepInput("run-old"))
    assert built["rows"]["marts.daily_port_sales"] > 0
    assert built["rows"]["marts.machine_uptime_daily"] == 0
    degraded = built["degraded"]
    assert "marts.machine_uptime_daily" in degraded
    assert "navi_devices_deviceevent" in degraded["marts.machine_uptime_daily"]
    assert "marts.daily_port_sales" not in degraded

    summary = env.run(act.run_quality_checks, StepInput("run-old"))
    assert summary["failed"] == 0
    freshness = wh_conn.execute(
        "select status, message from meta.dq_results where check_name = %s",
        ("freshness:navi.devices_deviceevent",),
    ).fetchone()
    assert freshness == {"status": "skipped", "message": navi.TABLE_MISSING}

    # A table that comes back (Navi upgraded) resumes and is marked ok again.
    with connect(_dsn("crumb_test_navi"), read_only=True) as full_navi:
        navi.extract_table(
            navi.TABLES["devices_deviceevent"],
            navi=full_navi,
            warehouse=wh_conn,
            salt=TEST_SALT.encode(),
            run_id="run-new",
            page_size=1000,
            lookback=dt.timedelta(0),
            full_refresh=False,
            resume_from=None,
            heartbeat=lambda _: None,
        )
    status = wh_conn.execute(
        "select status from meta.watermarks where table_name = 'devices_deviceevent'"
    ).fetchone()
    assert status == {"status": "ok"}


@pytest.mark.integration
@pytest.mark.usefixtures("old_navi_env", "navi_dsn")
def test_missing_required_table_still_fails(wh_conn: Conn, old_navi_dsn: str) -> None:
    required_events = dataclasses.replace(navi.TABLES["devices_deviceevent"], required=True)
    with (
        connect(old_navi_dsn, read_only=True) as src,
        pytest.raises(navi.MissingTableError, match="required navi table devices_deviceevent"),
    ):
        navi.extract_table(
            required_events,
            navi=src,
            warehouse=wh_conn,
            salt=TEST_SALT.encode(),
            run_id="r",
            page_size=100,
            lookback=dt.timedelta(0),
            full_refresh=False,
            resume_from=None,
            heartbeat=lambda _: None,
        )


@pytest.mark.integration
@pytest.mark.usefixtures("old_navi_env")
def test_freshness_reports_table_status_and_requiredness() -> None:
    extract_all()
    with TestClient(create_app()) as client:
        resp = client.get("/freshness", headers={"X-API-Key": TEST_API_KEY})
    assert resp.status_code == 200
    rows = {r["table"]: r for r in resp.json()}
    assert all(r["table"] and r["table"] == r["table_name"] for r in rows.values())
    events = rows["devices_deviceevent"]
    assert events["status"] == navi.TABLE_MISSING
    assert events["required"] is False and events["is_stale"] is False
    orders = rows["orders_order"]
    assert orders["status"] == "ok" and orders["required"] is True
    assert orders["last_success_at"] is not None and orders["is_stale"] is False
