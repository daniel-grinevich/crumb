"""Run-status bookkeeping (cancel / terminate / sibling failures) and the
re-read of rows Navi changes without bumping updated_at."""

import datetime as dt
import uuid
from collections.abc import AsyncIterator
from typing import Any

import psycopg
import pytest
from fastapi.testclient import TestClient
from psycopg import sql
from temporalio.client import WorkflowExecutionStatus
from temporalio.testing import ActivityEnvironment
from temporalio.testing import WorkflowEnvironment

from crumb import warehouse as wh
from crumb.api.app import create_app
from crumb.config import Settings
from crumb.db import connect
from crumb.sources import navi
from crumb.temporal import activities as act
from crumb.temporal import workflows as wf
from crumb.temporal.runsync import sync_open_runs
from crumb.temporal.types import ExtractTableInput
from crumb.temporal.types import PipelineParams
from crumb.temporal.types import StepInput
from tests.conftest import TEST_API_KEY
from tests.conftest import TEST_SALT
from tests.test_workflows import QUEUE
from tests.test_workflows import Fakes

Conn = psycopg.Connection[dict[str, Any]]


# --- config ---------------------------------------------------------------


@pytest.mark.parametrize(
    ("env", "explicit", "key", "expected"),
    [
        ("staging", None, "sk_test_x", "warn"),
        ("local", None, None, "warn"),
        ("production", None, "sk_test_x", "fail"),
        ("staging", None, "sk_live_x", "fail"),
        ("production", "warn", "sk_live_x", "warn"),
        ("staging", "fail", None, "fail"),
    ],
)
def test_reconciliation_severity_defaults(
    monkeypatch: pytest.MonkeyPatch, env: str, explicit: str | None, key: str | None, expected: str
) -> None:
    for var in (
        "CRUMB_RECONCILIATION_SEVERITY",
        "STRIPE_API_KEY",
        "CRUMB_STRIPE_API_KEY",
        "STRIPE_SECRET_KEY",
        "ENVIRONMENT",
        "CRUMB_ENVIRONMENT",
    ):
        monkeypatch.delenv(var, raising=False)
    monkeypatch.setenv("CRUMB_ENVIRONMENT", env)
    if explicit:
        monkeypatch.setenv("CRUMB_RECONCILIATION_SEVERITY", explicit)
    if key:
        monkeypatch.setenv("STRIPE_API_KEY", key)
    settings = Settings(_env_file=None)  # pyright: ignore[reportCallIssue]
    assert settings.effective_reconciliation_severity() == expected


# --- Navi status changes without updated_at -----------------------------------


def extract(table: str) -> dict[str, Any]:
    return ActivityEnvironment().run(act.extract_navi_table, ExtractTableInput("run-r", table))


@pytest.mark.integration
@pytest.mark.usefixtures("settings_env")
def test_status_change_without_updated_at_is_picked_up(wh_conn: Conn, navi_dsn: str) -> None:
    with connect(navi_dsn, autocommit=True) as src:
        order = (
            src.execute(
                "select id, updated_at from orders_order where order_status = 'O' limit 1"
            ).fetchone()
            or src.execute("select id, updated_at from orders_order limit 1").fetchone()
        )
        assert order is not None
        # A recent order, as of the first extract.
        src.execute(
            "update orders_order set created_at = now() - interval '1 hour', order_status = 'O' "
            "where id = %s",
            (order["id"],),
        )
        extract("orders_order")
        # Navi's claim/complete path: queryset .update() leaves updated_at alone.
        src.execute("update orders_order set order_status = 'D' where id = %s", (order["id"],))
        port = src.execute("select id from devices_naviport limit 1").fetchone()
        pi = src.execute("select id from devices_raspberrypi offset 1 limit 1").fetchone()
        assert port is not None and pi is not None
        extract("devices_naviport")
        src.execute(
            "update devices_naviport set raspberry_pi_id = %s where id = %s", (pi["id"], port["id"])
        )
    result = extract("orders_order")
    assert result["recent_rows"] >= 1
    landed = wh_conn.execute(
        "select data->>'order_status' as status, source_ts from raw.navi_orders_order where id = %s",
        (str(order["id"]),),
    ).fetchone()
    assert landed == {"status": "D", "source_ts": order["updated_at"]}
    extract("devices_naviport")
    assert wh_conn.execute(
        "select data->>'raspberry_pi_id' as pi from raw.navi_devices_naviport where id = %s",
        (str(port["id"]),),
    ).fetchone() == {"pi": str(pi["id"])}


@pytest.mark.integration
def test_resume_in_recent_phase_keeps_watermark(wh_conn: Conn, navi_dsn: str) -> None:
    spec = navi.TABLES["orders_order"]
    common: dict[str, Any] = {
        "salt": TEST_SALT.encode(),
        "run_id": "r",
        "page_size": 100,
        "lookback": dt.timedelta(0),
        "full_refresh": False,
        "heartbeat": lambda _: None,
    }
    with connect(navi_dsn, read_only=True) as src:
        navi.extract_table(spec, navi=src, warehouse=wh_conn, resume_from=None, **common)
        before = wh.get_watermark(wh_conn, "navi", spec.table)
        wh_conn.execute(sql.SQL("truncate {}").format(sql.Identifier("raw", spec.raw_table)))
        # Resuming in the recent phase must not redo (or move) the incremental pass.
        result = navi.extract_table(
            spec,
            navi=src,
            warehouse=wh_conn,
            resume_from=None,
            resume_phase="recent",
            recent_window=dt.timedelta(days=3650),
            **common,
        )
    assert result.rows == 0 and result.recent_rows > 0
    assert wh.get_watermark(wh_conn, "navi", spec.table) == before


# --- workflow run status -------------------------------------------------


@pytest.fixture(scope="module")
async def tenv() -> AsyncIterator[WorkflowEnvironment]:
    async with await WorkflowEnvironment.start_time_skipping() as e:
        yield e


async def test_pipeline_waits_for_sibling_before_failing(tenv: WorkflowEnvironment) -> None:
    from concurrent.futures import ThreadPoolExecutor

    from temporalio.client import WorkflowFailureError
    from temporalio.worker import Worker

    fakes = Fakes()
    fakes.stripe = True
    fakes.missing_table = "orders_order"
    fakes.missing_required = True
    fakes.slow_stripe = True
    with ThreadPoolExecutor(8) as pool:
        async with Worker(
            tenv.client,
            task_queue=QUEUE,
            workflows=wf.ALL,
            activities=fakes.activities(),
            activity_executor=pool,
        ):
            with pytest.raises(WorkflowFailureError):
                await tenv.client.execute_workflow(
                    wf.PipelineWorkflow.run,
                    PipelineParams(),
                    id=f"t-{uuid.uuid4()}",
                    task_queue=QUEUE,
                )
    statuses = {r["type"]: r["status"] for r in fakes.runs.values()}
    assert statuses == {
        "PipelineWorkflow": "failed",
        "ExtractNaviWorkflow": "failed",
        # Before the fix this child was terminated mid-run and stayed 'running'.
        "ExtractStripeWorkflow": "succeeded",
    }


async def test_cancelled_run_is_recorded_as_cancelled(tenv: WorkflowEnvironment) -> None:
    import asyncio
    from concurrent.futures import ThreadPoolExecutor

    from temporalio.client import WorkflowFailureError
    from temporalio.worker import Worker

    fakes = Fakes()
    fakes.block_transform = True
    with ThreadPoolExecutor(8) as pool:
        async with Worker(
            tenv.client,
            task_queue=QUEUE,
            workflows=wf.ALL,
            activities=fakes.activities(),
            activity_executor=pool,
        ):
            handle = await tenv.client.start_workflow(
                wf.TransformWorkflow.run,
                wf.TransformParams(),
                id=f"t-{uuid.uuid4()}",
                task_queue=QUEUE,
            )
            for _ in range(100):
                if "transform-started" in fakes.calls:
                    break
                await asyncio.sleep(0.05)
            await handle.cancel()
            with pytest.raises(WorkflowFailureError):
                await handle.result()
    (record,) = fakes.runs.values()
    assert record["status"] == "cancelled"


@pytest.mark.integration
async def test_sync_marks_terminated_runs(tenv: WorkflowEnvironment, wh_conn: Conn) -> None:
    from concurrent.futures import ThreadPoolExecutor

    from temporalio.worker import Worker

    fakes = Fakes()
    fakes.block_transform = True
    with ThreadPoolExecutor(8) as pool:
        async with Worker(
            tenv.client,
            task_queue=QUEUE,
            workflows=wf.ALL,
            activities=fakes.activities(),
            activity_executor=pool,
        ):
            handle = await tenv.client.start_workflow(
                wf.TransformWorkflow.run,
                wf.TransformParams(),
                id=f"t-{uuid.uuid4()}",
                task_queue=QUEUE,
            )
            await handle.terminate("operator")
            desc = await handle.describe()
    assert desc.status == WorkflowExecutionStatus.TERMINATED
    run_id = handle.result_run_id
    assert run_id
    wh.record_run_started(
        wh_conn,
        run_id=run_id,
        workflow_id=handle.id,
        workflow_type="TransformWorkflow",
        parent_run_id=None,
        params={},
    )
    wh.record_run_started(
        wh_conn,
        run_id="still-open",
        workflow_id="nope",
        workflow_type="TransformWorkflow",
        parent_run_id=None,
        params={},
    )
    fixed = await sync_open_runs(tenv.client, wh_conn)
    assert fixed == 1
    row = wh_conn.execute(
        "select status, finished_at, error from meta.runs where run_id = %s", (run_id,)
    ).fetchone()
    assert row is not None
    assert row["status"] == "terminated" and row["finished_at"] is not None
    assert "terminated" in row["error"]
    assert wh_conn.execute(
        "select status from meta.runs where run_id = 'still-open'"
    ).fetchone() == {"status": "running"}


# --- API --------------------------------------------------------------------


@pytest.mark.integration
@pytest.mark.usefixtures("settings_env")
def test_quality_endpoint_shows_latest_results_with_warnings(wh_conn: Conn) -> None:
    for t in navi.TABLES:
        extract(t)
    env = ActivityEnvironment()
    env.run(act.run_transform, StepInput("q1"))
    env.run(act.run_quality_checks, StepInput("q1"))
    wh_conn.execute(
        "insert into meta.dq_results (run_id, check_name, status, message) "
        "values ('q2', 'reconciliation', 'warned', 'Stripe/Navi mismatches: double_charge=2')"
    )
    with TestClient(create_app()) as client:
        resp = client.get("/quality", headers={"X-API-Key": TEST_API_KEY})
        warned = client.get(
            "/quality", params={"status": "warned"}, headers={"X-API-Key": TEST_API_KEY}
        ).json()
    assert resp.status_code == 200
    body = resp.json()
    assert body[0]["check_name"] == "reconciliation" and body[0]["status"] == "warned"
    assert len({r["check_name"] for r in body}) == len(body)  # latest result per check
    assert [r["check_name"] for r in warned] == ["reconciliation"]
