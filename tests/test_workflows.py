"""Workflow orchestration with Temporal's time-skipping test server and mocked
activities (registered under the real activity names)."""

import uuid
from collections.abc import AsyncIterator
from concurrent.futures import ThreadPoolExecutor
from typing import Any

import pytest
from temporalio import activity
from temporalio.client import WorkflowFailureError
from temporalio.exceptions import ApplicationError
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from crumb.sources import navi
from crumb.temporal import workflows as wf
from crumb.temporal.pipelines import PIPELINES
from crumb.temporal.types import ExtractParams
from crumb.temporal.types import ExtractTableInput
from crumb.temporal.types import PipelineParams
from crumb.temporal.types import RunFinished
from crumb.temporal.types import RunStarted
from crumb.temporal.types import StepInput
from crumb.temporal.types import TransformParams

QUEUE = "crumb-test"


class Fakes:
    def __init__(self) -> None:
        self.runs: dict[str, dict[str, Any]] = {}
        self.attempts: dict[str, int] = {}
        self.stripe = False
        self.flaky_table: str | None = None
        self.missing_table: str | None = None
        self.missing_required = False
        self.fail_quality = False
        self.calls: list[str] = []

    def activities(self) -> list[Any]:
        @activity.defn(name="record_run_started")
        def record_run_started(run: RunStarted) -> None:
            self.runs[run.run_id] = {
                "type": run.workflow_type,
                "status": "running",
                "parent": run.parent_run_id,
            }

        @activity.defn(name="record_run_finished")
        def record_run_finished(run: RunFinished) -> None:
            self.runs[run.run_id].update(status=run.status, error=run.error)

        @activity.defn(name="extract_navi_table")
        def extract_navi_table(inp: ExtractTableInput) -> dict[str, Any]:
            self.attempts[inp.table] = self.attempts.get(inp.table, 0) + 1
            if inp.table == self.flaky_table and self.attempts[inp.table] < 3:
                raise RuntimeError("replica connection reset")
            if inp.table == self.missing_table:
                if self.missing_required:
                    raise navi.MissingTableError(f"required navi table {inp.table} not found")
                return {
                    "table": inp.table,
                    "rows": 0,
                    "pages": 0,
                    "watermark": None,
                    "status": navi.TABLE_MISSING,
                    "missing_columns": [],
                }
            self.calls.append(f"navi:{inp.table}")
            return {
                "table": inp.table,
                "rows": 10,
                "pages": 1,
                "watermark": None,
                "status": "ok",
                "missing_columns": [],
            }

        @activity.defn(name="stripe_configured")
        def stripe_configured() -> bool:
            return self.stripe

        @activity.defn(name="extract_stripe_object")
        def extract_stripe_object(inp: ExtractTableInput) -> dict[str, Any]:
            self.calls.append(f"stripe:{inp.table}")
            return {"table": inp.table, "rows": 3, "pages": 1, "watermark": None}

        @activity.defn(name="run_transform")
        def run_transform(inp: StepInput) -> dict[str, Any]:
            self.calls.append("transform")
            return {"models": 25, "rows": {}}

        @activity.defn(name="run_quality_checks")
        def run_quality_checks(inp: StepInput) -> dict[str, Any]:
            self.calls.append("quality")
            if self.fail_quality:
                raise ApplicationError(
                    "1 data-quality check(s) failed: unique:marts.fct_orders",
                    type="DataQualityError",
                    non_retryable=True,
                )
            return {"passed": 10, "skipped": 0, "failed": 0, "failures": []}

        return [
            record_run_started,
            record_run_finished,
            extract_navi_table,
            stripe_configured,
            extract_stripe_object,
            run_transform,
            run_quality_checks,
        ]


@pytest.fixture(scope="module")
async def env() -> AsyncIterator[WorkflowEnvironment]:
    async with await WorkflowEnvironment.start_time_skipping() as e:
        yield e


async def run(env: WorkflowEnvironment, fakes: Fakes, workflow: Any, arg: Any) -> Any:
    with ThreadPoolExecutor(8) as pool:
        async with Worker(
            env.client,
            task_queue=QUEUE,
            workflows=wf.ALL,
            activities=fakes.activities(),
            activity_executor=pool,
        ):
            return await env.client.execute_workflow(
                workflow, arg, id=f"test-{uuid.uuid4()}", task_queue=QUEUE
            )


async def test_pipeline_extracts_in_parallel_then_transforms(env: WorkflowEnvironment) -> None:
    fakes = Fakes()
    fakes.stripe = True
    result = await run(env, fakes, wf.PipelineWorkflow.run, PipelineParams())
    assert result["status"] == "succeeded"
    assert fakes.calls[-2:] == ["transform", "quality"]  # after every extract
    assert {c for c in fakes.calls if c.startswith("navi:")} == {f"navi:{t}" for t in navi.TABLES}
    assert sum(c.startswith("stripe:") for c in fakes.calls) == 3
    types = sorted(r["type"] for r in fakes.runs.values())
    assert types == [
        "ExtractNaviWorkflow",
        "ExtractStripeWorkflow",
        "PipelineWorkflow",
        "TransformWorkflow",
    ]
    assert all(r["status"] == "succeeded" for r in fakes.runs.values())
    parent = next(k for k, r in fakes.runs.items() if r["type"] == "PipelineWorkflow")
    assert all(
        r["parent"] == parent for r in fakes.runs.values() if r["type"] != "PipelineWorkflow"
    )


async def test_stripe_without_key_is_skipped_not_failed(env: WorkflowEnvironment) -> None:
    fakes = Fakes()
    result = await run(env, fakes, wf.PipelineWorkflow.run, PipelineParams(run_checks=False))
    assert result["status"] == "succeeded"
    stripe_run = next(r for r in fakes.runs.values() if r["type"] == "ExtractStripeWorkflow")
    assert stripe_run["status"] == "skipped"
    assert "quality" not in fakes.calls


async def test_transient_extract_errors_are_retried(env: WorkflowEnvironment) -> None:
    fakes = Fakes()
    fakes.flaky_table = "orders_order"
    result = await run(
        env, fakes, wf.ExtractNaviWorkflow.run, ExtractParams(tables=["orders_order"])
    )
    assert result["status"] == "succeeded"
    assert fakes.attempts["orders_order"] == 3


async def test_quality_failure_fails_and_records_run(env: WorkflowEnvironment) -> None:
    fakes = Fakes()
    fakes.fail_quality = True
    with pytest.raises(WorkflowFailureError) as err:
        await run(env, fakes, wf.TransformWorkflow.run, TransformParams(run_checks=True))
    assert "unique:marts.fct_orders" in wf.describe_error(err.value)
    (record,) = fakes.runs.values()
    assert record["status"] == "failed"
    assert "DataQualityError" not in (record["error"] or "") or "unique" in record["error"]
    assert fakes.calls.count("quality") == 1  # non-retryable


async def test_missing_optional_table_is_reported_not_fatal(env: WorkflowEnvironment) -> None:
    fakes = Fakes()
    fakes.missing_table = "awards_userawardlevel"
    result = await run(env, fakes, wf.ExtractNaviWorkflow.run, ExtractParams())
    assert result["status"] == "succeeded"
    assert result["skipped_tables"] == {"awards_userawardlevel": navi.TABLE_MISSING}


async def test_missing_required_table_fails_without_retrying(env: WorkflowEnvironment) -> None:
    fakes = Fakes()
    fakes.missing_table = "orders_order"
    fakes.missing_required = True
    with pytest.raises(WorkflowFailureError) as err:
        await run(env, fakes, wf.ExtractNaviWorkflow.run, ExtractParams(tables=["orders_order"]))
    assert "required navi table orders_order not found" in wf.describe_error(err.value)
    assert fakes.attempts["orders_order"] == 1
    (record,) = fakes.runs.values()
    assert record["status"] == "failed"


def test_pipeline_registry_targets_real_workflows() -> None:
    names = {w.__name__ for w in wf.ALL}
    assert all(p.workflow in names for p in PIPELINES.values())
