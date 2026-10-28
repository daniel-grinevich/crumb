"""Temporal workflows.

    PipelineWorkflow
      ├─ ExtractNaviWorkflow    (child, parallel)  one activity per table
      ├─ ExtractStripeWorkflow  (child, parallel)  one activity per object
      └─ TransformWorkflow      (child)            staging -> marts, then DQ checks

Every workflow records itself in ``meta.runs`` (start, then succeeded / failed /
skipped) using its Temporal run id as the key.
"""

import asyncio
from collections.abc import Awaitable
from collections.abc import Callable
from dataclasses import asdict
from datetime import timedelta
from typing import Any

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import FailureError
from temporalio.exceptions import is_cancelled_exception

with workflow.unsafe.imports_passed_through():
    from crumb.sources import navi
    from crumb.sources import stripe_source
    from crumb.temporal import activities as act
    from crumb.temporal.types import ExtractParams
    from crumb.temporal.types import ExtractTableInput
    from crumb.temporal.types import PipelineParams
    from crumb.temporal.types import RunFinished
    from crumb.temporal.types import RunStarted
    from crumb.temporal.types import StepInput
    from crumb.temporal.types import TransformParams

NON_RETRYABLE = [
    "ConfigError",
    "DataQualityError",
    "MissingTableError",
    "LookupError",
    "KeyError",
]

EXTRACT_RETRY = RetryPolicy(
    initial_interval=timedelta(seconds=5),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(minutes=5),
    maximum_attempts=5,
    non_retryable_error_types=NON_RETRYABLE,
)
BOOKKEEPING_RETRY = RetryPolicy(
    initial_interval=timedelta(seconds=1),
    maximum_interval=timedelta(seconds=30),
    maximum_attempts=10,
)
TRANSFORM_RETRY = RetryPolicy(
    initial_interval=timedelta(seconds=10),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(minutes=5),
    maximum_attempts=3,
    non_retryable_error_types=NON_RETRYABLE,
)


class Skipped(Exception):  # noqa: N818 - control flow, not an error
    """Raised inside a run body to finish the run as 'skipped'."""

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


def describe_error(err: BaseException) -> str:
    """Flatten Temporal's error chain (ActivityError -> ApplicationError ...)."""
    parts: list[str] = []
    current: BaseException | None = err
    while current is not None:
        if str(current) and str(current) not in parts:
            parts.append(str(current))
        current = current.cause if isinstance(current, FailureError) else current.__cause__
    return " <- ".join(parts) or type(err).__name__


async def tracked(
    params: dict[str, Any], body: Callable[[str], Awaitable[dict[str, Any]]]
) -> dict[str, Any]:
    info = workflow.info()
    run_id = info.run_id
    await workflow.execute_activity(
        act.record_run_started,
        RunStarted(
            run_id=run_id,
            workflow_id=info.workflow_id,
            workflow_type=info.workflow_type,
            parent_run_id=info.parent.run_id if info.parent else None,
            params=params,
        ),
        start_to_close_timeout=timedelta(seconds=30),
        retry_policy=BOOKKEEPING_RETRY,
    )
    status, error, stats = "succeeded", None, {}
    try:
        stats = await body(run_id)
    except Skipped as skip:
        status, stats = "skipped", {"reason": skip.reason}
    except (FailureError, asyncio.CancelledError) as err:
        # A cancelled workflow may still run activities after catching the
        # cancellation, so the run is always closed out before re-raising.
        if is_cancelled_exception(err):
            status, error = "cancelled", "cancelled"
        else:
            status, error = "failed", describe_error(err)
        await _finish(RunFinished(run_id, status, stats, error))
        raise
    await _finish(RunFinished(run_id, status, stats, error))
    return {"run_id": run_id, "status": status, **stats}


async def _finish(run: RunFinished) -> None:
    await workflow.execute_activity(
        act.record_run_finished,
        run,
        start_to_close_timeout=timedelta(seconds=30),
        retry_policy=BOOKKEEPING_RETRY,
    )


def _extract_activity(fn: Any, inp: ExtractTableInput) -> Awaitable[dict[str, Any]]:
    return workflow.execute_activity(
        fn,
        inp,
        start_to_close_timeout=timedelta(hours=2),
        heartbeat_timeout=timedelta(minutes=2),
        retry_policy=EXTRACT_RETRY,
    )


@workflow.defn
class ExtractNaviWorkflow:
    @workflow.run
    async def run(self, params: ExtractParams) -> dict[str, Any]:
        tables = params.tables or list(navi.TABLES)

        async def body(run_id: str) -> dict[str, Any]:
            results = await asyncio.gather(
                *[
                    _extract_activity(
                        act.extract_navi_table,
                        ExtractTableInput(run_id, t, params.full_refresh),
                    )
                    for t in tables
                ]
            )
            skipped = {r["table"]: r["status"] for r in results if r.get("status", "ok") != "ok"}
            if skipped:
                workflow.logger.warning("navi tables skipped: %s", skipped)
            return {
                "rows": sum(r["rows"] for r in results),
                "skipped_tables": skipped,
                "tables": results,
            }

        return await tracked(asdict(params), body)


@workflow.defn
class ExtractStripeWorkflow:
    @workflow.run
    async def run(self, params: ExtractParams) -> dict[str, Any]:
        objects = params.tables or list(stripe_source.OBJECTS)

        async def body(run_id: str) -> dict[str, Any]:
            configured = await workflow.execute_activity(
                act.stripe_configured,
                start_to_close_timeout=timedelta(seconds=30),
                retry_policy=BOOKKEEPING_RETRY,
            )
            if not configured:
                raise Skipped("no Stripe API key configured")
            results = await asyncio.gather(
                *[
                    _extract_activity(
                        act.extract_stripe_object,
                        ExtractTableInput(run_id, o, params.full_refresh),
                    )
                    for o in objects
                ]
            )
            return {"rows": sum(r["rows"] for r in results), "tables": results}

        return await tracked(asdict(params), body)


@workflow.defn
class TransformWorkflow:
    @workflow.run
    async def run(self, params: TransformParams) -> dict[str, Any]:
        async def body(run_id: str) -> dict[str, Any]:
            built = await workflow.execute_activity(
                act.run_transform,
                StepInput(run_id),
                start_to_close_timeout=timedelta(minutes=30),
                retry_policy=TRANSFORM_RETRY,
            )
            stats: dict[str, Any] = {"transform": built}
            if params.run_checks:
                stats["quality"] = await workflow.execute_activity(
                    act.run_quality_checks,
                    StepInput(run_id),
                    start_to_close_timeout=timedelta(minutes=10),
                    retry_policy=TRANSFORM_RETRY,
                )
            return stats

        return await tracked(asdict(params), body)


@workflow.defn
class DataQualityWorkflow:
    @workflow.run
    async def run(self) -> dict[str, Any]:
        async def body(run_id: str) -> dict[str, Any]:
            summary = await workflow.execute_activity(
                act.run_quality_checks,
                StepInput(run_id),
                start_to_close_timeout=timedelta(minutes=10),
                retry_policy=TRANSFORM_RETRY,
            )
            return {"quality": summary}

        return await tracked({}, body)


@workflow.defn
class PipelineWorkflow:
    @workflow.run
    async def run(self, params: PipelineParams) -> dict[str, Any]:
        async def body(run_id: str) -> dict[str, Any]:
            wf_id = workflow.info().workflow_id
            extract = ExtractParams(full_refresh=params.full_refresh)
            children: list[Awaitable[dict[str, Any]]] = []
            if "navi" in params.sources:
                children.append(
                    workflow.execute_child_workflow(
                        ExtractNaviWorkflow.run, extract, id=f"{wf_id}-navi"
                    )
                )
            if "stripe" in params.sources:
                children.append(
                    workflow.execute_child_workflow(
                        ExtractStripeWorkflow.run, extract, id=f"{wf_id}-stripe"
                    )
                )
            # Wait for every extract to finish before failing: returning early
            # would make Temporal terminate the still-running sibling, which
            # then never gets to record its own result in meta.runs.
            outcomes = await asyncio.gather(*children, return_exceptions=True)
            for outcome in outcomes:
                if isinstance(outcome, BaseException):
                    raise outcome
            extracts = list(outcomes)
            transform = await workflow.execute_child_workflow(
                TransformWorkflow.run,
                TransformParams(run_checks=params.run_checks),
                id=f"{wf_id}-transform",
            )
            return {"extracts": extracts, "transform": transform}

        return await tracked(asdict(params), body)


ALL = [
    ExtractNaviWorkflow,
    ExtractStripeWorkflow,
    TransformWorkflow,
    DataQualityWorkflow,
    PipelineWorkflow,
]
