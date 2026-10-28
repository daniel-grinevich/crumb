"""Named pipelines (what `just run-pipeline <name>` and the API start) and the
Temporal Schedules that run them."""

import datetime as dt
import logging
import uuid
from dataclasses import dataclass
from typing import Any

from temporalio.client import Client
from temporalio.client import Schedule
from temporalio.client import ScheduleActionStartWorkflow
from temporalio.client import ScheduleAlreadyRunningError
from temporalio.client import ScheduleIntervalSpec
from temporalio.client import ScheduleOverlapPolicy
from temporalio.client import SchedulePolicy
from temporalio.client import ScheduleSpec
from temporalio.client import ScheduleUpdate
from temporalio.client import ScheduleUpdateInput
from temporalio.client import WorkflowHandle

from crumb.config import Settings
from crumb.temporal import workflows as wf
from crumb.temporal.types import ExtractParams
from crumb.temporal.types import PipelineParams
from crumb.temporal.types import TransformParams

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class Pipeline:
    name: str
    workflow: str
    arg: Any
    description: str


PIPELINES: dict[str, Pipeline] = {
    p.name: p
    for p in [
        Pipeline(
            "full",
            "PipelineWorkflow",
            PipelineParams(run_checks=True),
            "Incremental extract of Navi + Stripe, transform, data-quality checks.",
        ),
        Pipeline(
            "incremental",
            "PipelineWorkflow",
            PipelineParams(run_checks=False),
            "Incremental extract of Navi + Stripe, then transform (what the hourly schedule runs).",
        ),
        Pipeline(
            "full-refresh",
            "PipelineWorkflow",
            PipelineParams(full_refresh=True, run_checks=True),
            "Ignore watermarks: re-extract everything, transform, check.",
        ),
        Pipeline("navi", "ExtractNaviWorkflow", ExtractParams(), "Incremental Navi extract only."),
        Pipeline(
            "stripe", "ExtractStripeWorkflow", ExtractParams(), "Incremental Stripe extract only."
        ),
        Pipeline(
            "transform",
            "TransformWorkflow",
            TransformParams(run_checks=True),
            "Rebuild staging + marts from raw, then data-quality checks (nightly).",
        ),
        Pipeline("quality", "DataQualityWorkflow", None, "Data-quality checks only."),
    ]
}

SCHEDULES = {
    "crumb-hourly-pipeline": (
        "incremental",
        ScheduleSpec(intervals=[ScheduleIntervalSpec(every=dt.timedelta(hours=1))]),
    ),
    # 03:30 UTC nightly.
    "crumb-nightly-transform": ("transform", ScheduleSpec(cron_expressions=["30 3 * * *"])),
}


async def connect_client(settings: Settings) -> Client:
    return await Client.connect(settings.temporal_address, namespace=settings.temporal_namespace)


async def start_pipeline(client: Client, settings: Settings, name: str) -> WorkflowHandle[Any, Any]:
    pipeline = PIPELINES[name]
    stamp = dt.datetime.now(dt.UTC).strftime("%Y%m%dT%H%M%S")
    workflow_id = f"crumb-{name}-{stamp}-{uuid.uuid4().hex[:6]}"
    args = [] if pipeline.arg is None else [pipeline.arg]
    return await client.start_workflow(
        pipeline.workflow, args=args, id=workflow_id, task_queue=settings.task_queue
    )


def _schedule(settings: Settings, pipeline_name: str, spec: ScheduleSpec) -> Schedule:
    pipeline = PIPELINES[pipeline_name]
    return Schedule(
        action=ScheduleActionStartWorkflow(
            pipeline.workflow,
            args=[] if pipeline.arg is None else [pipeline.arg],
            id=f"crumb-{pipeline_name}-scheduled",
            task_queue=settings.task_queue,
        ),
        spec=spec,
        # A slow run must never stack up behind itself.
        policy=SchedulePolicy(overlap=ScheduleOverlapPolicy.SKIP),
    )


async def ensure_schedules(client: Client, settings: Settings) -> None:
    """Create the schedules, or update them in place if they already exist."""
    for schedule_id, (pipeline_name, spec) in SCHEDULES.items():
        schedule = _schedule(settings, pipeline_name, spec)
        try:
            await client.create_schedule(schedule_id, schedule)
            log.info("created schedule %s", schedule_id)
        except ScheduleAlreadyRunningError:

            def update(_: ScheduleUpdateInput, s: Schedule = schedule) -> ScheduleUpdate:
                return ScheduleUpdate(schedule=s)

            await client.get_schedule_handle(schedule_id).update(update)
            log.info("updated schedule %s", schedule_id)


WORKFLOW_NAMES = {w.__name__ for w in wf.ALL}
