"""Close out meta.runs rows that Temporal has already closed.

Workflows record their own result, but a workflow that is terminated, times
out, is reset, or loses its final bookkeeping write never gets to. This sweep
asks Temporal about every run still marked 'running' and copies the real
outcome over. The API runs it on read; the worker runs it periodically.
"""

import asyncio
import datetime as dt
import logging
from typing import Any

import psycopg
from temporalio.client import Client
from temporalio.client import WorkflowExecutionStatus
from temporalio.service import RPCError
from temporalio.service import RPCStatusCode

log = logging.getLogger(__name__)

CLOSED_STATUS = {
    WorkflowExecutionStatus.COMPLETED: "succeeded",
    WorkflowExecutionStatus.CONTINUED_AS_NEW: "succeeded",
    WorkflowExecutionStatus.FAILED: "failed",
    WorkflowExecutionStatus.CANCELED: "cancelled",
    WorkflowExecutionStatus.TERMINATED: "terminated",
    WorkflowExecutionStatus.TIMED_OUT: "timed_out",
}


async def _temporal_outcome(
    client: Client, workflow_id: str, run_id: str
) -> tuple[str, dt.datetime | None] | None:
    try:
        desc = await client.get_workflow_handle(workflow_id, run_id=run_id).describe()
    except RPCError as err:
        if err.status == RPCStatusCode.NOT_FOUND:
            # Past retention or a different namespace: don't guess.
            log.info("run %s not found in Temporal; leaving as is", run_id)
            return None
        raise
    if desc.status is None or desc.status not in CLOSED_STATUS:
        return None
    return CLOSED_STATUS[desc.status], desc.close_time


async def sync_open_runs(
    client: Client,
    conn: psycopg.Connection[dict[str, Any]],
    *,
    run_ids: list[str] | None = None,
    limit: int = 200,
) -> int:
    """Update 'running' rows whose workflow is closed in Temporal. Returns rows fixed."""
    if run_ids is not None:
        rows = conn.execute(
            "select run_id, workflow_id from meta.runs where status = 'running' "
            "and run_id = any(%s)",
            (run_ids,),
        ).fetchall()
    else:
        rows = conn.execute(
            "select run_id, workflow_id from meta.runs where status = 'running' "
            "order by started_at limit %s",
            (limit,),
        ).fetchall()
    if not rows:
        return 0
    outcomes = await asyncio.gather(
        *[_temporal_outcome(client, r["workflow_id"], r["run_id"]) for r in rows],
        return_exceptions=True,
    )
    fixed = 0
    for row, outcome in zip(rows, outcomes, strict=True):
        if isinstance(outcome, BaseException):
            log.warning("could not describe run %s: %s", row["run_id"], outcome)
            continue
        if outcome is None:
            continue
        status, closed_at = outcome
        cur = conn.execute(
            """
            update meta.runs
               set status = %s,
                   finished_at = coalesce(%s, now()),
                   error = coalesce(error, %s)
             where run_id = %s and status = 'running'
            """,
            (
                status,
                closed_at,
                f"closed in Temporal as {status} without recording a result",
                row["run_id"],
            ),
        )
        fixed += cur.rowcount
    if fixed:
        log.info("synced %s run(s) from Temporal", fixed)
    return fixed
