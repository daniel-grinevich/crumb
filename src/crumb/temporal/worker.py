"""The crumb Temporal worker: bootstraps the warehouse, ensures schedules, then
polls the task queue."""

import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor

from temporalio.worker import Worker

from crumb import warehouse as wh
from crumb.config import get_settings
from crumb.db import connect
from crumb.temporal import activities
from crumb.temporal import workflows
from crumb.temporal.pipelines import connect_client
from crumb.temporal.pipelines import ensure_schedules

log = logging.getLogger(__name__)

MAX_ACTIVITIES = 8


async def run_worker() -> None:
    settings = get_settings()
    settings.require_pii_salt()  # fail at startup, not mid-extract
    with connect(settings.warehouse_dsn.get_secret_value(), autocommit=True) as conn:
        wh.bootstrap(conn, activities.all_raw_tables())

    client = await connect_client(settings)
    if settings.create_schedules:
        await ensure_schedules(client, settings)

    with ThreadPoolExecutor(max_workers=MAX_ACTIVITIES) as executor:
        worker = Worker(
            client,
            task_queue=settings.task_queue,
            workflows=workflows.ALL,
            activities=activities.ALL,
            activity_executor=executor,
            max_concurrent_activities=MAX_ACTIVITIES,
        )
        log.info("worker polling task queue %r", settings.task_queue)
        await worker.run()


def main() -> None:
    asyncio.run(run_worker())
