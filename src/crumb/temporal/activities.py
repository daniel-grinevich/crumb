"""Temporal activities. Synchronous (psycopg, Stripe SDK); the worker runs them
in a thread pool. Each opens its own short-lived connections."""

import datetime as dt
import logging
from dataclasses import asdict
from typing import Any

from temporalio import activity
from temporalio.exceptions import ApplicationError

from crumb import quality
from crumb import warehouse as wh
from crumb.config import get_settings
from crumb.db import connect
from crumb.sources import navi
from crumb.sources import stripe_source
from crumb.temporal.types import ExtractTableInput
from crumb.temporal.types import RunFinished
from crumb.temporal.types import RunStarted
from crumb.temporal.types import StepInput
from crumb.transform import SqlFileTransformer
from crumb.transform import affected_models

log = logging.getLogger(__name__)


def all_raw_tables() -> list[str]:
    return [t.raw_table for t in navi.TABLES.values()] + [
        o.raw_table for o in stripe_source.OBJECTS.values()
    ]


def _resume_details() -> dict[str, Any] | None:
    details = activity.info().heartbeat_details
    return details[0] if details else None


@activity.defn
def record_run_started(run: RunStarted) -> None:
    with connect(get_settings().warehouse_dsn.get_secret_value(), autocommit=True) as conn:
        wh.record_run_started(
            conn,
            run_id=run.run_id,
            workflow_id=run.workflow_id,
            workflow_type=run.workflow_type,
            parent_run_id=run.parent_run_id,
            params=run.params,
        )


@activity.defn
def record_run_finished(run: RunFinished) -> None:
    with connect(get_settings().warehouse_dsn.get_secret_value(), autocommit=True) as conn:
        wh.record_run_finished(
            conn, run_id=run.run_id, status=run.status, stats=run.stats, error=run.error
        )


@activity.defn
def extract_navi_table(inp: ExtractTableInput) -> dict[str, Any]:
    settings = get_settings()
    spec = navi.TABLES[inp.table]
    resume = _resume_details()
    cursor = navi.Cursor.from_json(resume["cursor"]) if resume else None
    with (
        connect(settings.navi_dsn.get_secret_value(), read_only=True) as source,
        connect(settings.warehouse_dsn.get_secret_value(), autocommit=True) as warehouse,
    ):
        result = navi.extract_table(
            spec,
            navi=source,
            warehouse=warehouse,
            salt=settings.require_pii_salt(),
            run_id=inp.run_id,
            page_size=settings.extract_page_size,
            lookback=dt.timedelta(seconds=settings.navi_lookback_seconds),
            full_refresh=inp.full_refresh,
            resume_from=cursor,
            heartbeat=activity.heartbeat,
        )
    log.info("extracted navi.%s: %s rows in %s pages", spec.table, result.rows, result.pages)
    return asdict(result)


@activity.defn
def stripe_configured() -> bool:
    if get_settings().stripe_api_key is None:
        log.warning("Stripe extract skipped: no CRUMB_STRIPE_API_KEY / STRIPE_API_KEY configured")
        return False
    return True


@activity.defn
def extract_stripe_object(inp: ExtractTableInput) -> dict[str, Any]:
    settings = get_settings()
    key = settings.stripe_api_key
    if key is None:
        raise ApplicationError(
            "Stripe API key not configured", type="ConfigError", non_retryable=True
        )
    obj = stripe_source.OBJECTS[inp.table]
    resume = _resume_details()
    with connect(settings.warehouse_dsn.get_secret_value(), autocommit=True) as warehouse:
        result = stripe_source.extract_object(
            obj,
            lister=stripe_source.StripeSdkLister(key.get_secret_value()),
            warehouse=warehouse,
            salt=settings.require_pii_salt(),
            run_id=inp.run_id,
            page_size=settings.extract_page_size,
            lookback=dt.timedelta(seconds=settings.stripe_lookback_seconds),
            full_refresh=inp.full_refresh,
            resume_after=resume["starting_after"] if resume else None,
            heartbeat=activity.heartbeat,
        )
    log.info("extracted stripe.%s: %s rows", obj.name, result.rows)
    return asdict(result)


@activity.defn
def run_transform(inp: StepInput) -> dict[str, Any]:
    settings = get_settings()
    transformer = SqlFileTransformer()
    with connect(settings.warehouse_dsn.get_secret_value(), autocommit=True) as conn:
        wh.bootstrap(conn, all_raw_tables())
        result = transformer.run(conn, timezone=settings.reporting_tz)
        skipped = wh.skipped_tables(conn)
    log.info("transform built %s models", len(result.models))
    stats: dict[str, Any] = {"models": len(result.models), "rows": result.rows}
    missing_raw = {
        navi.TABLES[table].raw_table
        for (source, table), _ in skipped.items()
        if source == navi.SOURCE and table in navi.TABLES
    }
    degraded = affected_models(transformer.models(), missing_raw)
    if degraded:
        # Raw tables always exist (bootstrap), so these still build: empty if the
        # source table never existed, or from previously extracted rows.
        stats["degraded"] = {
            fqn: f"source table(s) missing in Navi: {', '.join(raws)}; built without new data"
            for fqn, raws in degraded.items()
        }
        log.warning("models built without missing source tables: %s", stats["degraded"])
    return stats


@activity.defn
def run_quality_checks(inp: StepInput) -> dict[str, Any]:
    settings = get_settings()
    with connect(settings.warehouse_dsn.get_secret_value(), autocommit=True) as conn:
        results = quality.run_checks(
            conn,
            SqlFileTransformer().models(),
            max_age=dt.timedelta(hours=settings.freshness_max_age_hours),
            stripe_enabled=settings.stripe_api_key is not None,
            run_id=inp.run_id,
        )
    summary = quality.summarise(results)
    if summary["failed"]:
        lines = "; ".join(f"{f['name']}: {f['message']}" for f in summary["failures"])
        raise ApplicationError(
            f"{summary['failed']} data-quality check(s) failed: {lines}",
            summary,
            type="DataQualityError",
            non_retryable=True,
        )
    return summary


ALL = [
    record_run_started,
    record_run_finished,
    extract_navi_table,
    stripe_configured,
    extract_stripe_object,
    run_transform,
    run_quality_checks,
]
