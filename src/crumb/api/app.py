"""crumb API: pipeline runs, freshness and read-only marts for Navi MCP / bean."""

import datetime as dt
import logging
import secrets
from collections.abc import AsyncIterator
from collections.abc import Iterator
from contextlib import asynccontextmanager
from typing import Annotated
from typing import Any

import psycopg
from fastapi import Depends
from fastapi import FastAPI
from fastapi import HTTPException
from fastapi import Path
from fastapi import Query
from fastapi import Request
from fastapi import Security
from fastapi.security import APIKeyHeader
from psycopg import sql
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool
from temporalio.client import Client

from crumb import warehouse as wh
from crumb.api import marts as mart_queries
from crumb.api.models import Freshness
from crumb.api.models import Health
from crumb.api.models import MartColumn
from crumb.api.models import MartInfo
from crumb.api.models import MartPage
from crumb.api.models import PipelineInfo
from crumb.api.models import PipelineStarted
from crumb.api.models import Run
from crumb.api.models import RunList
from crumb.config import Settings
from crumb.config import get_settings
from crumb.sources import navi
from crumb.temporal.activities import all_raw_tables
from crumb.temporal.pipelines import PIPELINES
from crumb.temporal.pipelines import connect_client
from crumb.temporal.pipelines import start_pipeline

log = logging.getLogger(__name__)

API_KEY_HEADER = "X-API-Key"
_api_key = APIKeyHeader(name=API_KEY_HEADER, auto_error=False)


def _db(request: Request) -> Iterator[psycopg.Connection[dict[str, Any]]]:
    pool: ConnectionPool[psycopg.Connection[dict[str, Any]]] = request.app.state.pool
    try:
        with pool.connection(timeout=5) as conn:
            yield conn
    except psycopg.OperationalError as err:
        raise HTTPException(503, f"warehouse unavailable: {err}") from err


Db = Annotated[psycopg.Connection[dict[str, Any]], Depends(_db)]


class Temporal:
    """Lazily (re)connects so the API stays up while Temporal is down."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._client: Client | None = None

    async def client(self) -> Client:
        if self._client is None:
            try:
                self._client = await connect_client(self._settings)
            except Exception as err:
                raise HTTPException(503, f"Temporal unavailable: {err}") from err
        return self._client


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or get_settings()

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        pool = ConnectionPool(
            settings.warehouse_dsn.get_secret_value(),
            connection_class=psycopg.Connection[dict[str, Any]],
            min_size=1,
            max_size=10,
            kwargs={"row_factory": dict_row, "autocommit": True},
            open=False,
        )
        pool.open(wait=False)
        app.state.pool = pool
        app.state.temporal = Temporal(settings)
        try:
            with pool.connection(timeout=10) as conn:
                wh.bootstrap(conn, all_raw_tables())
        except Exception:
            log.exception("warehouse bootstrap failed; continuing so /health can report it")
        yield
        pool.close()

    app = FastAPI(
        title="crumb API",
        version="0.1.0",
        description=(
            "Pipeline runs, data freshness and read-only analytics marts for the Navi "
            f"platform. Send the API key in the `{API_KEY_HEADER}` header."
        ),
        lifespan=lifespan,
    )

    def require_key(key: Annotated[str | None, Security(_api_key)]) -> None:
        expected = settings.api_key
        if expected is None:
            raise HTTPException(503, "CRUMB_API_KEY is not configured")
        if key is None or not secrets.compare_digest(key, expected.get_secret_value()):
            raise HTTPException(401, "invalid or missing API key")

    authed = [Depends(require_key)]

    @app.get("/health", response_model=Health, tags=["ops"])
    async def health(request: Request) -> Health:
        warehouse_ok = temporal_ok = False
        try:
            with request.app.state.pool.connection(timeout=2) as conn:
                conn.execute("select 1")
            warehouse_ok = True
        except Exception:
            log.warning("health: warehouse unreachable")
        try:
            client = await request.app.state.temporal.client()
            await client.service_client.check_health()
            temporal_ok = True
        except Exception:
            log.warning("health: temporal unreachable")
        status = "ok" if warehouse_ok and temporal_ok else "degraded"
        return Health(status=status, warehouse=warehouse_ok, temporal=temporal_ok)

    @app.get("/freshness", response_model=list[Freshness], dependencies=authed, tags=["ops"])
    def freshness(conn: Db) -> list[Freshness]:
        rows = conn.execute(
            """
            select source, table_name, status, watermark, last_success_at, last_run_id,
                   rows_last_run,
                   extract(epoch from now() - last_success_at)::bigint as age_seconds
              from meta.watermarks
             order by source, table_name
            """
        ).fetchall()
        max_age = settings.freshness_max_age_hours * 3600
        return [
            Freshness(
                **r,
                table=r["table_name"],
                required=r["source"] == navi.SOURCE
                and r["table_name"] in navi.TABLES
                and navi.TABLES[r["table_name"]].required,
                # A skipped optional table isn't stale; there's nothing to extract.
                is_stale=r["status"] == "ok"
                and (r["age_seconds"] is None or r["age_seconds"] > max_age),
            )
            for r in rows
        ]

    @app.get("/runs", response_model=RunList, dependencies=authed, tags=["runs"])
    def list_runs(
        conn: Db,
        status: Annotated[str | None, Query(pattern="^(running|succeeded|failed|skipped)$")] = None,
        workflow_type: str | None = None,
        top_level_only: bool = False,
        limit: Annotated[int, Query(ge=1, le=500)] = 50,
        offset: Annotated[int, Query(ge=0)] = 0,
    ) -> RunList:
        where = [
            sql.SQL("(%(status)s::text is null or status = %(status)s)"),
            sql.SQL("(%(wf)s::text is null or workflow_type = %(wf)s)"),
        ]
        if top_level_only:
            where.append(sql.SQL("parent_run_id is null"))
        clause = sql.SQL(" and ").join(where)
        params = {"status": status, "wf": workflow_type, "limit": limit, "offset": offset}
        total = conn.execute(
            sql.SQL("select count(*) as n from meta.runs where {}").format(clause), params
        ).fetchone()
        rows = conn.execute(
            sql.SQL(
                "select * from meta.runs where {} order by started_at desc "
                "limit %(limit)s offset %(offset)s"
            ).format(clause),
            params,
        ).fetchall()
        return RunList(
            total=total["n"] if total else 0,
            limit=limit,
            offset=offset,
            runs=[
                Run.from_row(r, settings.temporal_ui_url, settings.temporal_namespace) for r in rows
            ],
        )

    @app.get("/runs/{run_id}", response_model=Run, dependencies=authed, tags=["runs"])
    async def get_run(request: Request, run_id: str) -> Run:
        with request.app.state.pool.connection(timeout=5) as conn:
            row = conn.execute("select * from meta.runs where run_id = %s", (run_id,)).fetchone()
            children = conn.execute(
                "select * from meta.runs where parent_run_id = %s order by started_at", (run_id,)
            ).fetchall()
        if row is None:
            raise HTTPException(404, "run not found")
        run = Run.from_row(row, settings.temporal_ui_url, settings.temporal_namespace)
        run.children = [
            Run.from_row(c, settings.temporal_ui_url, settings.temporal_namespace) for c in children
        ]
        try:
            client = await request.app.state.temporal.client()
            desc = await client.get_workflow_handle(row["workflow_id"], run_id=run_id).describe()
            run.temporal_status = desc.status.name if desc.status else None
        except Exception as err:  # history may be gone (retention) or Temporal down
            log.info("temporal describe failed for %s: %s", run_id, err)
        return run

    @app.get("/pipelines", response_model=list[PipelineInfo], dependencies=authed, tags=["runs"])
    def list_pipelines() -> list[PipelineInfo]:
        return [
            PipelineInfo(name=p.name, workflow=p.workflow, description=p.description)
            for p in PIPELINES.values()
        ]

    @app.post(
        "/pipelines/{name}/run",
        response_model=PipelineStarted,
        status_code=202,
        dependencies=authed,
        tags=["runs"],
    )
    async def run_pipeline(request: Request, name: str) -> PipelineStarted:
        if name not in PIPELINES:
            raise HTTPException(404, f"unknown pipeline {name!r}; see GET /pipelines")
        client = await request.app.state.temporal.client()
        handle = await start_pipeline(client, settings, name)
        return PipelineStarted(
            pipeline=name,
            workflow_id=handle.id,
            run_id=handle.result_run_id or "",
            temporal_ui_url=f"{settings.temporal_ui_url}/namespaces/{settings.temporal_namespace}/workflows/{handle.id}",
        )

    @app.get("/marts", response_model=list[MartInfo], dependencies=authed, tags=["marts"])
    def list_marts(conn: Db) -> list[MartInfo]:
        columns = mart_queries.mart_columns(conn)
        return [
            MartInfo(
                name=name,
                description=spec.description,
                filters=[
                    f
                    for f, c in (
                        ("start_date/end_date", spec.date_column),
                        ("port_id", spec.port_column),
                    )
                    if c
                ],
                built=name in columns,
                columns=[MartColumn(**c) for c in columns.get(name, [])],
            )
            for name, spec in mart_queries.MARTS.items()
        ]

    @app.get("/marts/{name}", response_model=MartPage, dependencies=authed, tags=["marts"])
    def get_mart(
        conn: Db,
        name: Annotated[str, Path(description="One of GET /marts")],
        start_date: dt.date | None = None,
        end_date: dt.date | None = None,
        port_id: str | None = None,
        limit: Annotated[int, Query(ge=1, le=1000)] = 100,
        offset: Annotated[int, Query(ge=0)] = 0,
    ) -> MartPage:
        if name not in mart_queries.MARTS:
            raise HTTPException(404, f"unknown mart {name!r}; see GET /marts")
        try:
            total, rows = mart_queries.query_mart(
                conn,
                name,
                start_date=start_date,
                end_date=end_date,
                port_id=port_id,
                limit=limit,
                offset=offset,
            )
        except psycopg.errors.UndefinedTable as err:
            raise HTTPException(
                503, f"mart {name!r} has not been built yet; run a pipeline"
            ) from err
        return MartPage(mart=name, total=total, limit=limit, offset=offset, rows=rows)

    return app


def main() -> None:
    import uvicorn

    uvicorn.run("crumb.api.app:create_app", factory=True, host="0.0.0.0", port=8020)  # noqa: S104
