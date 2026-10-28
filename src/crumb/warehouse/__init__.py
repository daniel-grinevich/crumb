"""Warehouse access: bootstrap, raw landing, watermarks and the run log."""

import datetime as dt
from importlib import resources
from typing import Any

import psycopg
from psycopg import sql

from crumb.db import jsonb
from crumb.db import trusted_sql

# Serialises concurrent bootstraps (worker and API start together).
_BOOTSTRAP_LOCK = 0x6372756D62  # "crumb"

type Conn = psycopg.Connection[dict[str, Any]]


def bootstrap(conn: Conn, raw_tables: list[str]) -> None:
    ddl = resources.files("crumb.warehouse").joinpath("schema.sql").read_text()
    with conn.transaction():
        conn.execute("select pg_advisory_xact_lock(%s)", (_BOOTSTRAP_LOCK,))
        conn.execute(trusted_sql(ddl))
        for name in raw_tables:
            ensure_raw_table(conn, name)


def ensure_raw_table(conn: Conn, name: str) -> None:
    """Raw tables land source rows as JSONB keyed by the source primary key.

    JSONB keeps landing generic and tolerant of source schema drift; staging
    owns typing. ``source_ts`` is the row's watermark column value.
    """
    conn.execute(
        sql.SQL(
            """
            create table if not exists raw.{} (
                id            text primary key,
                data          jsonb not null,
                source_ts     timestamptz,
                _extracted_at timestamptz not null default now(),
                _run_id       text
            )
            """
        ).format(sql.Identifier(name))
    )


def upsert_raw(conn: Conn, name: str, records: list[dict[str, Any]], run_id: str) -> int:
    """Idempotent upsert; unchanged rows are left untouched. Returns rows written."""
    if not records:
        return 0
    query = sql.SQL(
        """
        insert into raw.{table} as t (id, data, source_ts, _run_id)
        values (%(id)s, %(data)s, %(source_ts)s, %(run_id)s)
        on conflict (id) do update
            set data = excluded.data,
                source_ts = excluded.source_ts,
                _extracted_at = now(),
                _run_id = excluded._run_id
            where t.data is distinct from excluded.data
        """
    ).format(table=sql.Identifier(name))
    params = [
        {
            "id": str(r["id"]),
            "data": jsonb(r["data"]),
            "source_ts": r.get("source_ts"),
            "run_id": run_id,
        }
        for r in records
    ]
    with conn.cursor() as cur:
        cur.executemany(query, params)
        return cur.rowcount if cur.rowcount >= 0 else len(records)


def get_watermark(conn: Conn, source: str, table: str) -> dt.datetime | None:
    row = conn.execute(
        "select watermark from meta.watermarks where source = %s and table_name = %s",
        (source, table),
    ).fetchone()
    return row["watermark"] if row else None


def set_watermark(
    conn: Conn,
    source: str,
    table: str,
    watermark: dt.datetime | None,
    rows: int,
    run_id: str,
) -> None:
    """Record a successful extract. The watermark only ever moves forward."""
    conn.execute(
        """
        insert into meta.watermarks as w
            (source, table_name, watermark, last_success_at, last_run_id, rows_last_run)
        values (%s, %s, %s, now(), %s, %s)
        on conflict (source, table_name) do update
            set watermark = greatest(w.watermark, excluded.watermark),
                status = 'ok',
                last_success_at = now(),
                last_run_id = excluded.last_run_id,
                rows_last_run = excluded.rows_last_run,
                updated_at = now()
        """,
        (source, table, watermark, run_id, rows),
    )


def mark_skipped(conn: Conn, source: str, table: str, status: str, run_id: str) -> None:
    """Record that a table was deliberately not extracted (and why).

    The watermark and last_success_at are left as they were, so a table that
    disappears keeps its history and one that reappears resumes incrementally.
    """
    conn.execute(
        """
        insert into meta.watermarks as w (source, table_name, status, last_run_id, rows_last_run)
        values (%s, %s, %s, %s, 0)
        on conflict (source, table_name) do update
            set status = excluded.status,
                last_run_id = excluded.last_run_id,
                rows_last_run = 0,
                updated_at = now()
        """,
        (source, table, status, run_id),
    )


def skipped_tables(conn: Conn) -> dict[tuple[str, str], str]:
    """(source, table) -> status for every table currently not being extracted."""
    rows = conn.execute(
        "select source, table_name, status from meta.watermarks where status <> 'ok'"
    ).fetchall()
    return {(r["source"], r["table_name"]): r["status"] for r in rows}


def record_run_started(
    conn: Conn,
    *,
    run_id: str,
    workflow_id: str,
    workflow_type: str,
    parent_run_id: str | None,
    params: dict[str, Any],
) -> None:
    conn.execute(
        """
        insert into meta.runs (run_id, workflow_id, workflow_type, parent_run_id, status, params)
        values (%s, %s, %s, %s, 'running', %s)
        on conflict (run_id) do update set status = 'running', finished_at = null, error = null
        """,
        (run_id, workflow_id, workflow_type, parent_run_id, jsonb(params)),
    )


def record_run_finished(
    conn: Conn,
    *,
    run_id: str,
    status: str,
    stats: dict[str, Any],
    error: str | None,
) -> None:
    conn.execute(
        """
        update meta.runs
           set status = %s, stats = %s, error = %s, finished_at = now()
         where run_id = %s
        """,
        (status, jsonb(stats), error, run_id),
    )
