"""Navi Postgres source: what we read, how we page it, and what we never land.

Every table has an explicit column allowlist. Anything not listed (emails,
names, phones, passwords, tokens, IPs, user agents, free text) is never even
selected. User identifiers listed in ``pseudonymise`` are replaced with a salted
HMAC before landing (see ``crumb.pii``).

The same specs work across Navi versions: columns missing from the source (an
older Navi schema) are skipped, and so are whole tables unless they are
``required``. A missing required table fails the extract, because the core
marts (orders, sales, refunds) would silently be wrong without it.
"""

import datetime as dt
import logging
from collections.abc import Callable
from dataclasses import dataclass
from dataclasses import field
from typing import Any

import psycopg
from psycopg import sql

from crumb.db import trusted_sql
from crumb.pii import pseudonymise

SOURCE = "navi"

log = logging.getLogger(__name__)

_AUDIT = ("status", "is_deleted", "deleted_at", "created_at", "updated_at")
_AUDIT_USERS = {"created_by_id": "user", "updated_by_id": "user"}


@dataclass(frozen=True)
class NaviTable:
    table: str
    columns: tuple[str, ...]
    # Column (or SQL expression) we page and watermark on. None = small table
    # without timestamps: re-read in full every run.
    watermark: str | None = "updated_at"
    id_type: str = "uuid"
    pseudonymise: dict[str, str] = field(default_factory=dict)
    # Required tables fail the extract when absent; optional ones are skipped.
    required: bool = False

    @property
    def raw_table(self) -> str:
        return f"navi_{self.table}"


TABLES: dict[str, NaviTable] = {
    t.table: t
    for t in [
        NaviTable(
            "users_user",
            # email, name, phone, first/last name and password are never read.
            ("id", "is_guest", "is_staff", "is_active", "email_confirmed",
             "date_joined", "last_login", "stripe_customer_id"),
            watermark="coalesce(last_login, date_joined)",
            id_type="bigint",
            pseudonymise={"id": "user", "stripe_customer_id": "stripe_customer"},
        ),
        NaviTable(
            "orders_order",
            # cart_token, qr_nonce (secrets) and cancel_reason (free text) dropped.
            ("id", "user_id", "navi_port_id", "payment_id", "order_status",
             "claimed_by_id", "claimed_at", "is_test", *_AUDIT,
             "created_by_id", "updated_by_id"),
            pseudonymise={"user_id": "user", **_AUDIT_USERS},
            required=True,
        ),
        NaviTable(
            "orders_orderitem",
            ("id", "order_id", "menu_item_id", "quantity", "unit_price", *_AUDIT),
            required=True,
        ),
        NaviTable(
            "orders_ordercustomization",
            ("id", "order_item_id", "customization_id", "quantity", "unit_price",
             *_AUDIT),
        ),
        NaviTable(
            "orders_machineerrorlog",
            # error_message is machine-generated (Pi firmware), not user text.
            ("id", "order_id", "raspberry_pi_id", "error_message", "is_recoverable",
             *_AUDIT),
        ),
        NaviTable(
            "payments_payment",
            # stripe_payment_method_id / setup intent ids aren't needed downstream.
            ("id", "stripe_payment_intent_id", "amount_received", "subtotal",
             "tax_amount", "total_amount", "currency", "status", "amount_refunded",
             "stripe_refund_id", "is_deleted", "created_at", "updated_at"),
            required=True,
        ),
        NaviTable(
            "payments_invoice",
            ("id", "order_id", "reference_number", *_AUDIT),
        ),
        NaviTable(
            "devices_naviport",
            # Street address lines and the customer-facing status_message dropped.
            ("id", "name", "slug", "city", "state_or_region", "postal_code",
             "country", "latitude", "longitude", "espresso_machine_id",
             "raspberry_pi_id", "accepting_orders", *_AUDIT),
            required=True,
        ),
        NaviTable(
            "devices_raspberrypi",
            # mac/ip addresses and device tokens dropped.
            ("id", "name", "slug", "location", "is_connected", "firmware_version",
             "last_seen", "is_simulated", *_AUDIT),
        ),
        NaviTable(
            "devices_espressomachine",
            ("id", "name", "serial_number", "machine_type_id", "is_online",
             "last_maintenance_at", *_AUDIT),
        ),
        NaviTable(
            "devices_machinetype",
            ("id", "name", "model_number", "maintenance_frequency", *_AUDIT),
        ),
        NaviTable(
            "devices_deviceevent",
            # message/payload are free-form (admin notes, request bodies): dropped.
            ("id", "raspberry_pi_id", "type", "actor_id", "created_at"),
            watermark="created_at",
            pseudonymise={"actor_id": "user"},
        ),
        NaviTable(
            "menu_category",
            ("id", "name", "slug", *_AUDIT),
        ),
        NaviTable(
            "menu_menuitem",
            ("id", "name", "slug", "price", "category_id", "is_featured", "version",
             *_AUDIT),
        ),
        NaviTable(
            "menu_customization",
            ("id", "name", "group_id", "price", *_AUDIT),
        ),
        NaviTable(
            "awards_award",
            ("id", "name", "rule_type", "threshold", "points_reward", *_AUDIT),
        ),
        NaviTable(
            "awards_awardlevel",
            ("id", "award_id", "rank", "name", "threshold", "points_reward"),
            watermark=None,
        ),
        NaviTable(
            "awards_userloyalty",
            ("id", "user_id", "lifetime_points", "balance_points",
             "orders_completed", "total_spent", "current_tier_id", "created_at",
             "updated_at"),
            pseudonymise={"user_id": "user"},
        ),
        NaviTable(
            "awards_useraward",
            ("id", "user_id", "award_id", "level_id", "earned_at"),
            watermark="earned_at",
            pseudonymise={"user_id": "user"},
        ),
        NaviTable(
            "awards_userawardlevel",
            ("id", "user_award_id", "level_id", "earned_at"),
            watermark="earned_at",
        ),
    ]
}


@dataclass(frozen=True)
class Cursor:
    """Keyset position inside one extract: (watermark value, id) of the last row."""

    ts: str | None
    id: str

    def to_json(self) -> dict[str, Any]:
        return {"ts": self.ts, "id": self.id}

    @classmethod
    def from_json(cls, data: dict[str, Any]) -> "Cursor":
        return cls(ts=data.get("ts"), id=data["id"])


class MissingTableError(LookupError):
    """A required Navi table (or its primary key) is absent. Not retryable."""


TABLE_MISSING = "skipped: table missing"


def available_columns(conn: psycopg.Connection[Any], spec: NaviTable) -> list[str] | None:
    """Allowlisted columns present in the source, or None if the table is absent."""
    rows = conn.execute(
        """
        select column_name from information_schema.columns
         where table_schema = current_schema() and table_name = %s
        """,
        (spec.table,),
    ).fetchall()
    present = {r["column_name"] if isinstance(r, dict) else r[0] for r in rows}
    if not present:
        return None
    if "id" not in present:
        msg = f"navi table {spec.table} has no id column"
        raise MissingTableError(msg)
    return [c for c in spec.columns if c in present]


def fetch_page(
    conn: psycopg.Connection[Any],
    spec: NaviTable,
    columns: list[str],
    *,
    since: dt.datetime | None,
    after: Cursor | None,
    limit: int,
) -> list[dict[str, Any]]:
    """One keyset-paginated page ordered by (watermark, id)."""
    cols = sql.SQL(", ").join(sql.Identifier(c) for c in columns)
    table = sql.Identifier(spec.table)
    id_cast = trusted_sql(spec.id_type)
    conds: list[sql.Composable] = []
    params: list[Any] = []

    if spec.watermark is None:
        ts_expr: sql.Composable = sql.SQL("null::timestamptz")
        order = sql.SQL("id")
        if after is not None:
            conds.append(sql.SQL("id > %s::{}").format(id_cast))
            params.append(after.id)
    else:
        ts_expr = trusted_sql(spec.watermark)
        order = sql.SQL("{ts}, id").format(ts=ts_expr)
        if since is not None:
            conds.append(sql.SQL("{ts} >= %s").format(ts=ts_expr))
            params.append(since)
        if after is not None:
            conds.append(
                sql.SQL("({ts}, id) > (%s::timestamptz, %s::{id_type})").format(
                    ts=ts_expr, id_type=id_cast
                )
            )
            params.extend([after.ts, after.id])

    where = sql.SQL(" where ") + sql.SQL(" and ").join(conds) if conds else sql.SQL("")
    query = sql.SQL(
        "select {cols}, {ts} as _wm from {table}{where} order by {order} limit %s"
    ).format(cols=cols, ts=ts_expr, table=table, where=where, order=order)
    params.append(limit)
    return conn.execute(query, params).fetchall()


def to_record(spec: NaviTable, row: dict[str, Any], salt: bytes) -> dict[str, Any]:
    """Source row -> raw record, with user identifiers pseudonymised."""
    data = {k: v for k, v in row.items() if k != "_wm"}
    for col, namespace in spec.pseudonymise.items():
        if col in data:
            data[col] = pseudonymise(salt, namespace, data[col])
    return {"id": data["id"], "data": data, "source_ts": row["_wm"]}


@dataclass
class ExtractResult:
    table: str
    rows: int
    pages: int
    watermark: str | None
    status: str = "ok"
    missing_columns: list[str] = field(default_factory=list)


def extract_table(
    spec: NaviTable,
    *,
    navi: psycopg.Connection[Any],
    warehouse: psycopg.Connection[Any],
    salt: bytes,
    run_id: str,
    page_size: int,
    lookback: dt.timedelta,
    full_refresh: bool,
    resume_from: Cursor | None,
    heartbeat: Callable[[dict[str, Any]], None],
) -> ExtractResult:
    """Incrementally copy one table into raw.*, committing page by page.

    Restart-safe: each page is committed with its upserts, the keyset cursor is
    heartbeated, and the watermark is only advanced once the whole table is read.
    """
    from crumb import warehouse as wh  # noqa: PLC0415

    columns = available_columns(navi, spec)
    navi.rollback()
    wh.ensure_raw_table(warehouse, spec.raw_table)
    if columns is None:
        if spec.required:
            msg = f"required navi table {spec.table} not found"
            raise MissingTableError(msg)
        log.warning("navi table %s not found in source; skipping (optional)", spec.table)
        with warehouse.transaction():
            wh.mark_skipped(warehouse, SOURCE, spec.table, TABLE_MISSING, run_id)
        return ExtractResult(table=spec.table, rows=0, pages=0, watermark=None, status=TABLE_MISSING)
    missing = [c for c in spec.columns if c not in columns]
    if missing:
        log.warning("navi table %s lacks columns %s; extracting without them", spec.table, missing)
    stored = None if full_refresh else wh.get_watermark(warehouse, SOURCE, spec.table)
    since = stored - lookback if stored and spec.watermark else None

    cursor = resume_from
    max_ts: dt.datetime | None = stored
    if cursor is not None and cursor.ts is not None:
        # Pages before the resume point were committed by the previous attempt.
        resumed_ts = dt.datetime.fromisoformat(cursor.ts)
        max_ts = resumed_ts if max_ts is None else max(max_ts, resumed_ts)
    rows = pages = 0
    while True:
        page = fetch_page(navi, spec, columns, since=since, after=cursor, limit=page_size)
        navi.rollback()  # end the read transaction between pages on the replica
        if not page:
            break
        records = [to_record(spec, r, salt) for r in page]
        with warehouse.transaction():
            wh.upsert_raw(warehouse, spec.raw_table, records, run_id)
        rows += len(page)
        pages += 1
        last = page[-1]
        last_ts = last["_wm"]
        if last_ts is not None and (max_ts is None or last_ts > max_ts):
            max_ts = last_ts
        cursor = Cursor(ts=last_ts.isoformat() if last_ts else None, id=str(last["id"]))
        heartbeat({"cursor": cursor.to_json(), "rows": rows})
        if len(page) < page_size:
            break

    with warehouse.transaction():
        wh.set_watermark(warehouse, SOURCE, spec.table, max_ts, rows, run_id)
    return ExtractResult(
        table=spec.table,
        rows=rows,
        pages=pages,
        watermark=max_ts.isoformat() if max_ts else None,
        missing_columns=missing,
    )
