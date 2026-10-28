"""Whitelisted, read-only mart access. Identifiers come only from MARTS; user
input only ever reaches the query as bound parameters."""

import datetime as dt
from dataclasses import dataclass
from typing import Any

import psycopg
from psycopg import sql


@dataclass(frozen=True)
class MartSpec:
    description: str
    date_column: str | None
    port_column: str | None
    order_by: tuple[str, ...]


MARTS: dict[str, MartSpec] = {
    "fct_orders": MartSpec(
        "One row per Navi order with payment, revenue and refund amounts.",
        "order_date",
        "port_id",
        ("order_date desc", "created_at desc", "order_id"),
    ),
    "fct_order_items": MartSpec(
        "One row per order line (menu item + customizations).",
        "order_date",
        "port_id",
        ("order_date desc", "order_id", "order_item_id"),
    ),
    "daily_port_sales": MartSpec(
        "Per NaviPort per day: orders, revenue, refunds, average ticket.",
        "date",
        "port_id",
        ("date desc", "port_name", "port_id"),
    ),
    "machine_uptime_daily": MartSpec(
        "Per Raspberry Pi per day: uptime from connect/disconnect/heartbeat events.",
        "date",
        "port_id",
        ("date desc", "pi_name", "pi_id"),
    ),
    "drink_popularity": MartSpec(
        "Per day per port per menu item: quantity, orders, revenue, rank.",
        "date",
        "port_id",
        ("date desc", "port_id", "rank_in_port_day", "menu_item_id"),
    ),
    "refunds_daily": MartSpec(
        "Daily refunds from Navi and Stripe, with reconciliation issue counts.",
        "date",
        None,
        ("date desc",),
    ),
    "reconciliation_issues": MartSpec(
        "Stripe <-> Navi mismatches (orphan charges, double charges, ...).",
        "date",
        None,
        ("date desc", "issue_type", "charge_id", "payment_intent_id"),
    ),
    "customer_cohorts": MartSpec(
        "Pseudonymised first-order-month cohorts with retention and repeat rate.",
        "cohort_month",
        None,
        ("cohort_month desc", "month_offset"),
    ),
}


def _order_clause(spec: MartSpec) -> sql.Composed:
    parts: list[sql.Composable] = []
    for item in spec.order_by:
        col, _, direction = item.partition(" ")
        parts.append(
            sql.SQL("{} {}").format(
                sql.Identifier(col), sql.SQL("desc" if direction == "desc" else "asc")
            )
        )
    return sql.SQL(", ").join(parts)


def query_mart(
    conn: psycopg.Connection[Any],
    name: str,
    *,
    start_date: dt.date | None,
    end_date: dt.date | None,
    port_id: str | None,
    limit: int,
    offset: int,
) -> tuple[int, list[dict[str, Any]]]:
    spec = MARTS[name]
    conds: list[sql.Composable] = []
    params: list[Any] = []
    if spec.date_column and start_date:
        conds.append(sql.SQL("{} >= %s").format(sql.Identifier(spec.date_column)))
        params.append(start_date)
    if spec.date_column and end_date:
        conds.append(sql.SQL("{} <= %s").format(sql.Identifier(spec.date_column)))
        params.append(end_date)
    if spec.port_column and port_id:
        conds.append(sql.SQL("{}::text = %s").format(sql.Identifier(spec.port_column)))
        params.append(port_id)
    where = sql.SQL(" where ") + sql.SQL(" and ").join(conds) if conds else sql.SQL("")
    table = sql.Identifier("marts", name)

    total_row = conn.execute(
        sql.SQL("select count(*) as n from {}{}").format(table, where), params
    ).fetchone()
    rows = conn.execute(
        sql.SQL("select * from {}{} order by {} limit %s offset %s").format(
            table, where, _order_clause(spec)
        ),
        [*params, limit, offset],
    ).fetchall()
    return (total_row["n"] if total_row else 0), rows


def mart_columns(conn: psycopg.Connection[Any]) -> dict[str, list[dict[str, str]]]:
    rows = conn.execute(
        """
        select table_name, column_name, data_type
          from information_schema.columns
         where table_schema = 'marts'
         order by table_name, ordinal_position
        """
    ).fetchall()
    out: dict[str, list[dict[str, str]]] = {}
    for r in rows:
        out.setdefault(r["table_name"], []).append(
            {"name": r["column_name"], "type": r["data_type"]}
        )
    return out
