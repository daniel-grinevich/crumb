"""Data-quality checks run after the transform. Failures fail the workflow.

Checks:
  * unique:<model>       - declared ``unique_key`` has no duplicates or nulls
  * rowcount:<model>     - staging models built from one raw table keep every row
  * not_empty:<model>    - core marts are populated when their source has rows
  * freshness:<src>.<t>  - last successful extract is recent enough
  * reconciliation       - no Stripe <-> Navi mismatches among charges belonging to this
                           environment (skipped until Stripe loads); fails or only warns
                           per CRUMB_RECONCILIATION_SEVERITY
"""

import datetime as dt
from dataclasses import asdict
from dataclasses import dataclass
from dataclasses import field
from typing import Any

import psycopg
from psycopg import sql

from crumb.db import jsonb
from crumb.sources import navi
from crumb.transform import Model

NOT_EMPTY = {
    "marts.fct_orders": "staging.stg_orders",
    "marts.daily_port_sales": "marts.fct_orders",
    "marts.machine_uptime_daily": "staging.stg_device_events",
}


class DataQualityError(RuntimeError):
    """One or more checks failed. Never retried: rerunning won't fix data."""


@dataclass
class CheckResult:
    name: str
    status: str  # passed | failed | skipped
    message: str = ""
    observed: dict[str, Any] = field(default_factory=dict)


def _scalar(
    conn: psycopg.Connection[Any], query: sql.Composed | sql.SQL, params: Any = None
) -> Any:
    row = conn.execute(query, params).fetchone()
    assert row is not None
    return next(iter(row.values())) if isinstance(row, dict) else row[0]


def check_unique(conn: psycopg.Connection[Any], model: Model) -> CheckResult:
    keys = sql.SQL(", ").join(sql.Identifier(c) for c in model.unique_key)
    any_null = sql.SQL(" or ").join(
        sql.SQL("{} is null").format(sql.Identifier(c)) for c in model.unique_key
    )
    table = sql.Identifier(model.layer, model.name)
    dupes = _scalar(
        conn,
        sql.SQL(
            "select count(*) from (select {k} from {t} group by {k} having count(*) > 1) d"
        ).format(k=keys, t=table),
    )
    nulls = _scalar(conn, sql.SQL("select count(*) from {t} where {n}").format(t=table, n=any_null))
    name = f"unique:{model.fqn}"
    observed = {"duplicate_keys": dupes, "null_keys": nulls}
    if dupes or nulls:
        msg = f"{dupes} duplicate and {nulls} null key(s) on ({', '.join(model.unique_key)})"
        return CheckResult(name, "failed", msg, observed)
    return CheckResult(name, "passed", observed=observed)


def check_rowcount(conn: psycopg.Connection[Any], model: Model) -> CheckResult:
    (raw_table,) = model.raw_sources
    raw_n = _scalar(
        conn, sql.SQL("select count(*) from {}").format(sql.Identifier("raw", raw_table))
    )
    model_n = _scalar(
        conn, sql.SQL("select count(*) from {}").format(sql.Identifier(model.layer, model.name))
    )
    observed = {"raw": raw_n, "model": model_n}
    name = f"rowcount:{model.fqn}"
    if raw_n != model_n:
        return CheckResult(
            name, "failed", f"raw.{raw_table} has {raw_n} rows, model {model_n}", observed
        )
    return CheckResult(name, "passed", observed=observed)


def check_not_empty(conn: psycopg.Connection[Any], target: str, source: str) -> CheckResult:
    def count(fqn: str) -> int:
        schema, table = fqn.split(".")
        return _scalar(
            conn, sql.SQL("select count(*) from {}").format(sql.Identifier(schema, table))
        )

    src_n, tgt_n = count(source), count(target)
    observed = {"source": src_n, "target": tgt_n}
    name = f"not_empty:{target}"
    if src_n == 0:
        return CheckResult(name, "skipped", f"{source} is empty", observed)
    if tgt_n == 0:
        return CheckResult(
            name, "failed", f"{target} is empty but {source} has {src_n} rows", observed
        )
    return CheckResult(name, "passed", observed=observed)


def check_freshness(
    conn: psycopg.Connection[Any], *, max_age: dt.timedelta, stripe_enabled: bool
) -> list[CheckResult]:
    rows = conn.execute(
        "select source, table_name, last_success_at, status from meta.watermarks"
    ).fetchall()
    seen = {(r["source"], r["table_name"]): r["last_success_at"] for r in rows}
    statuses = {(r["source"], r["table_name"]): r["status"] for r in rows}
    expected = [(navi.SOURCE, t) for t in navi.TABLES]
    if stripe_enabled:
        from crumb.sources import stripe_source

        expected += [(stripe_source.SOURCE, o) for o in stripe_source.OBJECTS]
    now = dt.datetime.now(dt.UTC)
    results = []
    for source, table in expected:
        name = f"freshness:{source}.{table}"
        status = statuses.get((source, table), "ok")
        if status != "ok":
            # Optional table absent from this Navi version; required ones fail
            # the extract before we get here.
            results.append(CheckResult(name, "skipped", status))
            continue
        last = seen.get((source, table))
        if last is None:
            results.append(CheckResult(name, "failed", "never extracted successfully"))
            continue
        age = now - last
        observed = {"last_success_at": last.isoformat(), "age_seconds": int(age.total_seconds())}
        if age > max_age:
            results.append(
                CheckResult(name, "failed", f"last success {age} ago (max {max_age})", observed)
            )
        else:
            results.append(CheckResult(name, "passed", observed=observed))
    return results


def check_reconciliation(conn: psycopg.Connection[Any], severity: str = "fail") -> CheckResult:
    """Mismatches are always recorded (marts.reconciliation_issues, dq_results);
    ``severity`` decides whether they fail the run or only warn."""
    loaded = _scalar(conn, sql.SQL("select exists (select 1 from staging.stg_stripe_charges)"))
    if not loaded:
        return CheckResult("reconciliation", "skipped", "no Stripe data loaded yet")
    rows = conn.execute(
        "select issue_type, count(*) as n from marts.reconciliation_issues group by 1 order by 1"
    ).fetchall()
    observed: dict[str, Any] = {r["issue_type"]: r["n"] for r in rows}
    scope = conn.execute(
        "select scope, count(*) as n from marts.stripe_charge_scope group by 1"
    ).fetchall()
    charges_by_scope = {r["scope"]: r["n"] for r in scope}
    issues = dict(observed)
    observed["charges_by_scope"] = charges_by_scope
    if issues:
        summary = ", ".join(f"{k}={v}" for k, v in issues.items())
        status = "failed" if severity == "fail" else "warned"
        return CheckResult("reconciliation", status, f"Stripe/Navi mismatches: {summary}", observed)
    return CheckResult("reconciliation", "passed", observed=observed)


def run_checks(
    conn: psycopg.Connection[Any],
    models: list[Model],
    *,
    max_age: dt.timedelta,
    stripe_enabled: bool,
    run_id: str,
    reconciliation_severity: str = "fail",
) -> list[CheckResult]:
    results: list[CheckResult] = []
    for model in models:
        if model.unique_key:
            results.append(check_unique(conn, model))
        if model.layer == "staging" and len(model.raw_sources) == 1 and not model.depends_on:
            results.append(check_rowcount(conn, model))
    for target, source in NOT_EMPTY.items():
        results.append(check_not_empty(conn, target, source))
    results.extend(check_freshness(conn, max_age=max_age, stripe_enabled=stripe_enabled))
    results.append(check_reconciliation(conn, reconciliation_severity))
    conn.rollback()

    with conn.transaction(), conn.cursor() as cur:
        cur.executemany(
            """
                insert into meta.dq_results (run_id, check_name, status, observed, message)
                values (%s, %s, %s, %s, %s)
                """,
            [(run_id, r.name, r.status, jsonb(r.observed), r.message) for r in results],
        )
    return results


def summarise(results: list[CheckResult]) -> dict[str, Any]:
    failed = [asdict(r) for r in results if r.status == "failed"]
    warned = [asdict(r) for r in results if r.status == "warned"]
    return {
        "passed": sum(r.status == "passed" for r in results),
        "skipped": sum(r.status == "skipped" for r in results),
        "warned": len(warned),
        "failed": len(failed),
        "warnings": warned,
        "failures": failed,
    }
