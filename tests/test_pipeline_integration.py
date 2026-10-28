"""Activities against real Postgres: extract, transform, quality, reconciliation."""

import datetime as dt
from typing import Any

import psycopg
import pytest
from psycopg import sql
from temporalio.exceptions import ApplicationError
from temporalio.testing import ActivityEnvironment

from crumb import quality
from crumb import warehouse as wh
from crumb.db import connect
from crumb.sources import navi
from crumb.sources import stripe_source
from crumb.temporal import activities as act
from crumb.temporal.types import ExtractTableInput
from crumb.temporal.types import StepInput
from crumb.transform import SqlFileTransformer
from tests.conftest import TEST_SALT

pytestmark = [pytest.mark.integration, pytest.mark.usefixtures("settings_env")]

Conn = psycopg.Connection[dict[str, Any]]


def extract_all(full_refresh: bool = False) -> int:
    env = ActivityEnvironment()
    total = 0
    for table in navi.TABLES:
        result = env.run(act.extract_navi_table, ExtractTableInput("run-test", table, full_refresh))
        total += result["rows"]
    return total


def count(conn: Conn, table: str) -> int:
    row = conn.execute(
        sql.SQL("select count(*) as n from {}").format(sql.Identifier(*table.split(".")))
    ).fetchone()
    assert row is not None
    return row["n"]


def test_extract_lands_everything_without_pii(wh_conn: Conn, navi_dsn: str) -> None:
    extract_all()
    with connect(navi_dsn) as src:
        for spec in navi.TABLES.values():
            assert count(wh_conn, f"raw.{spec.raw_table}") == count(src, spec.table), spec.table
        emails = [r["email"] for r in src.execute("select email from users_user").fetchall()]
    blob = " ".join(
        r["data"]
        for r in wh_conn.execute("select data::text as data from raw.navi_users_user").fetchall()
    )
    assert not any(e in blob for e in emails)
    assert "cus_seed" not in blob and "+1555" not in blob
    order = wh_conn.execute("select data from raw.navi_orders_order limit 1").fetchone()
    assert order is not None
    assert "cart_token" not in order["data"] and "qr_nonce" not in order["data"]
    assert len(order["data"]["user_id"]) == 32  # pseudonymised key, not the integer id
    marks = wh_conn.execute("select * from meta.watermarks where source = 'navi'").fetchall()
    assert len(marks) == len(navi.TABLES)
    assert all(m["last_success_at"] for m in marks)


def test_rerun_is_idempotent_and_incremental(wh_conn: Conn, navi_dsn: str) -> None:
    first = extract_all()
    before = {s.raw_table: count(wh_conn, f"raw.{s.raw_table}") for s in navi.TABLES.values()}
    second = extract_all()
    after = {s.raw_table: count(wh_conn, f"raw.{s.raw_table}") for s in navi.TABLES.values()}
    assert before == after
    # Incremental: only the lookback window (and timestamp-less tables) is re-read.
    assert second < first / 10

    # A source update past the watermark is picked up and upserted, not duplicated.
    with connect(navi_dsn, autocommit=True) as src:
        src.execute(
            "update orders_order set order_status = 'C', updated_at = now() "
            "where id = (select id from orders_order order by created_at limit 1)"
        )
    extract_all()
    assert count(wh_conn, "raw.navi_orders_order") == before["navi_orders_order"]
    assert wh_conn.execute(
        "select count(*) as n from raw.navi_orders_order where data->>'order_status' = 'C' "
        "and source_ts > now() - interval '1 minute'"
    ).fetchone() == {"n": 1}


def test_extract_resumes_from_heartbeat_cursor(wh_conn: Conn, navi_dsn: str) -> None:
    spec = navi.TABLES["devices_deviceevent"]
    beats: list[dict[str, Any]] = []
    with connect(navi_dsn, read_only=True) as src:
        navi.extract_table(
            spec,
            navi=src,
            warehouse=wh_conn,
            salt=TEST_SALT.encode(),
            run_id="r1",
            page_size=500,
            lookback=dt.timedelta(0),
            full_refresh=True,
            resume_from=None,
            heartbeat=beats.append,
        )
        total = count(wh_conn, f"raw.{spec.raw_table}")
        assert len(beats) > 2
        wh_conn.execute(sql.SQL("truncate {}").format(sql.Identifier("raw", spec.raw_table)))
        # Resume from the second page's cursor: only rows after it are read.
        resumed = navi.extract_table(
            spec,
            navi=src,
            warehouse=wh_conn,
            salt=TEST_SALT.encode(),
            run_id="r2",
            page_size=500,
            lookback=dt.timedelta(0),
            full_refresh=True,
            resume_from=navi.Cursor.from_json(beats[1]["cursor"]),
            heartbeat=lambda _: None,
        )
    assert resumed.rows == total - beats[1]["rows"]


def test_transform_and_quality_pass(wh_conn: Conn) -> None:
    extract_all()
    env = ActivityEnvironment()
    built = env.run(act.run_transform, StepInput("run-t"))
    assert built["rows"]["marts.daily_port_sales"] > 0
    assert built["rows"]["marts.fct_orders"] == built["rows"]["staging.stg_orders"]
    summary = env.run(act.run_quality_checks, StepInput("run-t"))
    assert summary["failed"] == 0
    assert summary["passed"] > 20
    stored = count(wh_conn, "meta.dq_results")
    assert stored == summary["passed"] + summary["skipped"]

    sales = wh_conn.execute(
        "select sum(orders) as orders, sum(revenue) as revenue from marts.daily_port_sales"
    ).fetchone()
    expected = wh_conn.execute(
        "select count(*) as orders, sum(total_amount) as revenue from staging.stg_payments p "
        "join staging.stg_orders o using (payment_id) where p.is_charged and not o.is_test"
    ).fetchone()
    assert sales == expected
    uptime = wh_conn.execute(
        "select min(uptime_ratio) as lo, max(uptime_ratio) as hi from marts.machine_uptime_daily"
    ).fetchone()
    assert uptime is not None and 0 <= uptime["lo"] <= uptime["hi"] <= 1


def test_quality_failure_is_loud(wh_conn: Conn) -> None:
    extract_all()
    env = ActivityEnvironment()
    env.run(act.run_transform, StepInput("run-q"))
    wh_conn.execute("insert into marts.fct_orders select * from marts.fct_orders limit 1")
    with pytest.raises(ApplicationError) as err:
        env.run(act.run_quality_checks, StepInput("run-q"))
    assert err.value.type == "DataQualityError" and err.value.non_retryable
    assert "unique:marts.fct_orders" in str(err.value)
    assert wh_conn.execute(
        "select status from meta.dq_results where check_name = 'unique:marts.fct_orders'"
    ).fetchone() == {"status": "failed"}


class FakeStripe:
    """Two pages per object, newest first, like the real API."""

    def __init__(self, objects: dict[str, list[dict[str, Any]]]) -> None:
        self.objects = objects
        self.calls: list[tuple[str, int | None, str | None]] = []

    def list_page(
        self, obj: str, *, created_gte: int | None, starting_after: str | None, limit: int
    ) -> tuple[list[dict[str, Any]], bool]:
        self.calls.append((obj, created_gte, starting_after))
        items = sorted(self.objects.get(obj, []), key=lambda i: -i["created"])
        if created_gte is not None:
            items = [i for i in items if i["created"] >= created_gte]
        if starting_after:
            idx = next(k for k, i in enumerate(items) if i["id"] == starting_after)
            items = items[idx + 1 :]
        return items[:limit], len(items) > limit


def _charge(
    cid: str, pi: str, cents: int, created: int, order_id: str = "", refunded: int = 0
) -> dict[str, Any]:
    return {
        "id": cid,
        "payment_intent": pi,
        "amount": cents,
        "amount_captured": cents,
        "amount_refunded": refunded,
        "currency": "usd",
        "status": "succeeded",
        "paid": True,
        "created": created,
        "metadata": {"order_id": order_id} if order_id else {},
        "billing_details": {"email": "nope@example.test"},
    }


def test_reconciliation_flags_orphans_double_charges_and_mismatches(wh_conn: Conn) -> None:
    extract_all()
    charged = wh_conn.execute(
        """
        select data->>'stripe_payment_intent_id' as pi, data->>'total_amount' as total,
               data->>'amount_refunded' as refunded
          from raw.navi_payments_payment
         where data->>'status' in ('succeeded', 'refunded')
         order by id
        """
    ).fetchall()
    now = int(dt.datetime.now(dt.UTC).timestamp())
    charges = [
        _charge(
            f"ch_{k}",
            r["pi"],
            round(float(r["total"]) * 100),
            now - k,
            refunded=round(float(r["refunded"]) * 100),
        )
        for k, r in enumerate(charged)
    ]
    good = len(charges)
    charges.append(_charge("ch_orphan", "pi_unknown", 450, now))  # no Navi order
    charges.append(_charge("ch_dup", charged[0]["pi"], 450, now))  # second charge, same PI
    charges[1]["amount_refunded"] += 100  # Stripe refunded $1 Navi doesn't know about
    fake = FakeStripe({"charges": charges, "payment_intents": [], "refunds": []})

    for obj in stripe_source.OBJECTS.values():
        stripe_source.extract_object(
            obj,
            lister=fake,
            warehouse=wh_conn,
            salt=TEST_SALT.encode(),
            run_id="s1",
            page_size=50,
            lookback=dt.timedelta(days=3),
            full_refresh=False,
            resume_after=None,
            heartbeat=lambda _: None,
        )
    assert count(wh_conn, "raw.stripe_charges") == good + 2
    assert "billing_details" not in str(
        wh_conn.execute("select data from raw.stripe_charges limit 1").fetchone()
    )

    SqlFileTransformer().run(wh_conn, timezone="UTC")
    issues = {
        r["issue_type"]: r["n"]
        for r in wh_conn.execute(
            "select issue_type, count(*) as n from marts.reconciliation_issues group by 1"
        ).fetchall()
    }
    assert issues == {"charge_without_order": 1, "double_charge": 2, "refund_mismatch": 1}
    result = quality.check_reconciliation(wh_conn)
    assert result.status == "failed"
    daily = wh_conn.execute(
        "select bool_and(stripe_loaded) as loaded, sum(charges_without_order) as orphans "
        "from marts.refunds_daily"
    ).fetchone()
    assert daily == {"loaded": True, "orphans": 1}

    # Second run pages from (watermark - lookback), not from the beginning of time.
    fake.calls.clear()
    stripe_source.extract_object(
        stripe_source.OBJECTS["charges"],
        lister=fake,
        warehouse=wh_conn,
        salt=TEST_SALT.encode(),
        run_id="s2",
        page_size=50,
        lookback=dt.timedelta(seconds=10),
        full_refresh=False,
        resume_after=None,
        heartbeat=lambda _: None,
    )
    assert fake.calls[0][1] is not None
    assert wh.get_watermark(wh_conn, "stripe", "charges") is not None


def test_stripe_skips_without_key() -> None:
    assert ActivityEnvironment().run(act.stripe_configured) is False
