"""Pure unit tests: no database, no Temporal."""

import datetime as dt
import decimal
import uuid

import pytest

from crumb.config import ConfigError
from crumb.config import Settings
from crumb.db import dumps
from crumb.pii import pseudonymise
from crumb.sources import navi
from crumb.sources import stripe_source
from crumb.transform import SqlFileTransformer
from crumb.transform import order_models
from crumb.transform import parse_model

SALT = b"unit-test-salt-0123456789"
# "name" is fine on ports/menu items; on users it's checked separately below.
PII_COLUMNS = {
    "email",
    "phone",
    "first_name",
    "last_name",
    "password",
    "cart_token",
    "qr_nonce",
    "cancel_reason",
    "token_hash",
    "device_token",
    "mac_address",
    "ip_address",
    "last_modified_ip",
    "last_modified_user_agent",
    "message",
    "payload",
    "status_message",
    "address_line_1",
}


def test_pseudonymise_is_stable_keyed_and_namespaced() -> None:
    a = pseudonymise(SALT, "user", 42)
    assert a == pseudonymise(SALT, "user", 42)
    assert a != pseudonymise(b"another-salt-0123456789", "user", 42)
    assert a != pseudonymise(SALT, "stripe_customer", 42)
    assert a is not None and len(a) == 32 and "42" not in a
    assert pseudonymise(SALT, "user", None) is None
    assert pseudonymise(SALT, "user", "") is None


def test_no_navi_spec_selects_pii_columns() -> None:
    for spec in navi.TABLES.values():
        assert not PII_COLUMNS & set(spec.columns), spec.table
    assert "name" not in navi.TABLES["users_user"].columns


def test_every_user_reference_is_pseudonymised() -> None:
    for spec in navi.TABLES.values():
        user_cols = {
            c
            for c in spec.columns
            if c in ("user_id", "actor_id", "created_by_id", "updated_by_id")
        }
        assert user_cols <= set(spec.pseudonymise), spec.table
    assert navi.TABLES["users_user"].pseudonymise["id"] == "user"


def test_navi_to_record_pseudonymises() -> None:
    spec = navi.TABLES["orders_order"]
    ts = dt.datetime(2026, 1, 1, tzinfo=dt.UTC)
    row = {"id": uuid.uuid4(), "user_id": 7, "created_by_id": 7, "updated_by_id": 1, "_wm": ts}
    rec = navi.to_record(spec, row, SALT)
    assert rec["source_ts"] == ts
    assert "_wm" not in rec["data"]
    assert rec["data"]["user_id"] == pseudonymise(SALT, "user", 7)
    assert rec["data"]["created_by_id"] == rec["data"]["user_id"]


def test_stripe_to_record_drops_unlisted_fields() -> None:
    obj = stripe_source.OBJECTS["charges"]
    item = {
        "id": "ch_1",
        "amount": 500,
        "created": 1_760_000_000,
        "customer": "cus_9",
        "billing_details": {"email": "a@b.c", "name": "Ann"},
        "receipt_email": "a@b.c",
        "metadata": {"order_id": "o1", "note": "call Ann"},
        "status": "succeeded",
    }
    rec = stripe_source.to_record(obj, item, SALT)
    assert "billing_details" not in rec["data"] and "receipt_email" not in rec["data"]
    assert rec["data"]["metadata"] == {"order_id": "o1"}
    assert rec["data"]["customer"] == pseudonymise(SALT, "stripe_customer", "cus_9")
    assert rec["source_ts"] == dt.datetime.fromtimestamp(1_760_000_000, tz=dt.UTC)


def test_dumps_keeps_money_exact() -> None:
    out = dumps({"a": decimal.Decimal("4.50"), "t": dt.datetime(2026, 1, 1, tzinfo=dt.UTC)})
    assert '"4.50"' in out and "2026-01-01T00:00:00+00:00" in out


def test_parse_model_detects_dependencies_and_headers() -> None:
    m = parse_model(
        "marts",
        "x",
        "-- unique_key: a, b\nselect * from staging.s join marts.y using (a) join raw.r using (a)",
    )
    assert m.depends_on == {"staging.s", "marts.y"}
    assert m.raw_sources == {"r"}
    assert m.unique_key == ("a", "b")


def test_order_models_is_topological_and_rejects_unknown() -> None:
    a = parse_model("staging", "a", "select 1")
    b = parse_model("marts", "b", "select * from staging.a")
    c = parse_model("marts", "c", "select * from marts.b")
    assert [m.fqn for m in order_models([c, b, a])] == ["staging.a", "marts.b", "marts.c"]
    with pytest.raises(ValueError, match="unknown"):
        order_models([parse_model("marts", "d", "select * from staging.nope")])


def test_shipped_models_load_in_dependency_order() -> None:
    models = SqlFileTransformer().models()
    position = {m.fqn: i for i, m in enumerate(models)}
    for m in models:
        for dep in m.depends_on:
            assert position[dep] < position[m.fqn]
    names = {m.fqn for m in models}
    for mart in (
        "fct_orders",
        "fct_order_items",
        "daily_port_sales",
        "machine_uptime_daily",
        "drink_popularity",
        "refunds_daily",
        "customer_cohorts",
    ):
        assert f"marts.{mart}" in names


def test_pii_salt_required(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("CRUMB_PII_SALT", raising=False)
    monkeypatch.delenv("PII_SALT", raising=False)
    with pytest.raises(ConfigError):
        Settings(_env_file=None).require_pii_salt()  # pyright: ignore[reportCallIssue]


def test_deployment_env_names_are_accepted(monkeypatch: pytest.MonkeyPatch) -> None:
    env = {
        "WAREHOUSE_DSN": "postgresql://w/x",
        "NAVI_SOURCE_DSN": "postgresql://n/y",
        "TEMPORAL_ADDRESS": "temporal:7233",
        "TEMPORAL_NAMESPACE": "crumb",
        "STRIPE_API_KEY": "sk_test_x",
        "CRUMB_API_KEY": "k",
        "PII_SALT": "s" * 20,
    }
    for k, v in env.items():
        monkeypatch.setenv(k, v)
    s = Settings(_env_file=None)  # pyright: ignore[reportCallIssue]
    assert s.warehouse_dsn.get_secret_value() == "postgresql://w/x"
    assert s.navi_dsn.get_secret_value() == "postgresql://n/y"
    assert (s.temporal_address, s.temporal_namespace) == ("temporal:7233", "crumb")
    assert s.stripe_api_key is not None and s.api_key is not None
    assert s.require_pii_salt() == b"s" * 20
