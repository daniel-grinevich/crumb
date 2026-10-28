"""Stripe source: PaymentIntents, Charges and Refunds, incremental by ``created``.

Only allowlisted fields land; billing details, receipt emails, card details and
the rest of the object are dropped. ``customer`` is pseudonymised with the same
namespace as Navi's ``users_user.stripe_customer_id`` so the two still join.
"""

import datetime as dt
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any
from typing import Protocol

import psycopg

from crumb.pii import pseudonymise

SOURCE = "stripe"


@dataclass(frozen=True)
class StripeObject:
    name: str  # also the raw table suffix and the watermark table name
    fields: tuple[str, ...]

    @property
    def raw_table(self) -> str:
        return f"stripe_{self.name}"


OBJECTS: dict[str, StripeObject] = {
    o.name: o
    for o in [
        StripeObject(
            "payment_intents",
            ("id", "amount", "amount_received", "currency", "status", "created",
             "customer", "latest_charge", "livemode", "canceled_at",
             "cancellation_reason"),
        ),
        StripeObject(
            "charges",
            ("id", "amount", "amount_captured", "amount_refunded", "currency",
             "status", "paid", "refunded", "captured", "disputed", "created",
             "payment_intent", "customer", "failure_code", "livemode"),
        ),
        StripeObject(
            "refunds",
            ("id", "amount", "currency", "status", "created", "charge",
             "payment_intent", "reason", "livemode"),
        ),
    ]
}

# Metadata keys that are safe to keep. Navi sets order_id on PaymentIntents;
# environment is honoured if Navi starts tagging objects (shared test accounts).
METADATA_KEYS = ("order_id", "environment")


class StripeLister(Protocol):
    def list_page(
        self, obj: str, *, created_gte: int | None, starting_after: str | None, limit: int
    ) -> tuple[list[dict[str, Any]], bool]: ...


class StripeSdkLister:
    """Thin wrapper over the official SDK so tests can swap in a fake."""

    def __init__(self, api_key: str) -> None:
        import stripe

        self._client = stripe.StripeClient(api_key, max_network_retries=3)

    def list_page(
        self, obj: str, *, created_gte: int | None, starting_after: str | None, limit: int
    ) -> tuple[list[dict[str, Any]], bool]:
        service = getattr(self._client.v1, obj)
        params: dict[str, Any] = {"limit": limit}
        if created_gte is not None:
            params["created"] = {"gte": created_gte}
        if starting_after:
            params["starting_after"] = starting_after
        page = service.list(params=params)
        return [item.to_dict() for item in page.data], bool(page.has_more)


def to_record(obj: StripeObject, item: dict[str, Any], salt: bytes) -> dict[str, Any]:
    data = {f: item.get(f) for f in obj.fields}
    customer = data.get("customer")
    if isinstance(customer, dict):  # expanded object
        data["customer"] = customer.get("id")
    if "customer" in data:
        data["customer"] = pseudonymise(salt, "stripe_customer", data["customer"])
    metadata = item.get("metadata") or {}
    data["metadata"] = {k: metadata[k] for k in METADATA_KEYS if k in metadata}
    created = dt.datetime.fromtimestamp(item["created"], tz=dt.UTC)
    return {"id": item["id"], "data": data, "source_ts": created}


@dataclass
class ExtractResult:
    table: str
    rows: int
    pages: int
    watermark: str | None


def extract_object(
    obj: StripeObject,
    *,
    lister: StripeLister,
    warehouse: psycopg.Connection[Any],
    salt: bytes,
    run_id: str,
    page_size: int,
    lookback: dt.timedelta,
    full_refresh: bool,
    resume_after: str | None,
    heartbeat: Callable[[dict[str, Any]], None],
) -> ExtractResult:
    """Page newest-first from (watermark - lookback); upsert each page."""
    from crumb import warehouse as wh

    wh.ensure_raw_table(warehouse, obj.raw_table)
    stored = None if full_refresh else wh.get_watermark(warehouse, SOURCE, obj.name)
    created_gte = int((stored - lookback).timestamp()) if stored else None

    starting_after = resume_after
    max_ts = stored
    rows = pages = 0
    page_size = min(page_size, 100)  # Stripe's maximum
    while True:
        items, has_more = lister.list_page(
            obj.name, created_gte=created_gte, starting_after=starting_after, limit=page_size
        )
        if not items:
            break
        records = [to_record(obj, i, salt) for i in items]
        with warehouse.transaction():
            wh.upsert_raw(warehouse, obj.raw_table, records, run_id)
        rows += len(records)
        pages += 1
        page_max = max(r["source_ts"] for r in records)
        max_ts = page_max if max_ts is None else max(max_ts, page_max)
        starting_after = items[-1]["id"]
        heartbeat({"starting_after": starting_after, "rows": rows})
        if not has_more:
            break

    with warehouse.transaction():
        wh.set_watermark(warehouse, SOURCE, obj.name, max_ts, rows, run_id)
    return ExtractResult(
        table=obj.name, rows=rows, pages=pages, watermark=max_ts.isoformat() if max_ts else None
    )
