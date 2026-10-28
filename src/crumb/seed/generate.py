"""Deterministic synthetic data for the navi_source database.

Run with ``crumb seed``. Recreates every table, so it is safe to rerun. Users get
fake emails, names and phones on purpose: the tests assert none of it reaches
the warehouse.
"""

import datetime as dt
import random
import uuid
from decimal import ROUND_HALF_UP
from decimal import Decimal
from importlib import resources
from typing import Any

import psycopg
from psycopg import sql

from crumb.db import trusted_sql

CENT = Decimal("0.01")
TAX_RATE = Decimal("0.0850")

PORTS = [
    ("Union Station", "Chicago", "IL", "60606", 41.878876, -87.640034),
    ("Ferry Building", "San Francisco", "CA", "94111", 37.795490, -122.393708),
    ("Pike Place", "Seattle", "WA", "98101", 47.609722, -122.342222),
    ("South Station", "Boston", "MA", "02111", 42.352271, -71.055242),
    ("Grand Central", "New York", "NY", "10017", 40.752726, -73.977229),
    ("Union Market", "Washington", "DC", "20002", 38.908611, -76.997222),
]
MENU = {
    "Coffee": [
        ("Espresso", "2.75"),
        ("Americano", "3.25"),
        ("Cappuccino", "4.25"),
        ("Latte", "4.50"),
        ("Flat White", "4.25"),
        ("Mocha", "4.95"),
    ],
    "Tea": [("Chai Latte", "4.50"), ("Matcha Latte", "4.95")],
    "Other": [("Hot Chocolate", "3.75"), ("Steamer", "3.25")],
}
POPULARITY = {
    "Latte": 9,
    "Cappuccino": 7,
    "Americano": 6,
    "Flat White": 5,
    "Espresso": 4,
    "Mocha": 4,
    "Chai Latte": 3,
    "Matcha Latte": 3,
    "Hot Chocolate": 2,
    "Steamer": 1,
}
CUSTOMIZATIONS = [
    ("Oat Milk", "0.75"),
    ("Extra Shot", "1.00"),
    ("Vanilla Syrup", "0.50"),
    ("Caramel Syrup", "0.50"),
    ("Decaf", "0.00"),
    ("Large", "0.75"),
]
FIRST = ["Ava", "Ben", "Cleo", "Dev", "Eli", "Fay", "Gus", "Hana", "Ivan", "June", "Kai", "Lena"]
LAST = ["Park", "Okafor", "Silva", "Novak", "Haddad", "Kim", "Moreau", "Rossi", "Tanaka"]
ERRORS = ["Grinder jam detected", "Water tank low", "Milk frother timeout", "Brew pressure fault"]


def q(value: Decimal) -> Decimal:
    return value.quantize(CENT, rounding=ROUND_HALF_UP)


class Gen:
    def __init__(self, rng: random.Random, now: dt.datetime) -> None:
        self.rng = rng
        self.now = now
        self.rows: dict[str, list[dict[str, Any]]] = {}

    def uid(self) -> uuid.UUID:
        return uuid.UUID(int=self.rng.getrandbits(128), version=4)

    def add(self, table: str, **row: Any) -> dict[str, Any]:
        self.rows.setdefault(table, []).append(row)
        return row

    def audit(self, ts: dt.datetime, staff: int | None = 1, **extra: Any) -> dict[str, Any]:
        return {
            "created_at": ts,
            "updated_at": ts,
            "created_by_id": staff,
            "updated_by_id": staff,
            "last_modified_ip": "203.0.113.7",
            "last_modified_user_agent": "Mozilla/5.0 (seed)",
            **extra,
        }


def generate(
    days: int = 90, users: int = 400, seed: int = 7, now: dt.datetime | None = None
) -> dict[str, list[dict[str, Any]]]:
    rng = random.Random(seed)
    now = (now or dt.datetime.now(dt.UTC)).replace(microsecond=0)
    start = (now - dt.timedelta(days=days)).replace(hour=0, minute=0, second=0)
    g = Gen(rng, now)
    t0 = start - dt.timedelta(days=30)

    # Users: two staff, then customers (a slice of them guests).
    for i in range(1, users + 1):
        first, last = rng.choice(FIRST), rng.choice(LAST)
        joined = t0 + dt.timedelta(seconds=rng.randint(0, int((now - t0).total_seconds() * 0.9)))
        g.add(
            "users_user",
            id=i,
            password="pbkdf2_sha256$seed",
            last_login=None,
            is_superuser=i == 1,
            first_name=first,
            last_name=last,
            is_staff=i <= 2,
            is_active=True,
            date_joined=joined if i > 2 else t0,
            name=f"{first} {last}",
            email=f"{first.lower()}.{last.lower()}{i}@example.test",
            phone=f"+1555{i:07d}",
            stripe_customer_id=f"cus_seed{i:06d}" if i > 2 else None,
            email_confirmed=rng.random() < 0.7,
            is_guest=i > 2 and rng.random() < 0.15,
        )
    customers = [u for u in g.rows["users_user"] if not u["is_staff"]]

    # Menu.
    items: list[dict[str, Any]] = []
    for cat_name, entries in MENU.items():
        cat = g.add(
            "menu_category",
            id=g.uid(),
            name=cat_name,
            slug=cat_name.lower(),
            status="A",
            is_deleted=False,
            deleted_at=None,
            **g.audit(t0),
        )
        for name, price in entries:
            items.append(
                g.add(
                    "menu_menuitem",
                    id=g.uid(),
                    name=name,
                    slug=name.lower().replace(" ", "-"),
                    price=Decimal(price),
                    description=f"A {name.lower()}",
                    body="",
                    view_count=0,
                    selected_count=0,
                    is_featured=name == "Latte",
                    category_id=cat["id"],
                    version=1,
                    status="A",
                    is_deleted=False,
                    deleted_at=None,
                    **g.audit(t0),
                )
            )
    item_weights = [POPULARITY[i["name"]] for i in items]
    customs = [
        g.add(
            "menu_customization",
            id=g.uid(),
            name=n,
            slug=n.lower().replace(" ", "-"),
            group_id=None,
            description="",
            display_order=k,
            price=Decimal(p),
            status="A",
            is_deleted=False,
            deleted_at=None,
            **g.audit(t0),
        )
        for k, (n, p) in enumerate(CUSTOMIZATIONS)
    ]

    # Devices and ports.
    mtype = g.add(
        "devices_machinetype",
        id=g.uid(),
        name="Navi One",
        slug="navi-one",
        model_number="N1-2025",
        maintenance_frequency=30,
        status="A",
        is_deleted=False,
        deleted_at=None,
        **g.audit(t0),
    )
    pis: list[dict[str, Any]] = []
    ports: list[dict[str, Any]] = []
    for k, (name, city, state, postal, lat, lon) in enumerate(PORTS):
        pi = g.add(
            "devices_raspberrypi",
            id=g.uid(),
            name=f"pi-{k + 1:02d}",
            slug=f"pi-{k + 1:02d}",
            mac_address=f"dc:a6:32:00:00:{k:02x}",
            ip_address=f"10.0.0.{k + 10}",
            location=name,
            is_connected=True,
            firmware_version="1.4.2",
            last_seen=now - dt.timedelta(seconds=rng.randint(5, 50)),
            token_hash=f"{k:064x}",
            is_simulated=False,
            status="A",
            is_deleted=False,
            deleted_at=None,
            **g.audit(t0),
        )
        machine = g.add(
            "devices_espressomachine",
            id=g.uid(),
            name=f"machine-{k + 1:02d}",
            slug=f"machine-{k + 1:02d}",
            serial_number=f"SN-{k + 1:05d}",
            machine_type_id=mtype["id"],
            ip_address=f"10.0.1.{k + 10}",
            port=8080,
            is_online=True,
            last_maintenance_at=now - dt.timedelta(days=rng.randint(1, 30)),
            status="A",
            is_deleted=False,
            deleted_at=None,
            **g.audit(t0),
        )
        ports.append(
            g.add(
                "devices_naviport",
                id=g.uid(),
                name=name,
                slug=name.lower().replace(" ", "-"),
                address_line_1=f"{k + 1} Main St",
                address_line_2="",
                address_line_3="",
                city=city,
                state_or_region=state,
                postal_code=postal,
                country="US",
                espresso_machine_id=machine["id"],
                raspberry_pi_id=pi["id"],
                latitude=Decimal(str(lat)),
                longitude=Decimal(str(lon)),
                accepting_orders=True,
                status_message="",
                status="A",
                is_deleted=False,
                deleted_at=None,
                **g.audit(t0),
            )
        )
        pis.append(pi)

    # Device events: connected + 10-minute heartbeats, with a few outages per Pi.
    for pi in pis:
        t = start
        g.add(
            "devices_deviceevent",
            id=g.uid(),
            raspberry_pi_id=pi["id"],
            type="connected",
            message="",
            payload={},
            actor_id=None,
            created_at=t,
        )
        while t < now:
            if rng.random() < 0.0015:  # ~1 outage per Pi per week
                g.add(
                    "devices_deviceevent",
                    id=g.uid(),
                    raspberry_pi_id=pi["id"],
                    type="disconnected",
                    message="socket closed",
                    payload={},
                    actor_id=None,
                    created_at=t,
                )
                t += dt.timedelta(minutes=rng.randint(15, 240))
                if t >= now:
                    break
                g.add(
                    "devices_deviceevent",
                    id=g.uid(),
                    raspberry_pi_id=pi["id"],
                    type="connected",
                    message="",
                    payload={},
                    actor_id=None,
                    created_at=t,
                )
            t += dt.timedelta(minutes=10, seconds=rng.randint(0, 30))
            if t < now:
                g.add(
                    "devices_deviceevent",
                    id=g.uid(),
                    raspberry_pi_id=pi["id"],
                    type="heartbeat",
                    message="",
                    payload={},
                    actor_id=None,
                    created_at=t,
                )

    # Orders. Repeat customers are favoured (a few regulars, a long tail).
    weights = [1.0 / (k + 1) ** 0.6 for k in range(len(customers))]
    loyalty: dict[int, dict[str, Any]] = {}
    invoice_no = 0
    day = start
    while day < now:
        weekday_factor = 0.6 if day.weekday() >= 5 else 1.0
        for port, pi in zip(ports, pis, strict=True):
            for _ in range(int(rng.randint(8, 30) * weekday_factor)):
                created = day + dt.timedelta(
                    hours=rng.triangular(6, 20, 9), seconds=rng.randint(0, 3599)
                )
                if created >= now:
                    continue
                user = rng.choices(customers, weights)[0]
                is_test = rng.random() < 0.01
                order_id = g.uid()
                subtotal = Decimal("0")
                lines = []
                for _line in range(rng.choices([1, 2, 3], [80, 17, 3])[0]):
                    item = rng.choices(items, item_weights)[0]
                    qty = rng.choices([1, 2], [92, 8])[0]
                    price = Decimal("0.00") if is_test else item["price"]
                    item_id = g.uid()
                    g.add(
                        "orders_orderitem",
                        id=item_id,
                        slug=f"oi-{item_id.hex[:12]}",
                        menu_item_id=item["id"],
                        order_id=order_id,
                        quantity=qty,
                        unit_price=price,
                        status="A",
                        is_deleted=False,
                        deleted_at=None,
                        **g.audit(created, user["id"]),
                    )
                    subtotal += price * qty
                    for c in rng.sample(customs, rng.choices([0, 1, 2], [55, 35, 10])[0]):
                        cprice = Decimal("0.00") if is_test else c["price"]
                        cid = g.uid()
                        g.add(
                            "orders_ordercustomization",
                            id=cid,
                            slug=f"oc-{cid.hex[:12]}",
                            order_item_id=item_id,
                            customization_id=c["id"],
                            quantity=1,
                            unit_price=cprice,
                            status="A",
                            is_deleted=False,
                            deleted_at=None,
                            **g.audit(created, user["id"]),
                        )
                        subtotal += cprice
                    lines.append(item)

                age = now - created
                roll = rng.random()
                if age < dt.timedelta(minutes=20):
                    status, pay_status = ("O", "ready") if roll < 0.5 else ("S", "ready")
                elif roll < 0.04:
                    status, pay_status = "C", "canceled"
                elif roll < 0.07:
                    status, pay_status = "C", "refunded"  # machine failure -> refund
                else:
                    status, pay_status = "D", "succeeded"
                if is_test:
                    pay_status = "ready" if status in ("O", "S") else "canceled"

                tax = q(subtotal * TAX_RATE)
                total = subtotal + tax
                charged = pay_status in ("succeeded", "refunded")
                updated = created + dt.timedelta(minutes=rng.randint(1, 6))
                payment = g.add(
                    "payments_payment",
                    id=g.uid(),
                    stripe_setup_intent_id=f"seti_{order_id.hex[:16]}",
                    stripe_payment_method_id="pm_card_visa",
                    stripe_payment_intent_id=f"pi_{order_id.hex[:24]}" if charged else None,
                    amount_received=total if charged else Decimal("0"),
                    subtotal=subtotal,
                    tax_amount=tax,
                    total_amount=total,
                    currency="usd",
                    status=pay_status,
                    stripe_tax_calculation_id="",
                    stripe_tax_transaction_id="",
                    stripe_refund_id=f"re_{order_id.hex[:24]}" if pay_status == "refunded" else "",
                    amount_refunded=total if pay_status == "refunded" else Decimal("0"),
                    is_deleted=False,
                    deleted_at=None,
                    **g.audit(created, user["id"], updated_at=updated),
                )
                g.add(
                    "orders_order",
                    id=order_id,
                    slug=f"o-{order_id.hex[:12]}",
                    user_id=user["id"],
                    navi_port_id=port["id"],
                    payment_id=payment["id"],
                    cart_token=uuid.UUID(int=rng.getrandbits(128)).hex,
                    order_status=status,
                    claimed_by_id=pi["id"] if status != "O" else None,
                    claimed_at=created + dt.timedelta(minutes=1) if status != "O" else None,
                    is_test=is_test,
                    qr_nonce=f"{rng.getrandbits(64):016x}",
                    cancel_reason="Brew failed, refunded" if pay_status == "refunded" else "",
                    status="A",
                    is_deleted=False,
                    deleted_at=None,
                    **g.audit(created, user["id"], updated_at=updated),
                )
                if status in ("D", "C") and not is_test:
                    brew_at = created + dt.timedelta(minutes=1)
                    g.add(
                        "devices_deviceevent",
                        id=g.uid(),
                        raspberry_pi_id=pi["id"],
                        type="brew_started",
                        message="",
                        payload={"order_id": str(order_id)},
                        actor_id=None,
                        created_at=brew_at,
                    )
                    failed = pay_status == "refunded"
                    g.add(
                        "devices_deviceevent",
                        id=g.uid(),
                        raspberry_pi_id=pi["id"],
                        type="brew_failed" if failed else "brew_completed",
                        message="",
                        payload={"order_id": str(order_id)},
                        actor_id=None,
                        created_at=brew_at + dt.timedelta(seconds=rng.randint(40, 120)),
                    )
                    if failed:
                        g.add(
                            "orders_machineerrorlog",
                            id=g.uid(),
                            order_id=order_id,
                            raspberry_pi_id=pi["id"],
                            error_message=rng.choice(ERRORS),
                            is_recoverable=rng.random() < 0.5,
                            status="A",
                            is_deleted=False,
                            deleted_at=None,
                            **g.audit(brew_at, staff=None),
                        )
                if pay_status == "succeeded":
                    invoice_no += 1
                    g.add(
                        "payments_invoice",
                        id=g.uid(),
                        order_id=order_id,
                        reference_number=invoice_no,
                        pdf=None,
                        status="A",
                        is_deleted=False,
                        deleted_at=None,
                        **g.audit(updated, user["id"]),
                    )
                    state = loyalty.setdefault(
                        user["id"], {"orders": 0, "spent": Decimal(0), "first": created}
                    )
                    state["orders"] += 1
                    state["spent"] += total
                    state["last"] = updated
                    user["last_login"] = max(user["last_login"] or created, created)
                    if user["date_joined"] > created:
                        user["date_joined"] = created - dt.timedelta(minutes=5)
        day += dt.timedelta(days=1)

    # Loyalty and awards.
    award = g.add(
        "awards_award",
        id=g.uid(),
        name="Regular",
        slug="regular",
        description="Complete orders",
        icon="",
        rule_type="orders_completed",
        threshold=None,
        points_reward=0,
        category="habits",
        status="A",
        is_deleted=False,
        deleted_at=None,
        **g.audit(t0),
    )
    levels = [
        g.add(
            "awards_awardlevel",
            id=g.uid(),
            award_id=award["id"],
            rank=r,
            name=n,
            threshold=th,
            points_reward=pts,
            icon="",
        )
        for r, (n, th, pts) in enumerate(
            [("Bronze", 5, 10), ("Silver", 15, 25), ("Gold", 40, 50)], 1
        )
    ]
    for user_id, state in loyalty.items():
        points = int(state["spent"])
        g.add(
            "awards_userloyalty",
            id=g.uid(),
            user_id=user_id,
            lifetime_points=points,
            balance_points=points // 2,
            orders_completed=state["orders"],
            total_spent=state["spent"],
            current_tier_id=None,
            notifications_enabled=True,
            created_at=state["first"],
            updated_at=state["last"],
        )
        reached = [lv for lv in levels if state["orders"] >= lv["threshold"]]
        if reached:
            ua = g.add(
                "awards_useraward",
                id=g.uid(),
                user_id=user_id,
                award_id=award["id"],
                level_id=reached[-1]["id"],
                earned_at=state["last"],
            )
            for lv in reached:
                g.add(
                    "awards_userawardlevel",
                    id=g.uid(),
                    user_award_id=ua["id"],
                    level_id=lv["id"],
                    earned_at=state["last"],
                )
    return g.rows


INSERT_ORDER = [
    "users_user",
    "menu_category",
    "menu_menuitem",
    "menu_customization",
    "devices_raspberrypi",
    "devices_machinetype",
    "devices_espressomachine",
    "devices_naviport",
    "devices_deviceevent",
    "payments_payment",
    "orders_order",
    "orders_orderitem",
    "orders_ordercustomization",
    "orders_machineerrorlog",
    "payments_invoice",
    "awards_award",
    "awards_awardlevel",
    "awards_userloyalty",
    "awards_useraward",
    "awards_userawardlevel",
]


def load(conn: psycopg.Connection[Any], rows: dict[str, list[dict[str, Any]]]) -> dict[str, int]:
    from psycopg.types.json import Jsonb

    ddl = resources.files("crumb.seed").joinpath("navi_schema.sql").read_text()
    counts: dict[str, int] = {}
    with conn.transaction():
        conn.execute(trusted_sql(ddl))
        for table in INSERT_ORDER:
            data = rows.get(table, [])
            if not data:
                continue
            cols = list(data[0])
            stmt = trusted_sql("copy {} ({}) from stdin").format(
                sql.Identifier(table), sql.SQL(", ").join(sql.Identifier(c) for c in cols)
            )
            with conn.cursor().copy(stmt) as copy:
                for r in data:
                    copy.write_row([Jsonb(r[c]) if isinstance(r[c], dict) else r[c] for c in cols])
            counts[table] = len(data)
        conn.execute("select setval('users_user_id_seq', (select max(id) from users_user))")
    return counts
