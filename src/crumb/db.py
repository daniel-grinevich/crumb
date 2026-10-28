"""Small Postgres helpers shared by activities, the API and the seeder."""

import datetime as dt
import decimal
import json
import uuid
from typing import Any
from typing import LiteralString
from typing import cast

import psycopg
from psycopg import sql
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb


def connect(
    dsn: str, *, read_only: bool = False, autocommit: bool = False
) -> psycopg.Connection[dict[str, Any]]:
    """Open a connection with dict rows.

    Warehouse writers use ``autocommit=True`` so each ``conn.transaction()``
    block is a real, independently committed transaction (not a savepoint).
    """
    # psycopg's overloads don't type dict_row for pyright; the runtime type is right.
    conn = cast(
        psycopg.Connection[dict[str, Any]],
        psycopg.connect(
            dsn,
            row_factory=dict_row,  # pyright: ignore[reportArgumentType]
            autocommit=autocommit,
        ),
    )
    if read_only:
        conn.read_only = True
    return conn


def trusted_sql(text: str) -> sql.SQL:
    """SQL from files shipped in this package (DDL, transform models), never user input."""
    return sql.SQL(cast(LiteralString, text))


def _default(value: object) -> object:
    # Decimals go in as strings so money stays exact; staging casts to numeric.
    if isinstance(value, decimal.Decimal):
        return str(value)
    if isinstance(value, dt.datetime | dt.date):
        return value.isoformat()
    if isinstance(value, uuid.UUID):
        return str(value)
    msg = f"not JSON serialisable: {type(value).__name__}"
    raise TypeError(msg)


def dumps(value: object) -> str:
    return json.dumps(value, default=_default, sort_keys=True)


def jsonb(value: object) -> Jsonb:
    return Jsonb(value, dumps=dumps)
