"""Versioned-SQL transform layer (staging -> marts).

Each model is one file, ``sql/<layer>/<name>.sql``, holding a single SELECT.
The runner materialises it as ``<layer>.<name>`` with ``create table as``.
Dependencies are discovered from ``staging.x`` / ``marts.x`` references in the
SQL and run in topological order, all inside one transaction, so readers see
either the previous build or the new one, never a half-built mix.

Optional header comments:
    -- unique_key: col_a, col_b     (checked by data-quality)

The ``Transformer`` protocol is the seam for swapping in dbt later: a dbt
implementation only needs ``models()`` and ``run()``.
"""

import re
from dataclasses import dataclass
from graphlib import TopologicalSorter
from importlib import resources
from typing import Any
from typing import Protocol

import psycopg
from psycopg import sql

from crumb.db import trusted_sql

LAYERS = ("staging", "marts")
_REF = re.compile(r"\b(staging|marts)\.([a-z_][a-z0-9_]*)\b")
_RAW_REF = re.compile(r"\braw\.([a-z_][a-z0-9_]*)\b")
_HEADER = re.compile(r"^--\s*(\w+)\s*:\s*(.+)$", re.MULTILINE)


@dataclass(frozen=True)
class Model:
    layer: str
    name: str
    sql: str
    depends_on: frozenset[str]
    raw_sources: frozenset[str]
    unique_key: tuple[str, ...]

    @property
    def fqn(self) -> str:
        return f"{self.layer}.{self.name}"


@dataclass
class TransformResult:
    models: list[str]
    rows: dict[str, int]


class Transformer(Protocol):
    def models(self) -> list[Model]: ...

    def run(
        self, conn: psycopg.Connection[dict[str, Any]], *, timezone: str
    ) -> TransformResult: ...


def parse_model(layer: str, name: str, text: str) -> Model:
    headers = {k.lower(): v.strip() for k, v in _HEADER.findall(text)}
    deps = {f"{lyr}.{n}" for lyr, n in _REF.findall(text)} - {f"{layer}.{name}"}
    unique = tuple(c.strip() for c in headers.get("unique_key", "").split(",") if c.strip())
    return Model(
        layer=layer,
        name=name,
        sql=text.strip().rstrip(";"),
        depends_on=frozenset(deps),
        raw_sources=frozenset(_RAW_REF.findall(text)),
        unique_key=unique,
    )


def order_models(models: list[Model]) -> list[Model]:
    by_fqn = {m.fqn: m for m in models}
    graph: dict[str, set[str]] = {}
    for m in models:
        missing = m.depends_on - by_fqn.keys()
        if missing:
            msg = f"{m.fqn} depends on unknown model(s): {', '.join(sorted(missing))}"
            raise ValueError(msg)
        graph[m.fqn] = set(m.depends_on)
    sorter = TopologicalSorter(graph)
    return [by_fqn[f] for f in sorter.static_order()]


def affected_models(models: list[Model], missing_raw: set[str]) -> dict[str, list[str]]:
    """Models that (transitively) read any of ``missing_raw``, with the culprits."""
    by_fqn = {m.fqn: m for m in models}
    cache: dict[str, set[str]] = {}

    def sources(fqn: str) -> set[str]:
        if fqn not in cache:
            m = by_fqn[fqn]
            found = set(m.raw_sources)
            for dep in m.depends_on:
                found |= sources(dep)
            cache[fqn] = found
        return cache[fqn]

    return {
        m.fqn: sorted(sources(m.fqn) & missing_raw) for m in models if sources(m.fqn) & missing_raw
    }


class SqlFileTransformer:
    def __init__(self, package: str = "crumb.transform") -> None:
        self._package = package

    def models(self) -> list[Model]:
        root = resources.files(self._package).joinpath("sql")
        found: list[Model] = []
        for layer in LAYERS:
            for entry in sorted(root.joinpath(layer).iterdir(), key=lambda e: e.name):
                if entry.name.endswith(".sql"):
                    found.append(parse_model(layer, entry.name[:-4], entry.read_text()))
        return order_models(found)

    def run(self, conn: psycopg.Connection[dict[str, Any]], *, timezone: str) -> TransformResult:
        built: list[str] = []
        rows: dict[str, int] = {}
        with conn.transaction():
            # Day bucketing (ts::date, date_trunc) follows the reporting timezone.
            conn.execute(sql.SQL("set local timezone to {}").format(sql.Literal(timezone)))
            for model in self.models():
                ident = sql.Identifier(model.layer, model.name)
                conn.execute(sql.SQL("drop table if exists {} cascade").format(ident))
                create = sql.SQL("create table {} as\n").format(ident) + trusted_sql(model.sql)
                conn.execute(create)
                row = conn.execute(sql.SQL("select count(*) as n from {}").format(ident)).fetchone()
                rows[model.fqn] = int(row["n"]) if row else 0
                built.append(model.fqn)
        return TransformResult(models=built, rows=rows)
