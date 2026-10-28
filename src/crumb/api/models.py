"""Response models for the crumb API."""

import datetime as dt
from typing import Any

from pydantic import BaseModel


class Health(BaseModel):
    status: str
    warehouse: bool
    temporal: bool


class Freshness(BaseModel):
    source: str
    table: str
    # Same value as ``table``; kept for clients written against the first release.
    table_name: str
    # 'ok', or why it isn't extracted, e.g. 'skipped: table missing'.
    status: str
    # Required tables fail the pipeline when missing; optional ones are skipped.
    required: bool
    watermark: dt.datetime | None
    last_success_at: dt.datetime | None
    last_run_id: str | None
    rows_last_run: int
    age_seconds: int | None
    is_stale: bool


class Run(BaseModel):
    run_id: str
    workflow_id: str
    workflow_type: str
    parent_run_id: str | None
    status: str
    params: dict[str, Any]
    stats: dict[str, Any]
    error: str | None
    started_at: dt.datetime
    finished_at: dt.datetime | None
    duration_seconds: float | None
    temporal_ui_url: str
    temporal_status: str | None = None
    children: list["Run"] = []

    @classmethod
    def from_row(cls, row: dict[str, Any], ui_base: str, namespace: str = "default") -> "Run":
        finished = row["finished_at"]
        duration = (finished - row["started_at"]).total_seconds() if finished else None
        url = f"{ui_base}/namespaces/{namespace}/workflows/{row['workflow_id']}/{row['run_id']}/history"
        return cls(**row, duration_seconds=duration, temporal_ui_url=url)


class RunList(BaseModel):
    total: int
    limit: int
    offset: int
    runs: list[Run]


class PipelineInfo(BaseModel):
    name: str
    workflow: str
    description: str


class PipelineStarted(BaseModel):
    pipeline: str
    workflow_id: str
    run_id: str
    temporal_ui_url: str


class MartColumn(BaseModel):
    name: str
    type: str


class MartInfo(BaseModel):
    name: str
    description: str
    filters: list[str]
    built: bool
    columns: list[MartColumn]


class MartPage(BaseModel):
    mart: str
    total: int
    limit: int
    offset: int
    rows: list[dict[str, Any]]
