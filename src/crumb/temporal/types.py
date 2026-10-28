"""Workflow/activity payloads. Plain dataclasses with no heavy imports so they
are safe inside the workflow sandbox."""

from dataclasses import dataclass
from dataclasses import field
from typing import Any


@dataclass
class ExtractParams:
    full_refresh: bool = False
    # Subset of tables/objects to extract; empty means all.
    tables: list[str] = field(default_factory=list)


@dataclass
class TransformParams:
    run_checks: bool = True


@dataclass
class PipelineParams:
    full_refresh: bool = False
    run_checks: bool = True
    sources: list[str] = field(default_factory=lambda: ["navi", "stripe"])


@dataclass
class ExtractTableInput:
    run_id: str
    table: str
    full_refresh: bool = False


@dataclass
class RunStarted:
    run_id: str
    workflow_id: str
    workflow_type: str
    parent_run_id: str | None
    params: dict[str, Any]


@dataclass
class RunFinished:
    run_id: str
    status: str
    stats: dict[str, Any]
    error: str | None = None


@dataclass
class StepInput:
    run_id: str
