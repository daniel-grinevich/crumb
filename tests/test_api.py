"""crumb API against a real (test) warehouse."""

from collections.abc import Iterator
from typing import Any

import psycopg
import pytest
from fastapi.testclient import TestClient
from temporalio.testing import ActivityEnvironment

from crumb import warehouse as wh
from crumb.api.app import create_app
from crumb.temporal import activities as act
from crumb.temporal.types import StepInput
from tests.conftest import TEST_API_KEY
from tests.test_pipeline_integration import extract_all

pytestmark = [pytest.mark.integration, pytest.mark.usefixtures("settings_env")]

AUTH = {"X-API-Key": TEST_API_KEY}


@pytest.fixture
def client(settings_env: None) -> Iterator[TestClient]:
    with TestClient(create_app()) as c:
        yield c


@pytest.fixture
def built(settings_env: None) -> None:
    extract_all()
    ActivityEnvironment().run(act.run_transform, StepInput("api-test"))


def test_requires_api_key(client: TestClient) -> None:
    assert client.get("/marts").status_code == 401
    assert client.get("/marts", headers={"X-API-Key": "wrong"}).status_code == 401
    assert client.get("/marts", headers=AUTH).status_code == 200


def test_health_is_public_and_reports_dependencies(client: TestClient) -> None:
    body = client.get("/health").json()
    assert body["warehouse"] is True
    assert body["status"] in ("ok", "degraded")


def test_openapi_docs(client: TestClient) -> None:
    assert client.get("/docs").status_code == 200
    paths = client.get("/openapi.json").json()["paths"]
    assert {
        "/health",
        "/freshness",
        "/runs",
        "/runs/{run_id}",
        "/pipelines/{name}/run",
        "/marts/{name}",
    } <= set(paths)


def test_marts_whitelist_and_unbuilt(client: TestClient) -> None:
    assert client.get("/marts/pg_user", headers=AUTH).status_code == 404
    assert client.get("/marts/fct_orders;drop", headers=AUTH).status_code == 404
    assert client.get("/marts/daily_port_sales", headers=AUTH).status_code == 503


def test_mart_paging_and_filters(built: None, client: TestClient) -> None:
    resp = client.get("/marts/daily_port_sales", params={"limit": 5}, headers=AUTH)
    assert resp.status_code == 200, resp.text
    page = resp.json()
    assert page["total"] > 5 and len(page["rows"]) == 5
    row = page["rows"][0]
    assert {"date", "port_id", "orders", "revenue", "refunds", "avg_ticket"} <= set(row)

    port, day = row["port_id"], row["date"]
    filtered = client.get(
        "/marts/daily_port_sales",
        params={"port_id": port, "start_date": day, "end_date": day},
        headers=AUTH,
    ).json()
    assert filtered["total"] == 1 and filtered["rows"][0]["port_id"] == port

    nxt = client.get(
        "/marts/daily_port_sales", params={"limit": 5, "offset": 5}, headers=AUTH
    ).json()
    assert nxt["rows"][0] != page["rows"][0]
    listing = {m["name"]: m for m in client.get("/marts", headers=AUTH).json()}
    assert listing["daily_port_sales"]["built"] and listing["daily_port_sales"]["columns"]


def test_freshness_and_runs(
    built: None, client: TestClient, wh_conn: psycopg.Connection[dict[str, Any]]
) -> None:
    fresh = client.get("/freshness", headers=AUTH).json()
    assert {f["table_name"] for f in fresh} >= {"orders_order", "payments_payment"}
    assert all(not f["is_stale"] for f in fresh)

    wh.record_run_started(
        wh_conn,
        run_id="r-parent",
        workflow_id="wf-1",
        workflow_type="PipelineWorkflow",
        parent_run_id=None,
        params={"full_refresh": False},
    )
    wh.record_run_started(
        wh_conn,
        run_id="r-child",
        workflow_id="wf-1-navi",
        workflow_type="ExtractNaviWorkflow",
        parent_run_id="r-parent",
        params={},
    )
    wh.record_run_finished(wh_conn, run_id="r-child", status="failed", stats={}, error="boom")

    runs = client.get("/runs", headers=AUTH).json()
    assert runs["total"] == 2
    top = client.get("/runs", params={"top_level_only": True}, headers=AUTH).json()
    assert [r["run_id"] for r in top["runs"]] == ["r-parent"]
    failed = client.get("/runs", params={"status": "failed"}, headers=AUTH).json()
    assert failed["runs"][0]["error"] == "boom"

    detail = client.get("/runs/r-parent", headers=AUTH).json()
    assert detail["children"][0]["run_id"] == "r-child"
    assert "/workflows/wf-1/r-parent" in detail["temporal_ui_url"]
    assert client.get("/runs/nope", headers=AUTH).status_code == 404


def test_unknown_pipeline_404(client: TestClient) -> None:
    assert client.post("/pipelines/nope/run", headers=AUTH).status_code == 404
    names = {p["name"] for p in client.get("/pipelines", headers=AUTH).json()}
    assert {"full", "incremental", "transform", "navi", "stripe"} <= names
