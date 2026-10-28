"""``crumb`` command line: worker, api, migrate, seed, run <pipeline>."""

import argparse
import asyncio
import json
import logging
import sys

from crumb.config import get_settings


def _migrate() -> None:
    from crumb import warehouse as wh
    from crumb.db import connect
    from crumb.temporal.activities import all_raw_tables

    with connect(get_settings().warehouse_dsn.get_secret_value(), autocommit=True) as conn:
        wh.bootstrap(conn, all_raw_tables())
    print("warehouse schemas ready")


def _seed(days: int, users: int) -> None:
    from crumb.db import connect
    from crumb.seed.generate import generate
    from crumb.seed.generate import load

    rows = generate(days=days, users=users)
    with connect(get_settings().navi_seed_dsn.get_secret_value(), autocommit=True) as conn:
        counts = load(conn, rows)
    for table, n in counts.items():
        print(f"  {table:<28} {n:>7}")
    print(f"seeded navi_source with {sum(counts.values())} rows")


async def _run(name: str, wait: bool) -> int:
    from temporalio.client import WorkflowFailureError

    from crumb.temporal.pipelines import PIPELINES
    from crumb.temporal.pipelines import connect_client
    from crumb.temporal.pipelines import start_pipeline

    if name not in PIPELINES:
        print(f"unknown pipeline {name!r}; choose from: {', '.join(PIPELINES)}", file=sys.stderr)
        return 2
    settings = get_settings()
    client = await connect_client(settings)
    handle = await start_pipeline(client, settings, name)
    ui = (
        f"{settings.temporal_ui_url}/namespaces/{settings.temporal_namespace}/workflows/{handle.id}"
    )
    print(f"started {name}: workflow_id={handle.id} run_id={handle.result_run_id}\n  {ui}")
    if not wait:
        return 0
    try:
        result = await handle.result()
    except WorkflowFailureError as err:
        from crumb.temporal.workflows import describe_error

        print(f"FAILED: {describe_error(err)}", file=sys.stderr)
        return 1
    print(json.dumps(_brief(result), indent=2, default=str))
    return 0


def _brief(result: object) -> object:
    """Drop per-table detail so CLI output stays readable."""
    if isinstance(result, dict):
        return {
            k: _brief(v)
            for k, v in result.items()
            if k not in ("tables", "rows") or not isinstance(v, list | dict)
        }
    if isinstance(result, list):
        return [_brief(v) for v in result]
    return result


def main() -> None:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    parser = argparse.ArgumentParser(prog="crumb")
    sub = parser.add_subparsers(dest="cmd", required=True)
    sub.add_parser("worker", help="run the Temporal worker (also creates schedules)")
    sub.add_parser("api", help="run the FastAPI service on :8020")
    sub.add_parser("migrate", help="create warehouse schemas and meta tables")
    seed = sub.add_parser("seed", help="(re)create the synthetic navi_source database")
    seed.add_argument("--days", type=int, default=90)
    seed.add_argument("--users", type=int, default=400)
    run = sub.add_parser("run", help="start a named pipeline")
    run.add_argument("name")
    run.add_argument("--no-wait", action="store_true")
    args = parser.parse_args()

    if args.cmd == "worker":
        from crumb.temporal.worker import main as worker_main

        worker_main()
    elif args.cmd == "api":
        from crumb.api.app import main as api_main

        api_main()
    elif args.cmd == "migrate":
        _migrate()
    elif args.cmd == "seed":
        _seed(args.days, args.users)
    elif args.cmd == "run":
        sys.exit(asyncio.run(_run(args.name, wait=not args.no_wait)))
