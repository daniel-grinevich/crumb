# crumb

ETL and analytics for the Navi coffee-machine platform: Temporal workflows extract data from Navi,
Stripe and the device fleet into a Postgres warehouse, versioned SQL models it into marts, and a
FastAPI service exposes pipeline runs, freshness and the marts (for the Navi MCP and the bean agent).

See [docs/architecture.md](docs/architecture.md) for the design, PII policy and how to add a source.

## Quickstart

Needs Docker, [`just`](https://github.com/casey/just) and [`uv`](https://docs.astral.sh/uv/).

```sh
just up                    # writes .env with random secrets (first run), builds, starts everything
just seed                  # (re)create the synthetic navi_source DB (~140k rows, 90 days)
just run-pipeline full     # extract -> transform -> data-quality checks, waits for the result
```

Then:

| What | Where |
| --- | --- |
| API docs (OpenAPI) | http://localhost:8020/docs |
| Temporal UI | http://localhost:8233 |
| Warehouse Postgres | `postgresql://crumb:crumb@localhost:5440/crumb` |
| Synthetic Navi source | `postgresql://navi:navi@localhost:5441/navi` (read-only login `crumb_reader`) |
| Temporal frontend | `localhost:7233` |

```sh
KEY=$(grep CRUMB_API_KEY .env | cut -d= -f2)
curl -H "X-API-Key: $KEY" "localhost:8020/marts/daily_port_sales?start_date=2026-10-01&limit=10"
curl -H "X-API-Key: $KEY" -X POST localhost:8020/pipelines/incremental/run
```

Pipelines: `full`, `incremental` (hourly schedule), `full-refresh`, `navi`, `stripe`, `transform`
(nightly schedule), `quality`. Set `CRUMB_STRIPE_API_KEY` (a test key) in `.env` to enable the
Stripe extract; without it the Stripe run is recorded as `skipped`.

## Development

```sh
uv sync
just test      # unit + Temporal time-skipping workflow tests + integration tests (need `just up`)
just lint      # ruff check, ruff format --check, pyright
just logs worker
just down      # `just down -v` also drops the volumes
```

The worker and API also run outside Docker (`uv run crumb worker`, `uv run crumb api`); the
defaults point at the compose ports.

## Image and commands

One image runs every role; the command picks it (entrypoint is `crumb`):

```sh
docker build -t ghcr.io/daniel-grinevich/crumb:<tag> .
docker run IMAGE api       # FastAPI on :8020, liveness/readiness: GET /health
docker run IMAGE worker    # Temporal worker; bootstraps the schema and schedules on start
docker run IMAGE migrate   # create/upgrade warehouse schemas, then exit (k8s Job / init container)
```

Environment (unprefixed names are what the k8s manifests use; `CRUMB_*` works too):

| Variable | Used by | Notes |
| --- | --- | --- |
| `WAREHOUSE_DSN` | all | crumb's Postgres |
| `NAVI_SOURCE_DSN` | worker | Navi read replica, read-only user |
| `TEMPORAL_ADDRESS`, `TEMPORAL_NAMESPACE` | api, worker | default `localhost:7233`, `default` |
| `PII_SALT` | worker | required, >= 16 chars, keep stable |
| `CRUMB_API_KEY` | api | `X-API-Key` header value |
| `STRIPE_API_KEY` | worker | optional |
| `CRUMB_TEMPORAL_UI_URL`, `CRUMB_REPORTING_TZ`, `CRUMB_CREATE_SCHEDULES` | optional | see `src/crumb/config.py` |

## Deploying

CI (`.github/workflows/ci.yml`) runs lint (ruff, pyright), tests (against a Postgres service
container) and an image build without pushing on every PR and push to `main`.

Deploys are tag-driven and build the single image `ghcr.io/daniel-grinevich/crumb` (used by both
`crumb-api` and `crumb-worker`). The tagged commit must already be on `main`, or the workflow fails.

| Tag | Image tags pushed | rainbow-road change |
|-----|-------------------|---------------------|
| `staging` (one moving tag) | `staging-latest`, `staging-<short-sha>` | `staging/crumb/kustomization.yaml`: pod-template annotation `navi.dev/deployed-sha: "<short-sha>"` (ArgoCD auto-syncs) |
| `v1.4.0` (any `vX.Y.Z`) | `1.4.0`, `prod-latest`, `prod-<short-sha>`, the tag name | `production/crumb/kustomization.yaml`: `newTag: 1.4.0` (sync manually in ArgoCD) |

Staging always runs `staging-latest` (`imagePullPolicy: Always`); its `newTag` never changes.
Bumping the `navi.dev/deployed-sha` annotation changes the pod template, so ArgoCD rolls the pods
onto the freshly pushed image. `staging-<short-sha>` is pushed for traceability and rollback.
Production is pinned to the version tag.

```sh
git checkout main && git pull
git tag -f staging && git push -f origin staging   # staging: move the tag to this commit
git tag v1.4.0 && git push origin v1.4.0           # production
```

The manifest bump needs the `RAINBOW_ROAD_PAT` repo secret (write access to
`daniel-grinevich/rainbow-road`).
