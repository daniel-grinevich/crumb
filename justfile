set dotenv-load := false

compose := "docker compose"

# List recipes
default:
    @just --list

# Create .env with random local secrets if it doesn't exist
env:
    #!/usr/bin/env sh
    if [ ! -f .env ]; then
        sed -e "s/^CRUMB_API_KEY=.*/CRUMB_API_KEY=$(openssl rand -hex 24)/" \
            -e "s/^CRUMB_PII_SALT=.*/CRUMB_PII_SALT=$(openssl rand -hex 32)/" \
            .env.example > .env
        echo "wrote .env (API key: grep CRUMB_API_KEY .env)"
    fi

# Build and start the whole stack (Temporal, Postgres x2, worker, API)
up: env
    {{compose}} up -d --build --wait
    @echo "API        http://localhost:8020/docs"
    @echo "Temporal   http://localhost:8233"

# Stop the stack (add `-v` via `just down -v` to drop volumes)
down *args:
    {{compose}} down {{args}}

# (Re)create the synthetic navi_source database
seed *args:
    {{compose}} run --rm worker seed {{args}}

# Start a named pipeline and wait (full, incremental, full-refresh, navi, stripe, transform, quality)
run-pipeline name:
    {{compose}} exec worker crumb run {{name}}

# Tail service logs
logs *services:
    {{compose}} logs -f {{services}}

# Run the test suite (integration tests use the compose warehouse on :5440)
test *args:
    uv run pytest {{args}}

# Lint, format check and type check
lint:
    uv run ruff check .
    uv run ruff format --check .
    uv run pyright

# Auto-fix lint and formatting
fmt:
    uv run ruff check --fix .
    uv run ruff format .
