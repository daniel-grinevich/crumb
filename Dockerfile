# One image for every crumb process; the command picks the role:
#   docker run IMAGE api       FastAPI on :8020 (GET /health)
#   docker run IMAGE worker    Temporal worker (bootstraps schema + schedules on start)
#   docker run IMAGE migrate   create/upgrade warehouse schemas, then exit (k8s Job)
FROM python:3.13-slim AS build

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never \
    UV_PROJECT_ENVIRONMENT=/opt/venv

# uv from PyPI rather than ghcr.io/astral-sh/uv (ghcr pulls are blocked in some environments).
RUN pip install --no-cache-dir uv==0.13.1

WORKDIR /app
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project
COPY README.md ./
COPY src ./src
RUN uv sync --frozen --no-dev --no-editable


FROM python:3.13-slim

LABEL org.opencontainers.image.source="https://github.com/daniel-grinevich/crumb" \
      org.opencontainers.image.description="crumb: Navi ETL (Temporal workers + FastAPI)"

ENV PYTHONUNBUFFERED=1 \
    PATH="/opt/venv/bin:$PATH"

RUN useradd --create-home --uid 10001 crumb
COPY --from=build /opt/venv /opt/venv

USER 10001
EXPOSE 8020
ENTRYPOINT ["crumb"]
CMD ["api"]
