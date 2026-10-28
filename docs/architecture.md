# crumb architecture

```
 Navi Postgres (read replica) ──┐                       ┌── staging.* ──► marts.* ──► FastAPI (:8020)
   orders, payments, devices,   ├─► raw.* (JSONB) ──────┤   (typed)       (facts,      /marts/{name}
   DeviceEvent, menu, awards,   │   + meta.watermarks   │                  daily
   users (pseudonymised)        │                       └── data-quality checks ──► meta.dq_results
 Stripe API ────────────────────┘
   PaymentIntents, Charges, Refunds          all orchestrated by Temporal; every run in meta.runs
```

## Warehouse layout (Postgres)

| Schema | Contents |
| --- | --- |
| `raw` | One table per source table: `id text pk, data jsonb, source_ts, _extracted_at, _run_id`. Upserted, never truncated. |
| `staging` | `stg_*` models: one per source entity, typed columns cast out of `raw` JSONB. |
| `marts` | Analytics tables served by the API (below). |
| `meta` | `watermarks` (incremental state + last success per table), `runs` (every workflow run), `dq_results`. |

Landing as JSONB keeps extraction generic and tolerant of Navi schema drift (a new column is just a
new key); typing lives in one place, staging.

### Marts (v1)

| Mart | Grain |
| --- | --- |
| `fct_orders` | order: status, port, pseudonymised user, items, subtotal/tax/total, charged?, refunded, net |
| `fct_order_items` | order line incl. customizations |
| `daily_port_sales` | day × NaviPort: orders, revenue (charged totals incl. tax), refunds, avg ticket. Excludes staff test orders. |
| `machine_uptime_daily` | day × Raspberry Pi: up seconds / uptime ratio, connects, disconnects, brews, errors |
| `drink_popularity` | day × port × menu item: quantity, orders, revenue, rank |
| `refunds_daily` | day: Navi vs Stripe refunds, reconciliation issue counts, `is_reconciled` |
| `reconciliation_issues` | one row per Stripe↔Navi problem: `charge_without_order`, `double_charge`, `missing_stripe_charge`, `refund_mismatch` |
| `customer_cohorts` | first-order month × months since: active customers, retention, repeat rate |

Days are bucketed in `CRUMB_REPORTING_TZ` (default UTC). Uptime: any event except `disconnected`
means the Pi was alive until the next event, capped at 15 minutes (Navi samples heartbeats every
10 minutes); `disconnected` starts downtime.

## Transform layer: versioned SQL files

`src/crumb/transform/sql/{staging,marts}/<model>.sql`, one `SELECT` per file. The runner
materialises each as `create table <layer>.<model> as …`, discovers dependencies from
`staging.x` / `marts.x` references, runs them in topological order, and does it all in one
transaction (readers see the old build or the new one). A `-- unique_key:` header drives the
uniqueness checks.

**Why not dbt (yet)?** Eight marts over ~17 staging models fit comfortably in plain SQL; this avoids
a second toolchain, a profile/target config, and a subprocess inside a Temporal activity, and the
models stay testable from pytest against real Postgres. The files are already dbt-shaped (one select
per model, refs by `schema.name`), and `Transformer` in `crumb/transform/__init__.py` is the seam: a
`DbtTransformer` implementing `models()`/`run()` (shelling out to `dbt build`) can replace
`SqlFileTransformer` without touching workflows or the API. Switch once models need incremental
materialisation, docs/lineage UI, or many contributors.

v1 rebuilds staging and marts fully on every transform (seconds at current volumes).

## Workflows (Temporal, task queue `crumb`)

| Workflow | Does |
| --- | --- |
| `ExtractNaviWorkflow` | one `extract_navi_table` activity per table, in parallel |
| `ExtractStripeWorkflow` | `stripe_configured`, then one `extract_stripe_object` per object; **skipped** (not failed) without a key |
| `TransformWorkflow` | `run_transform`, then `run_quality_checks` if `run_checks` |
| `DataQualityWorkflow` | checks only |
| `PipelineWorkflow` | both extracts as parallel child workflows, then `TransformWorkflow` |

Reliability:
- **Retries with backoff**: extracts 5 attempts, 5s → 5 min (×2); transform/checks 3 attempts.
  `ConfigError` and `DataQualityError` are non-retryable.
- **Heartbeats**: extract activities heartbeat after every committed page with their keyset cursor
  (Navi: `(watermark, id)`; Stripe: `starting_after`). A retried attempt resumes from the last
  heartbeat instead of starting over. Heartbeat timeout 2 min.
- **Idempotency**: raw upserts by source primary key (unchanged rows aren't rewritten); the
  watermark only moves forward and only after a table is fully read; transforms are full rebuilds
  in one transaction. Rerunning any window is safe.
- **Run log**: every workflow writes `meta.runs` (running → succeeded / failed / skipped, stats,
  flattened error chain, parent run id). Exposed at `GET /runs`, `GET /runs/{run_id}`.

### Schedules (created/updated by the worker on start)

| Schedule | Spec | Runs |
| --- | --- | --- |
| `crumb-hourly-pipeline` | every hour | `PipelineWorkflow(run_checks=false)` (incremental) |
| `crumb-nightly-transform` | `30 3 * * *` UTC | `TransformWorkflow(run_checks=true)`: full rebuild + all DQ checks |

Overlap policy `SKIP`: a slow run never queues another behind it.

## Incremental extraction

- **Navi**: keyset pages ordered by `(watermark column, id)` starting from
  `stored watermark − CRUMB_NAVI_LOOKBACK_SECONDS` (default 5 min) so rows committed late are
  still caught. Watermark column is `updated_at` for most tables, `created_at` for `DeviceEvent`,
  `earned_at` for awards, `coalesce(last_login, date_joined)` for users. `awards_awardlevel` has no
  timestamp and is re-read in full (tiny). Selects only allowlisted columns and skips any missing
  from the source, so older Navi schemas still work. Connections are read-only.
- **Older Navi versions**: allowlisted columns missing from the source are skipped (logged, and
  listed as `missing_columns` in the run stats). Whole tables are **required** or **optional**:

  | Required (missing → extract fails, non-retryable `MissingTableError`) | Optional (missing → skipped) |
  | --- | --- |
  | `orders_order`, `orders_orderitem`, `payments_payment`, `devices_naviport` | everything else: users, menu, devices/Pis/machines, `devices_deviceevent`, `orders_machineerrorlog`, `orders_ordercustomization`, invoices, `awards_*` |

  A skipped table gets `status = 'skipped: table missing'` in `meta.watermarks` (watermark and last
  success are kept, so it resumes incrementally if it reappears), shows up in `GET /freshness`
  (`status`, `required`, never `is_stale`), is listed under `skipped_tables` in the
  `ExtractNaviWorkflow` stats in `meta.runs`, and its freshness check is `skipped`. Raw tables
  always exist, so dependent models still build (empty if the table never existed); the transform
  run's stats list them under `degraded` with the missing source tables (e.g. no
  `devices_deviceevent` → `marts.machine_uptime_daily` is empty).
- **Stripe**: `created >= watermark − 3 days` (objects change after creation, e.g. charges get
  refunded), newest first, 100 per page.
- `full-refresh` pipeline ignores watermarks.

Known v1 limits: hard deletes in Navi are not propagated (Navi soft-deletes; `is_deleted` is
carried through and marts exclude it). Stripe objects modified more than 3 days after creation are
only refreshed by `full-refresh`.

## Data-quality checks

Run after the transform (always nightly; `full`, `full-refresh`, `transform`, `quality` pipelines).
Results land in `meta.dq_results`; any failure fails the workflow with every failure in the message.

- `unique:<model>`: declared `unique_key` has no duplicates or nulls (all keyed models)
- `rowcount:<staging model>`: single-source staging models keep every raw row
- `not_empty:<mart>`: core marts are populated when their inputs are
- `freshness:<source>.<table>`: last successful extract within `CRUMB_FRESHNESS_MAX_AGE_HOURS` (3h);
  `skipped` for optional tables missing from the source
- `reconciliation`: `marts.reconciliation_issues` is empty (skipped until Stripe data exists)

## PII policy

Nothing that identifies a person lands in the warehouse.

1. **Allowlist, not blocklist.** Each source table lists the columns crumb selects
   (`crumb/sources/navi.py`, `crumb/sources/stripe_source.py`). Never selected: emails, names,
   phones, passwords, first/last names, cart tokens, QR nonces, device tokens, MAC/IP addresses,
   user agents, street address lines, free-text fields (`cancel_reason`, `status_message`,
   DeviceEvent `message`/`payload`, Stripe `metadata` other than `order_id`, billing details,
   receipt emails, card details).
2. **Pseudonymisation.** User identifiers (`users_user.id`, every `user_id`, `created_by_id`,
   `updated_by_id`, `actor_id`) and Stripe customer ids are replaced before landing with
   `HMAC-SHA256(PII_SALT, "<namespace>:<value>")[:32]`. Same input → same key, so joins and cohorts
   work; without the salt the warehouse can't be joined back to Navi users. The salt comes from the
   environment, is never stored in the warehouse, and must stay stable (changing it re-keys every user).
3. Machine-generated text (`MachineErrorLog.error_message`, from Pi firmware) is kept.
4. Internal integer user ids can appear transiently in Temporal heartbeat details (keyset cursor);
   no names or contact details ever do.

Tests assert none of the seeded emails, phones or Stripe customer ids reach `raw`.

## API

`X-API-Key` header on everything except `/health`; OpenAPI at `/docs`.

| Endpoint | |
| --- | --- |
| `GET /health` | warehouse + Temporal reachability |
| `GET /freshness` | per source table: `table` (also `table_name`), `status`, `required`, watermark, last success, age, `is_stale` |
| `GET /runs`, `GET /runs/{run_id}` | run log, filters `status`, `workflow_type`, `top_level_only`; detail adds child runs + live Temporal status |
| `GET /pipelines`, `POST /pipelines/{name}/run` | start a named pipeline, returns workflow id, run id, Temporal UI link |
| `GET /marts`, `GET /marts/{name}` | whitelisted marts only; `start_date`, `end_date`, `port_id`, `limit` (≤1000), `offset` |

Mart names, columns and ordering come from a whitelist; user input only reaches SQL as bound
parameters. Money is returned as decimal strings (exact).

## Adding a source

1. Write `crumb/sources/<name>.py` with an allowlisted spec per table/object, a `to_record()` that
   applies the PII rules, and an `extract_*()` that pages, upserts with `warehouse.upsert_raw`,
   heartbeats a cursor and calls `warehouse.set_watermark` at the end.
2. Add its raw tables to `all_raw_tables()` in `crumb/temporal/activities.py`, an activity that
   wraps the extract, and an `Extract<Name>Workflow` in `crumb/temporal/workflows.py`; register
   both in the `ALL` lists and add it to `PipelineWorkflow` behind `params.sources`.
3. Add `stg_*.sql` models (and marts) under `crumb/transform/sql/`; set `-- unique_key:`.
4. Add a `Pipeline` entry in `crumb/temporal/pipelines.py`, freshness expectations in
   `quality.check_freshness`, and tests (unit for `to_record`, integration for extract + models).
