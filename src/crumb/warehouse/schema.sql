-- Idempotent warehouse bootstrap. Runs on worker/API start and `crumb migrate`.
create schema if not exists raw;
create schema if not exists staging;
create schema if not exists marts;
create schema if not exists meta;

-- Incremental extraction state, one row per source table.
create table if not exists meta.watermarks (
    source          text not null,
    table_name      text not null,
    watermark       timestamptz,
    last_success_at timestamptz,
    last_run_id     text,
    rows_last_run   bigint not null default 0,
    updated_at      timestamptz not null default now(),
    primary key (source, table_name)
);

-- 'ok', or why the table wasn't extracted (e.g. 'skipped: table missing').
alter table meta.watermarks add column if not exists status text not null default 'ok';

-- Every workflow run (scheduled or manual), written by the workflows themselves.
create table if not exists meta.runs (
    run_id        text primary key,
    workflow_id   text not null,
    workflow_type text not null,
    parent_run_id text,
    status        text not null,
    params        jsonb not null default '{}'::jsonb,
    stats         jsonb not null default '{}'::jsonb,
    error         text,
    started_at    timestamptz not null default now(),
    finished_at   timestamptz
);
-- Constraints are (re)declared separately so existing warehouses pick up new values.
alter table meta.runs drop constraint if exists runs_status_check;
alter table meta.runs add constraint runs_status_check check (
    status in ('running', 'succeeded', 'failed', 'skipped', 'cancelled', 'terminated', 'timed_out')
);
create index if not exists runs_started_at_idx on meta.runs (started_at desc);
create index if not exists runs_workflow_id_idx on meta.runs (workflow_id);

-- Data-quality results, one row per check per run.
create table if not exists meta.dq_results (
    id         bigserial primary key,
    run_id     text,
    check_name text not null,
    status     text not null,
    observed   jsonb not null default '{}'::jsonb,
    message    text not null default '',
    checked_at timestamptz not null default now()
);
-- 'warned': a check that found problems but is configured not to fail the run.
alter table meta.dq_results drop constraint if exists dq_results_status_check;
alter table meta.dq_results add constraint dq_results_status_check check (
    status in ('passed', 'failed', 'skipped', 'warned')
);
create index if not exists dq_results_checked_at_idx on meta.dq_results (checked_at desc);
