-- unique_key: date, pi_id
-- Uptime from the DeviceEvent log. Any event other than 'disconnected' means the Pi
-- was alive; it stays "up" until the next event, capped at 15 minutes (Navi samples
-- heartbeats every 10 minutes while a Pi checks in). 'disconnected' starts downtime.
with events as (
    select
        pi_id,
        event_type,
        created_at,
        lead(created_at) over (partition by pi_id order by created_at, event_id) as next_at
    from staging.stg_device_events
),
up_intervals as (
    select
        pi_id,
        created_at                                      as start_at,
        least(coalesce(next_at, now()), created_at + interval '15 minutes', now()) as end_at
    from events
    where event_type <> 'disconnected'
),
split as (
    select
        u.pi_id,
        d::date                                         as date,
        greatest(u.start_at, d)                         as start_at,
        least(u.end_at, d + interval '1 day')           as end_at
    from up_intervals u
    cross join lateral generate_series(
        date_trunc('day', u.start_at),
        date_trunc('day', u.end_at - interval '1 microsecond'),
        interval '1 day'
    ) d
    where u.end_at > u.start_at
),
calendar as (
    select pi_id, d::date as date, d as day_start
    from (
        select pi_id, min(created_at) as first_at
        from staging.stg_device_events
        group by pi_id
    ) f
    cross join lateral generate_series(
        date_trunc('day', f.first_at), date_trunc('day', now()), interval '1 day'
    ) d
),
uptime as (
    select pi_id, date, sum(extract(epoch from end_at - start_at)) as up_seconds
    from split
    group by pi_id, date
),
counts as (
    select
        pi_id,
        created_at::date                                as date,
        count(*) filter (where event_type = 'connected')      as connects,
        count(*) filter (where event_type = 'disconnected')   as disconnects,
        count(*) filter (where event_type = 'heartbeat')      as heartbeats,
        count(*) filter (where event_type = 'brew_completed') as brews_completed,
        count(*) filter (where event_type = 'brew_failed')    as brews_failed
    from staging.stg_device_events
    group by pi_id, created_at::date
),
errors as (
    select pi_id, created_at::date as date, count(*) as machine_errors
    from staging.stg_machine_error_logs
    where pi_id is not null
    group by pi_id, created_at::date
)
select
    c.date,
    c.pi_id,
    pi.pi_name,
    port.port_id,
    port.port_name,
    round(coalesce(u.up_seconds, 0))::integer          as up_seconds,
    round(extract(epoch from least(c.day_start + interval '1 day', now()) - c.day_start))::integer
                                                       as observed_seconds,
    round(
        coalesce(u.up_seconds, 0)
        / nullif(extract(epoch from least(c.day_start + interval '1 day', now()) - c.day_start), 0),
        4
    )                                                  as uptime_ratio,
    coalesce(n.connects, 0)                            as connects,
    coalesce(n.disconnects, 0)                         as disconnects,
    coalesce(n.heartbeats, 0)                          as heartbeats,
    coalesce(n.brews_completed, 0)                     as brews_completed,
    coalesce(n.brews_failed, 0)                        as brews_failed,
    coalesce(e.machine_errors, 0)                      as machine_errors
from calendar c
left join uptime u using (pi_id, date)
left join counts n using (pi_id, date)
left join errors e using (pi_id, date)
left join staging.stg_raspberry_pis pi on pi.pi_id = c.pi_id
left join lateral (
    select p.port_id, p.port_name
    from staging.stg_ports p
    where p.pi_id = c.pi_id and not p.is_deleted
    order by p.created_at desc
    limit 1
) port on true
