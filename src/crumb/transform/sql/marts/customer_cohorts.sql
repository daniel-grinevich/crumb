-- unique_key: cohort_month, month_offset
-- Pseudonymised customers grouped by first charged order month. One row per
-- cohort per month since first order: how many came back (retention), plus the
-- cohort's overall repeat rate (2+ charged orders ever).
with paid as (
    select user_key, order_date
    from marts.fct_orders
    where is_charged and not is_test and user_key is not null
),
firsts as (
    select
        user_key,
        date_trunc('month', min(order_date))::date      as cohort_month,
        count(*)                                        as order_count
    from paid
    group by user_key
),
sizes as (
    select
        cohort_month,
        count(*)                                        as cohort_size,
        count(*) filter (where order_count > 1)         as repeat_customers
    from firsts
    group by cohort_month
),
activity as (
    select distinct
        f.cohort_month,
        p.user_key,
        ((extract(year from p.order_date) - extract(year from f.cohort_month)) * 12
            + extract(month from p.order_date) - extract(month from f.cohort_month))::integer
                                                        as month_offset
    from paid p
    join firsts f using (user_key)
)
select
    a.cohort_month,
    a.month_offset,
    s.cohort_size,
    count(*)                                            as active_customers,
    round(count(*)::numeric / s.cohort_size, 4)         as retention_rate,
    s.repeat_customers,
    round(s.repeat_customers::numeric / s.cohort_size, 4) as repeat_rate
from activity a
join sizes s using (cohort_month)
group by a.cohort_month, a.month_offset, s.cohort_size, s.repeat_customers
