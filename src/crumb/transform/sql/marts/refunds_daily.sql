-- unique_key: date
-- Refunds per day from both sides, plus that day's reconciliation issues.
-- Navi refunds are dated by the payment's last update (when it was refunded);
-- Stripe refunds by their creation time.
with navi as (
    select updated_at::date as date, count(*) as refund_count, sum(refunded_amount) as amount
    from staging.stg_payments
    where refunded_amount > 0
    group by 1
),
stripe as (
    select created_at::date as date, count(*) as refund_count, sum(amount) as amount
    from staging.stg_stripe_refunds
    where status = 'succeeded'
    group by 1
),
issues as (
    select
        date,
        count(*) filter (where issue_type = 'charge_without_order')  as charges_without_order,
        count(*) filter (where issue_type = 'double_charge')         as double_charges,
        count(*) filter (where issue_type = 'missing_stripe_charge') as missing_stripe_charges,
        count(*) filter (where issue_type = 'refund_mismatch')       as refund_mismatches
    from marts.reconciliation_issues
    group by 1
),
days as (
    select date from navi union select date from stripe union select date from issues
),
stripe_loaded as (
    select exists (select 1 from staging.stg_stripe_charges) as loaded
)
select
    d.date,
    coalesce(n.refund_count, 0)                        as navi_refund_count,
    coalesce(n.amount, 0)::numeric(12, 2)              as navi_refund_amount,
    coalesce(s.refund_count, 0)                        as stripe_refund_count,
    coalesce(s.amount, 0)::numeric(12, 2)              as stripe_refund_amount,
    (coalesce(n.amount, 0) - coalesce(s.amount, 0))::numeric(12, 2) as refund_amount_diff,
    coalesce(i.charges_without_order, 0)               as charges_without_order,
    coalesce(i.double_charges, 0)                      as double_charges,
    coalesce(i.missing_stripe_charges, 0)              as missing_stripe_charges,
    coalesce(i.refund_mismatches, 0)                   as refund_mismatches,
    l.loaded                                           as stripe_loaded,
    l.loaded
        and coalesce(i.charges_without_order, 0) + coalesce(i.double_charges, 0)
            + coalesce(i.missing_stripe_charges, 0) + coalesce(i.refund_mismatches, 0) = 0
                                                       as is_reconciled
from days d
cross join stripe_loaded l
left join navi n using (date)
left join stripe s using (date)
left join issues i using (date)
