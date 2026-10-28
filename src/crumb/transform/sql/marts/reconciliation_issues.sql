-- Stripe <-> Navi mismatches, one row per problem. Empty means reconciled.
--   charge_without_order : a succeeded Stripe charge no Navi order accounts for
--   double_charge        : more than one succeeded charge for the same order
--   missing_stripe_charge: Navi says charged, Stripe has no succeeded charge (only
--                          checked once Stripe data has been loaded)
--   refund_mismatch      : Navi refunded amount != Stripe refunded amount
with charges as (
    select
        c.*,
        coalesce(o.order_id::text, c.metadata_order_id) as order_ref,
        o.order_id
    from staging.stg_stripe_charges c
    left join staging.stg_payments p on p.stripe_payment_intent_id = c.payment_intent_id
    left join staging.stg_orders o on o.payment_id = p.payment_id
    where c.status = 'succeeded'
),
stripe_loaded as (
    select exists (select 1 from staging.stg_stripe_charges) as loaded
),
charge_totals as (
    select payment_intent_id, sum(amount_refunded) as amount_refunded
    from staging.stg_stripe_charges
    group by payment_intent_id
)
select
    'charge_without_order'::text  as issue_type,
    charge_id,
    payment_intent_id,
    null::uuid                    as order_id,
    amount,
    created_at,
    created_at::date              as date,
    'succeeded Stripe charge has no matching Navi order' as detail
from charges
where order_id is null
union all
select
    'double_charge',
    charge_id,
    payment_intent_id,
    order_id,
    amount,
    created_at,
    created_at::date,
    format('%s succeeded charges for order %s', n, order_ref)
from (
    select *, count(*) over (partition by order_ref) as n
    from charges
    where order_ref is not null
) d
where n > 1
union all
select
    'missing_stripe_charge',
    null,
    p.stripe_payment_intent_id,
    o.order_id,
    p.total_amount,
    p.updated_at,
    p.updated_at::date,
    'Navi payment is charged but Stripe has no succeeded charge for its PaymentIntent'
from staging.stg_payments p
join staging.stg_orders o on o.payment_id = p.payment_id
cross join stripe_loaded s
where s.loaded
  and p.is_charged
  and not o.is_test
  and not exists (
      select 1 from charges c where c.payment_intent_id = p.stripe_payment_intent_id
  )
union all
select
    'refund_mismatch',
    null,
    p.stripe_payment_intent_id,
    o.order_id,
    p.refunded_amount - t.amount_refunded,
    p.updated_at,
    p.updated_at::date,
    format('Navi refunded %s, Stripe refunded %s', p.refunded_amount, t.amount_refunded)
from staging.stg_payments p
join staging.stg_orders o on o.payment_id = p.payment_id
join charge_totals t on t.payment_intent_id = p.stripe_payment_intent_id
where p.refunded_amount <> t.amount_refunded
