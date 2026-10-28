-- unique_key: charge_id
-- Every Stripe charge, attributed (or not) to this Navi environment.
-- Stripe test accounts are shared by local dev, CI and staging, so a charge only
-- belongs here if this Navi source knows it: its PaymentIntent is recorded on a
-- Navi payment, or its order_id metadata (Navi sets it on the PaymentIntent;
-- Stripe doesn't copy it to the Charge) is an order in this source. Objects
-- tagged with metadata.environment must match CRUMB_ENVIRONMENT.
with charges as (
    select
        c.charge_id,
        c.payment_intent_id,
        c.status,
        c.amount,
        c.amount_refunded,
        c.created_at,
        coalesce(c.metadata_order_id, pi.metadata_order_id)       as metadata_order_id,
        coalesce(c.metadata_environment, pi.metadata_environment) as metadata_environment
    from staging.stg_stripe_charges c
    left join staging.stg_stripe_payment_intents pi using (payment_intent_id)
),
matched as (
    select
        ch.*,
        p.payment_id,
        o_pay.order_id                                            as order_id_via_payment,
        o_meta.order_id                                           as order_id_via_metadata
    from charges ch
    left join staging.stg_payments p on p.stripe_payment_intent_id = ch.payment_intent_id
    left join staging.stg_orders o_pay on o_pay.payment_id = p.payment_id
    left join staging.stg_orders o_meta on o_meta.order_id::text = ch.metadata_order_id
)
select
    charge_id,
    payment_intent_id,
    status,
    amount,
    amount_refunded,
    created_at,
    created_at::date                                              as date,
    metadata_order_id,
    metadata_environment,
    payment_id,
    coalesce(order_id_via_payment, order_id_via_metadata)        as order_id,
    order_id_via_payment is not null                              as recorded_in_navi,
    case
        when metadata_environment is not null
             and metadata_environment is distinct from current_setting('crumb.environment', true)
            then 'other_environment'
        when payment_id is null and order_id_via_metadata is null
            then 'unknown_to_this_navi'
        else 'in_scope'
    end                                                           as scope
from matched
