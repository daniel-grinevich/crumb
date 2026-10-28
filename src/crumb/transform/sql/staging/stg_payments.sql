-- unique_key: payment_id
-- Charged = a PaymentIntent succeeded (status stays 'succeeded' or moves to 'refunded').
-- Older Navi schemas have no amount_refunded; a 'refunded' payment then counts as fully refunded.
select
    payment_id,
    stripe_payment_intent_id,
    status,
    status in ('succeeded', 'refunded') and stripe_payment_intent_id is not null as is_charged,
    currency,
    subtotal,
    tax_amount,
    total_amount,
    amount_received,
    case
        when amount_refunded > 0 then amount_refunded
        when status = 'refunded' then total_amount
        else 0
    end::numeric(10, 2)                          as refunded_amount,
    is_deleted,
    created_at,
    updated_at
from (
    select
        (data->>'id')::uuid                      as payment_id,
        nullif(data->>'stripe_payment_intent_id', '') as stripe_payment_intent_id,
        data->>'status'                          as status,
        coalesce(data->>'currency', 'usd')       as currency,
        coalesce((data->>'subtotal')::numeric(10, 2), 0)        as subtotal,
        coalesce((data->>'tax_amount')::numeric(10, 2), 0)      as tax_amount,
        coalesce((data->>'total_amount')::numeric(10, 2), 0)    as total_amount,
        coalesce((data->>'amount_received')::numeric(10, 2), 0) as amount_received,
        coalesce((data->>'amount_refunded')::numeric(10, 2), 0) as amount_refunded,
        coalesce((data->>'is_deleted')::boolean, false) as is_deleted,
        (data->>'created_at')::timestamptz       as created_at,
        (data->>'updated_at')::timestamptz       as updated_at
    from raw.navi_payments_payment
) p
