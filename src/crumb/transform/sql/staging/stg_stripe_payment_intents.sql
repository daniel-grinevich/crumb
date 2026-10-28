-- unique_key: payment_intent_id
-- Stripe amounts are in minor units; Navi only charges USD (2 decimals).
select
    data->>'id'                                  as payment_intent_id,
    data->>'status'                              as status,
    (data->>'amount')::numeric / 100             as amount,
    (data->>'amount_received')::numeric / 100    as amount_received,
    data->>'currency'                            as currency,
    data->>'customer'                            as stripe_customer_key,
    data->>'latest_charge'                       as latest_charge_id,
    data->'metadata'->>'order_id'                as metadata_order_id,
    (data->>'livemode')::boolean                 as livemode,
    source_ts                                    as created_at
from raw.stripe_payment_intents
