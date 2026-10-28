-- unique_key: charge_id
select
    data->>'id'                                  as charge_id,
    data->>'payment_intent'                      as payment_intent_id,
    data->>'status'                              as status,
    coalesce((data->>'paid')::boolean, false)    as paid,
    coalesce((data->>'refunded')::boolean, false) as refunded,
    coalesce((data->>'disputed')::boolean, false) as disputed,
    (data->>'amount')::numeric / 100             as amount,
    (data->>'amount_captured')::numeric / 100    as amount_captured,
    coalesce((data->>'amount_refunded')::numeric, 0) / 100 as amount_refunded,
    data->>'currency'                            as currency,
    data->>'customer'                            as stripe_customer_key,
    data->'metadata'->>'order_id'                as metadata_order_id,
    data->'metadata'->>'environment'             as metadata_environment,
    data->>'failure_code'                        as failure_code,
    (data->>'livemode')::boolean                 as livemode,
    source_ts                                    as created_at
from raw.stripe_charges
