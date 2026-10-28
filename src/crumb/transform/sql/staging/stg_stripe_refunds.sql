-- unique_key: refund_id
select
    data->>'id'                                  as refund_id,
    data->>'charge'                              as charge_id,
    data->>'payment_intent'                      as payment_intent_id,
    data->>'status'                              as status,
    data->>'reason'                              as reason,
    (data->>'amount')::numeric / 100             as amount,
    data->>'currency'                            as currency,
    source_ts                                    as created_at
from raw.stripe_refunds
