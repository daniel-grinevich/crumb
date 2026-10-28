-- unique_key: order_id
select
    (data->>'id')::uuid                          as order_id,
    data->>'user_id'                             as user_key,
    (data->>'navi_port_id')::uuid                as port_id,
    (data->>'payment_id')::uuid                  as payment_id,
    data->>'order_status'                        as order_status_code,
    case data->>'order_status'
        when 'O' then 'ordered'
        when 'S' then 'sent'
        when 'D' then 'done'
        when 'C' then 'cancelled'
        else 'unknown'
    end                                          as order_status,
    (data->>'claimed_by_id')::uuid               as claimed_by_pi_id,
    (data->>'claimed_at')::timestamptz           as claimed_at,
    coalesce((data->>'is_test')::boolean, false) as is_test,
    coalesce((data->>'is_deleted')::boolean, false) as is_deleted,
    (data->>'created_at')::timestamptz           as created_at,
    (data->>'updated_at')::timestamptz           as updated_at
from raw.navi_orders_order
