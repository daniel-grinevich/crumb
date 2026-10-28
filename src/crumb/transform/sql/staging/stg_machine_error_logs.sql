-- unique_key: error_id
select
    (data->>'id')::uuid                          as error_id,
    (data->>'order_id')::uuid                    as order_id,
    (data->>'raspberry_pi_id')::uuid             as pi_id,
    data->>'error_message'                       as error_message,
    (data->>'is_recoverable')::boolean           as is_recoverable,
    (data->>'created_at')::timestamptz           as created_at
from raw.navi_orders_machineerrorlog
