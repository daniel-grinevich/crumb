-- unique_key: order_customization_id
select
    (data->>'id')::uuid                          as order_customization_id,
    (data->>'order_item_id')::uuid               as order_item_id,
    (data->>'customization_id')::uuid            as customization_id,
    (data->>'quantity')::integer                 as quantity,
    (data->>'unit_price')::numeric(10, 2)        as unit_price,
    (data->>'quantity')::integer * (data->>'unit_price')::numeric(10, 2) as line_amount,
    coalesce((data->>'is_deleted')::boolean, false) as is_deleted,
    (data->>'created_at')::timestamptz           as created_at
from raw.navi_orders_ordercustomization
