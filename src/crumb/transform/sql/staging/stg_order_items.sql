-- unique_key: order_item_id
select
    (data->>'id')::uuid                          as order_item_id,
    (data->>'order_id')::uuid                    as order_id,
    (data->>'menu_item_id')::uuid                as menu_item_id,
    (data->>'quantity')::integer                 as quantity,
    (data->>'unit_price')::numeric(10, 2)        as unit_price,
    (data->>'quantity')::integer * (data->>'unit_price')::numeric(10, 2) as line_amount,
    coalesce((data->>'is_deleted')::boolean, false) as is_deleted,
    (data->>'created_at')::timestamptz           as created_at,
    (data->>'updated_at')::timestamptz           as updated_at
from raw.navi_orders_orderitem
