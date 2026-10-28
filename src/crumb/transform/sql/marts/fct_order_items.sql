-- unique_key: order_item_id
with customizations as (
    select order_item_id, sum(line_amount) as amount, sum(quantity) as count
    from staging.stg_order_customizations
    where not is_deleted
    group by order_item_id
)
select
    i.order_item_id,
    i.order_id,
    o.order_date,
    o.port_id,
    o.port_name,
    o.order_status,
    o.is_test,
    o.is_charged,
    i.menu_item_id,
    m.menu_item_name,
    m.category_name,
    i.quantity,
    i.unit_price,
    coalesce(c.count, 0)::integer                      as customization_count,
    coalesce(c.amount, 0)::numeric(12, 2)              as customization_amount,
    (i.line_amount + coalesce(c.amount, 0))::numeric(12, 2) as line_amount
from staging.stg_order_items i
join marts.fct_orders o using (order_id)
left join staging.stg_menu_items m using (menu_item_id)
left join customizations c using (order_item_id)
where not i.is_deleted
