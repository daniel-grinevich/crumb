-- unique_key: order_id
-- One row per non-deleted Navi order. Money comes from the Payment (tax included);
-- items_amount is the menu-price sum (items + customizations) before tax.
with customizations as (
    select order_item_id, sum(line_amount) as amount
    from staging.stg_order_customizations
    where not is_deleted
    group by order_item_id
),
items as (
    select
        i.order_id,
        sum(i.quantity)                                as item_count,
        sum(i.line_amount + coalesce(c.amount, 0))     as items_amount
    from staging.stg_order_items i
    left join customizations c using (order_item_id)
    where not i.is_deleted
    group by i.order_id
)
select
    o.order_id,
    o.created_at,
    o.created_at::date                                 as order_date,
    o.port_id,
    p.port_name,
    o.user_key,
    o.order_status,
    o.is_test,
    coalesce(i.item_count, 0)::integer                 as item_count,
    coalesce(i.items_amount, 0)::numeric(12, 2)        as items_amount,
    o.payment_id,
    pay.status                                         as payment_status,
    pay.stripe_payment_intent_id,
    coalesce(pay.is_charged, false)                    as is_charged,
    coalesce(pay.subtotal, 0)                          as subtotal,
    coalesce(pay.tax_amount, 0)                        as tax_amount,
    coalesce(pay.total_amount, 0)                      as total_amount,
    case when pay.is_charged then pay.total_amount else 0 end::numeric(12, 2) as gross_revenue,
    coalesce(pay.refunded_amount, 0)                   as refunded_amount,
    (case when pay.is_charged then pay.total_amount else 0 end
        - coalesce(pay.refunded_amount, 0))::numeric(12, 2) as net_revenue,
    o.updated_at
from staging.stg_orders o
left join items i using (order_id)
left join staging.stg_ports p on p.port_id = o.port_id
left join staging.stg_payments pay on pay.payment_id = o.payment_id
where not o.is_deleted
