-- unique_key: date, port_id
-- Per NaviPort per day (reporting timezone), excluding staff test orders.
-- revenue = charged totals incl. tax; refunds are attributed to the order's day.
select
    order_date                                         as date,
    port_id,
    max(port_name)                                     as port_name,
    count(*)                                           as orders_placed,
    count(*) filter (where is_charged)                 as orders,
    count(*) filter (where order_status = 'cancelled') as orders_cancelled,
    sum(item_count) filter (where is_charged)::integer as items_sold,
    sum(gross_revenue)::numeric(12, 2)                 as revenue,
    sum(tax_amount) filter (where is_charged)::numeric(12, 2) as tax,
    sum(refunded_amount)::numeric(12, 2)               as refunds,
    count(*) filter (where refunded_amount > 0)        as refund_count,
    sum(net_revenue)::numeric(12, 2)                   as net_revenue,
    round(sum(gross_revenue) / nullif(count(*) filter (where is_charged), 0), 2) as avg_ticket
from marts.fct_orders
where not is_test and port_id is not null
group by order_date, port_id
