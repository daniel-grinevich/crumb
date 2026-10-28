-- unique_key: date, port_id, menu_item_id
-- What sold, per day per port per menu item (charged, non-test orders).
select
    order_date                                         as date,
    port_id,
    max(port_name)                                     as port_name,
    menu_item_id,
    max(menu_item_name)                                as menu_item_name,
    max(category_name)                                 as category_name,
    sum(quantity)::integer                             as quantity,
    count(distinct order_id)                           as orders,
    sum(line_amount)::numeric(12, 2)                   as revenue,
    dense_rank() over (
        partition by order_date, port_id order by sum(quantity) desc
    )                                                  as rank_in_port_day
from marts.fct_order_items
where is_charged and not is_test and port_id is not null and menu_item_id is not null
group by order_date, port_id, menu_item_id
