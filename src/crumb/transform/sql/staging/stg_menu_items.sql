-- unique_key: menu_item_id
select
    (m.data->>'id')::uuid                        as menu_item_id,
    m.data->>'name'                              as menu_item_name,
    (m.data->>'price')::numeric(10, 2)           as price,
    (m.data->>'category_id')::uuid               as category_id,
    c.data->>'name'                              as category_name,
    coalesce((m.data->>'is_featured')::boolean, false) as is_featured,
    m.data->>'status'                            as status,
    coalesce((m.data->>'is_deleted')::boolean, false) as is_deleted
from raw.navi_menu_menuitem m
left join raw.navi_menu_category c on c.id = m.data->>'category_id'
