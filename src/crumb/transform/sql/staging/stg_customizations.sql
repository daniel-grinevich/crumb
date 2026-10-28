-- unique_key: customization_id
select
    (data->>'id')::uuid                          as customization_id,
    data->>'name'                                as customization_name,
    (data->>'price')::numeric(10, 2)             as price
from raw.navi_menu_customization
