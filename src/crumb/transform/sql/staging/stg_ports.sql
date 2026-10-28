-- unique_key: port_id
select
    (data->>'id')::uuid                          as port_id,
    data->>'name'                                as port_name,
    data->>'slug'                                as port_slug,
    nullif(data->>'city', '')                    as city,
    nullif(data->>'state_or_region', '')         as state_or_region,
    nullif(data->>'postal_code', '')             as postal_code,
    nullif(data->>'country', '')                 as country,
    (data->>'latitude')::numeric(9, 6)           as latitude,
    (data->>'longitude')::numeric(9, 6)          as longitude,
    (data->>'raspberry_pi_id')::uuid             as pi_id,
    (data->>'espresso_machine_id')::uuid         as espresso_machine_id,
    coalesce((data->>'accepting_orders')::boolean, true) as accepting_orders,
    data->>'status'                              as status,
    coalesce((data->>'is_deleted')::boolean, false) as is_deleted,
    (data->>'created_at')::timestamptz           as created_at
from raw.navi_devices_naviport
