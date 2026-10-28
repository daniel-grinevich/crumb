-- unique_key: pi_id
select
    (data->>'id')::uuid                          as pi_id,
    data->>'name'                                as pi_name,
    nullif(data->>'location', '')                as location,
    nullif(data->>'firmware_version', '')        as firmware_version,
    coalesce((data->>'is_connected')::boolean, false) as is_enabled,
    coalesce((data->>'is_simulated')::boolean, false) as is_simulated,
    (data->>'last_seen')::timestamptz            as last_seen,
    coalesce((data->>'is_deleted')::boolean, false) as is_deleted,
    (data->>'created_at')::timestamptz           as created_at
from raw.navi_devices_raspberrypi
