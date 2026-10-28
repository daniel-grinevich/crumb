-- unique_key: event_id
select
    (data->>'id')::uuid                          as event_id,
    (data->>'raspberry_pi_id')::uuid             as pi_id,
    data->>'type'                                as event_type,
    data->>'actor_id'                            as actor_user_key,
    (data->>'created_at')::timestamptz           as created_at
from raw.navi_devices_deviceevent
