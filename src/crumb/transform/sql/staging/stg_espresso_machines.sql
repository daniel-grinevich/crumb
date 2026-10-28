-- unique_key: espresso_machine_id
select
    (e.data->>'id')::uuid                        as espresso_machine_id,
    e.data->>'name'                              as machine_name,
    e.data->>'serial_number'                     as serial_number,
    (e.data->>'machine_type_id')::uuid           as machine_type_id,
    t.data->>'name'                              as machine_type_name,
    t.data->>'model_number'                      as model_number,
    (t.data->>'maintenance_frequency')::integer  as maintenance_frequency_days,
    (e.data->>'last_maintenance_at')::timestamptz as last_maintenance_at,
    coalesce((e.data->>'is_deleted')::boolean, false) as is_deleted
from raw.navi_devices_espressomachine e
left join raw.navi_devices_machinetype t on t.id = e.data->>'machine_type_id'
