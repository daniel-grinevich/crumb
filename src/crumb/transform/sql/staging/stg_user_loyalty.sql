-- unique_key: user_key
select
    data->>'user_id'                             as user_key,
    (data->>'lifetime_points')::integer          as lifetime_points,
    (data->>'balance_points')::integer           as balance_points,
    (data->>'orders_completed')::integer         as orders_completed,
    (data->>'total_spent')::numeric(12, 2)       as total_spent,
    (data->>'current_tier_id')::uuid             as current_tier_id,
    (data->>'updated_at')::timestamptz           as updated_at
from raw.navi_awards_userloyalty
