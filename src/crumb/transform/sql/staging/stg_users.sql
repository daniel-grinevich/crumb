-- unique_key: user_key
-- Pseudonymised: user_key is HMAC(salt, navi user id); no names/emails/phones exist upstream of here.
select
    data->>'id'                                  as user_key,
    data->>'stripe_customer_id'                  as stripe_customer_key,
    coalesce((data->>'is_guest')::boolean, false) as is_guest,
    coalesce((data->>'is_staff')::boolean, false) as is_staff,
    coalesce((data->>'is_active')::boolean, true) as is_active,
    (data->>'email_confirmed')::boolean          as email_confirmed,
    (data->>'date_joined')::timestamptz          as date_joined,
    (data->>'last_login')::timestamptz           as last_login
from raw.navi_users_user
