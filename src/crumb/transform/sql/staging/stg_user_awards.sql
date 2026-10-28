-- unique_key: user_award_id
select
    (ua.data->>'id')::uuid                       as user_award_id,
    ua.data->>'user_id'                          as user_key,
    (ua.data->>'award_id')::uuid                 as award_id,
    a.data->>'name'                              as award_name,
    a.data->>'rule_type'                         as rule_type,
    (ua.data->>'level_id')::uuid                 as level_id,
    l.data->>'name'                              as level_name,
    (ua.data->>'earned_at')::timestamptz         as earned_at
from raw.navi_awards_useraward ua
left join raw.navi_awards_award a on a.id = ua.data->>'award_id'
left join raw.navi_awards_awardlevel l on l.id = ua.data->>'level_id'
