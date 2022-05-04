select
  user_pseudo_id
  ,up.value.int_value as ga_session_id
  ,event_timestamp
  ,event_date_parsed
  ,operating_system
  ,country
from {{ ref('base__app_logs_new') }},
     unnest (user_properties) as up
where event_name = 'session_start' and up.key = 'ga_session_id'
