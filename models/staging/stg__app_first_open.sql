select
  user_pseudo_id
  --,up.value.int_value as ga_session_id
  ,event_timestamp
  ,parse_date('%Y%m%d',event_date) as event_date
  ,device.operating_system
  ,regexp_replace(
    case 
      when geo.country not in ('Australia', 'New Zealand', 'United States') then 'Other'
      else geo.country
   end, ' ', '') as country
from {{ ref('base__app_logs') }},
    unnest (user_properties) as up
where event_name = 'first_open' --and up.key = 'ga_session_id'
