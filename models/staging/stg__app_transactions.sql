select
   user_pseudo_id
  --,up.value.int_value as ga_session_id
  ,event_timestamp
  ,event_date_parsed
  ,country
  ,operating_system
  ,max(event_value_in_usd) as event_value_in_usd
from {{ ref('base__app_logs') }},
     unnest (user_properties) as up
where event_name = 'ecommerce_purchase' or event_name = 'purchase' --and up.key = 'ga_session_id'
group by 1,2,3,4,5
