{{ 
    config(
      materialized='table'
    ) 
}}

with l90d_plus_intraday as (
  select * 
  from {{ source('analytics_195776711', 'events_*')}}
  where _table_suffix > format_date('%Y%m%d', date_sub(current_date(), interval 3 day))
        --EDIT START 11/4 to filter to old app
         and stream_id in ('1425059297','1425059297')
        --EDIT END  11/4
  union all
  select * from {{ source('analytics_195776711', 'events_intraday_*')}} 
        --EDIT START 11/4 to filter to old app
        where stream_id in ('1425059297','1425059297')
        --EDIT END  11/4
)

select *

,parse_date('%Y%m%d',event_date) as event_date_parsed
,case 
    when device.operating_system in ('iOS', 'IOS') then 'iOS' else 'Android' end 
 as operating_system
,regexp_replace(
  case 
    when geo.country not in ('Australia', 'New Zealand', 'United States') then 'Other'
    else geo.country
  end, ' ', '') 
 as country

from l90d_plus_intraday


 
