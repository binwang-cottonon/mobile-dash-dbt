with first_open
as
  (select
      user_pseudo_id
      ,timestamp_micros(event_timestamp) as first_open_ts
      ,PARSE_DATE('%Y%m%d',event_date) as event_date
      ,REGEXP_REPLACE(
      case 
        when geo.country not in ('Australia', 'New Zealand', 'United States') then 'Other'
        else geo.country
      end, ' ', '') as country
      ,case 
        when device.operating_system in ('iOS', 'IOS') then 'iOS' else 'Android' end as operating_system
      ,row_number() over (partition by user_pseudo_id order by event_timestamp) as rn
    from `cotton-on-e41b2.analytics_195776711.events_*` ev 
  where event_name = 'first_open'
  )

select
  user_pseudo_id,
  first_open_ts,
  event_date,
  country,
  operating_system
from first_open     
where rn = 1



