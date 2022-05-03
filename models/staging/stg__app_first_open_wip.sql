with first_open
as
  (select
      user_pseudo_id
      ,timestamp_micros(event_timestamp) as first_open_ts
      ,event_date_parsed
      ,country
      ,operating_system
      ,row_number() over (partition by user_pseudo_id order by event_timestamp) as rn
    from {{ ref('base__app_logs_wip') }}
  where event_name = 'first_open'
  )

select
  user_pseudo_id
  ,first_open_ts
  ,event_date_parsed
  ,country
  ,operating_system
from first_open     
where rn = 1



