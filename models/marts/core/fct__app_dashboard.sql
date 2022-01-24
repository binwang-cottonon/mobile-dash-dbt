with sessions as
(
  select
    event_date
    ,country
    ,operating_system
    ,count(*) as sessions
  from {{ ref('stg__app_sessions') }}
  group by 1,2,3  
),

transactions as 
(
  select
    event_date
    ,country
    ,operating_system
    ,count(*) as transactions
    ,sum(event_value_in_usd) as sales_usd
  from {{ ref('stg__app_transactions') }}
  group by 1,2,3
),

first_open as 
(
  select
    event_date
    ,country
    ,operating_system
    ,count(*) as downloads
  from {{ ref('stg__app_first_open') }}
  group by 1,2,3
)


select
  s.event_date
  ,s.country
  ,s.operating_system
  ,s.sessions
  ,fo.downloads
  ,t.transactions
  ,t.sales_usd

from sessions s

left join transactions t  
  on s.event_date = t.event_date
  and s.country = t.country
  and s.operating_system = t.operating_system

left join first_open fo  
  on s.event_date = fo.event_date
  and s.country = fo.country
  and s.operating_system = fo.operating_system

order by event_date desc, country  


