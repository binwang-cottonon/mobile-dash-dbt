{{ 
  config(
    materialized='incremental',
    unique_key='surrogate_key'
  ) 
}}

with sessions as
(
  select
    event_date_parsed
    ,country
    ,operating_system
    ,count(*) as sessions
  from {{ ref('stg__app_sessions') }}
  where event_date_parsed > date_sub(current_date(), interval 90 day)
  group by 1,2,3  
),

transactions as 
(
  select
    event_date_parsed
    ,country
    ,operating_system
    ,count(*) as transactions
    ,sum(event_value_in_usd) as sales_usd
  from {{ ref('stg__app_transactions') }}
  where event_date_parsed > date_sub(current_date(), interval 90 day)
  group by 1,2,3
),

first_open as 
(
  select
    event_date_parsed
    ,country
    ,operating_system
    ,count(*) as downloads
  from {{ ref('stg__app_first_open') }}
  where event_date_parsed > date_sub(current_date(), interval 90 day)
  group by 1,2,3
)


select
  s.event_date_parsed || '_' || s.country || '_' || s.operating_system as surrogate_key
  ,dd.Trade_Month_Code
  ,dd.TradeWeekCode
  ,s.event_date_parsed
  ,s.country
  ,s.operating_system
  ,ifnull(s.sessions, 0) as sessions
  ,ifnull(fo.downloads, 0) as downloads
  ,ifnull(t.transactions, 0) as transactions
  ,ifnull(t.sales_usd, 0) as sales_usd
  ,ifnull(t.sales_usd*1.47, 0) as sales_aud

from sessions s

left join transactions t  
  on s.event_date_parsed = t.event_date_parsed
  and s.country = t.country
  and s.operating_system = t.operating_system

left join first_open fo  
  on s.event_date_parsed = fo.event_date_parsed
  and s.country = fo.country
  and s.operating_system = fo.operating_system

left join {{ ref('dim__date') }} dd
  on s.event_date_parsed = dd.date  

order by event_date_parsed desc, country  


