Welcome to your new dbt project!

### Using the starter project

Try running the following commands:
- dbt run
- dbt test


### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [dbt community](http://community.getbdt.com/) to learn from other analytics engineers
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices


### 

#### update fct__app_dashboard to include a surrogate key so we can do increments

- update table with surrogate key
```sql
--scripts to update app_dashboard table
alter table `cotton-on-e41b2.dbt_prod_app.fct__app_dashboard`
add column surrogate_key STRING;

update `cotton-on-e41b2.dbt_prod_app.fct__app_dashboard`
set surrogate_key = event_date_parsed || '_' || country || '_' || operating_system
where event_date_parsed is not null;
-- end of script
```

- update model


### rebuild history data of fct__app_dashboard_new


```sql
declare x int64 default 1;
declare dates array<string>;

set dates = (
  select array_agg(datekey) as list
  from (
    select cast(datekey as string) as datekey
    from `cotton-on-e41b2.dbt_prod_app.dim__date` 
    where date between '2019-05-14' and '2022-05-xx'
  )
);

while x <= array_length(dates) do

insert into `cotton-on-e41b2.dbt_prod_app.fct__app_dashboard_new` 
with l90d_plus_intraday as (
  select * 
  from `cotton-on-e41b2.analytics_195776711.events_*`
  where _table_suffix = dates[ORDINAL(x)]
  union all
  select * from `cotton-on-e41b2.analytics_195776711.events_intraday_*`
),
base as (
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
),
sessions_temp as (
  select
    user_pseudo_id
    ,up.value.int_value as ga_session_id
    ,event_timestamp
    ,event_date_parsed
    ,operating_system
    ,country
  from base,
  unnest (user_properties) as up
  where event_name = 'session_start' and up.key = 'ga_session_id'
),
first_open_t1 as (
  select
      user_pseudo_id
      ,timestamp_micros(event_timestamp) as first_open_ts
      ,event_date_parsed
      ,country
      ,operating_system
      ,row_number() over (partition by user_pseudo_id order by event_timestamp) as rn
    from base
  where event_name = 'first_open'
),
first_open_temp as (
  select
    user_pseudo_id
    ,first_open_ts
    ,event_date_parsed
    ,country
    ,operating_system
  from first_open_t1
  where rn = 1
),
transactions_temp as (
  select
    user_pseudo_id
    --,up.value.int_value as ga_session_id
    ,event_timestamp
    ,event_date_parsed
    ,country
    ,operating_system
    ,max(event_value_in_usd) as event_value_in_usd
  from base,
      unnest (user_properties) as up
  where event_name = 'ecommerce_purchase' or event_name = 'purchase' --and up.key = 'ga_session_id'
  group by 1,2,3,4,5
),
sessions as
(
  select
    event_date_parsed
    ,country
    ,operating_system
    ,count(*) as sessions
  from sessions_temp
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
  from transactions_temp
  group by 1,2,3
),
first_open as 
(
  select
    event_date_parsed
    ,country
    ,operating_system
    ,count(*) as downloads
  from first_open_temp
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

left join `cotton-on-e41b2.dbt_prod_app.dim__date` dd
  on s.event_date_parsed = dd.date  
order by event_date_parsed desc, country  ;

SET x = x + 1;
end while;
```