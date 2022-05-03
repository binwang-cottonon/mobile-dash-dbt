{{ 
    config(
      materialized='incremental',
      on_schema_change='append_new_columns'
    ),
    partition_by={
      "field": "event_date_parsed",
      "data_type": "date",
      "granularity": "day"
    }
}}

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

from {{ source('analytics_195776711', 'events_*')}}
where _table_suffix > format_date('%Y%m%d', date_sub(current_date(), interval 90 day))
{% if is_incremental() %}
  -- this filter will only be applied on an incremental run
  and event_timestamp > (select max(event_timestamp) from {{ this }})
{% endif %}

 
