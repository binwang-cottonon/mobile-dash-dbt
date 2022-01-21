{{ 
    config(
    materialized='incremental'
    ) 
}}

select *
from {{ source('analytics_195776711', 'events_*')}}

{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  where event_timestamp > (select max(event_timestamp) from {{ this }})

{% endif %}

--where _table_suffix BETWEEN format_date('%Y%m%d', date_sub(current_date(), interval 750 day)) 
--   AND format_date('%Y%m%d',date_sub(current_date(), interval 1 day)) 