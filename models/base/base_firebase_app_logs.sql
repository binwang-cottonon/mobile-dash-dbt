{{ config(materialized='table') }}

select *
from {{ source('analytics_195776711', 'events_*')}}