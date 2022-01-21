select *
from {{ source('analytics_195776711', 'dateDimension')}}