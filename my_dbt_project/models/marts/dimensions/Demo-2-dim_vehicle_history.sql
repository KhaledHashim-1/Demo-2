with stg as (
    select * from {{ ref('Demo-2-stg_vehicle_price_prediction') }}
)

select
    vehicle_id,
    owner_count,
    accident_history
from stg
where owner_count is not null
   or accident_history is not null;
