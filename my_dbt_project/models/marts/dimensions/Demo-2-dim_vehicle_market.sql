with stg as (
    select * from {{ ref('Demo-2-stg_vehicle_price_prediction') }}
)

select
    vehicle_id,
    seller_type,
    brand_popularity
from stg
where seller_type is not null
   or brand_popularity is not null;
