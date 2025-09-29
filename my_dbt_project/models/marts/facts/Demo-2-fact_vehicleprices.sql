with stg as (
    select * from {{ ref('Demo-2-stg_vehicle_price_prediction') }}
)

select
    vehicle_id,
    staging_id,
    price,
    mileage,
    engine_hp,
    vehicle_age,
    mileage_per_year,
    price_per_mile,
    is_electric
from stg;
