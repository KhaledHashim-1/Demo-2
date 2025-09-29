with stg as (
    select * from {{ ref('Demo-2-stg_vehicle_price_prediction') }}
)

select
    vehicle_id,
    make,
    model,
    year,
    engine_hp,
    mileage,
    vehicle_age,
    mileage_per_year,
    fuel_type,
    transmission,
    drivetrain,
    body_type,
    vehicle_condition,
    is_electric,
    price,
    price_per_mile
from stg;
