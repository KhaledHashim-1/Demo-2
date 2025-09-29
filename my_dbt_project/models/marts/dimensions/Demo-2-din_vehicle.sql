with stg as (
    select * from {{ ref('Demo-2-stg_vehicle_price_prediction') }}
)

select distinct
    vehicle_id,
    make,
    model,
    year,
    body_type,
    drivetrain,
    transmission,
    fuel_type,
    vehicle_condition,
    vehicle_trim
from stg;
