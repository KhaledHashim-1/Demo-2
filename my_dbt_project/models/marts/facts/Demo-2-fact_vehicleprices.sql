with stg as (
    select * 
    from {{ ref('Demo-2-stg_vehicle_price_prediction') }}
)

select
    vehicle_id,
    current_date() as snapshot_date,  
    
    mileage,
    price,
    vehicle_age,
    mileage_per_year,
    price_per_mile,
    
    is_electric,
    is_outlier_mileage,
    is_outlier_price

from stg
