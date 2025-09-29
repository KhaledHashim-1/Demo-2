with fact as (
    select * from {{ ref('Demo-2-fact_vehicleprices') }}
),
aesthetics as (
    select * from {{ ref('Demo-2-dim_vehicle_aesthetics') }}
),
history as (
    select * from {{ ref('Demo-2-dim_vehicle_history') }}
),
market as (
    select * from {{ ref('Demo-2-dim_vehicle_market') }}
)

select
    f.vehicle_id,
    f.make,
    f.model,
    f.year,
    f.engine_hp,
    f.mileage,
    f.vehicle_age,
    f.mileage_per_year,
    f.fuel_type,
    f.transmission,
    f.drivetrain,
    f.body_type,
    f.vehicle_condition,
    f.is_electric,
    f.price,
    f.price_per_mile,

    a.exterior_color,
    a.interior_color,

    h.owner_count,
    h.accident_history,
    
    m.seller_type,
    m.brand_popularity

from fact f
left join aesthetics a on f.vehicle_id = a.vehicle_id
left join history h    on f.vehicle_id = h.vehicle_id
left join market m     on f.vehicle_id = m.vehicle_id;
