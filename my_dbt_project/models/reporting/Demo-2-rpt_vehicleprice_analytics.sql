create or replace view reporting.vehicle_price_analysis as

with fact as (
    select * from {{ ref('Demo-2-fact_vehicle_snapshot') }}
),
vehicles as (
    select * from {{ ref('Demo-2-dim_vehicles') }}
),
aesthetics as (
    select * from {{ ref('Demo-2-dim_aesthetic') }}
),
history as (
    select * from {{ ref('Demo-2-dim_vehicle_history') }}
),
market as (
    select * from {{ ref('Demo-2-dim_market') }}
)

select
    f.snapshot_date,
    f.vehicle_id,
    f.mileage,
    f.vehicle_age,
    f.mileage_per_year,
    f.price,
    f.price_per_mile,
    f.is_electric,
    f.is_outlier_mileage,
    f.is_outlier_price,

    v.make,
    v.model,
    v.year,
    v.engine_hp,
    v.fuel_type,
    v.transmission,
    v.drivetrain,
    v.body_type,
    v.vehicle_condition,
    v.vehicle_trim,

    a.exterior_color,
    a.interior_color,

    h.owner_count,
    h.accident_history,
    h.owner_category,
    h.accident_flag,

    m.seller_type,
    m.brand_popularity,
    m.brand_popularity_bucket

from fact f
left join vehicles v   on f.vehicle_id = v.vehicle_id
left join aesthetics a on f.vehicle_id = a.vehicle_id
left join history h    on f.vehicle_id = h.vehicle_id
left join market m     on f.vehicle_id = m.vehicle_id;
