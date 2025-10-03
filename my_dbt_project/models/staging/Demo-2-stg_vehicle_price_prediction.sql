with source as (
    select * 
    from {{ source('khaled_projects', 'Demo-2-raw_vehicle_price_prediction') }}
),

cleaned as (
    select
        to_hex(md5(
            concat(
                coalesce(lower(trim(nullif(cast(make as string), ''))), ''), '|',
                coalesce(lower(trim(nullif(cast(model as string), ''))), ''), '|',
                coalesce(nullif(cast(year as string), ''), ''), '|',
                coalesce(lower(trim(nullif(cast(transmission as string), ''))), ''), '|',
                coalesce(lower(trim(nullif(cast(fuel_type as string), ''))), ''), '|',
                coalesce(lower(trim(nullif(cast(drivetrain as string), ''))), ''), '|',
                coalesce(lower(trim(nullif(cast(trim as string), ''))), '')
            )
        )) as vehicle_id,

        lower(trim(nullif(cast(make as string), ''))) as make,
        lower(trim(nullif(cast(model as string), ''))) as model,
        safe_cast(nullif(cast(year as string), '') as int64) as year,
        safe_cast(nullif(cast(mileage as string), '') as int64) as mileage,
        safe_cast(nullif(cast(engine_hp as string), '') as float64) as engine_hp,

        lower(trim(nullif(cast(transmission as string), ''))) as transmission,
        lower(trim(nullif(cast(fuel_type as string), ''))) as fuel_type,
        lower(trim(nullif(cast(drivetrain as string), ''))) as drivetrain,
        lower(trim(nullif(cast(body_type as string), ''))) as body_type,
        lower(trim(nullif(cast(exterior_color as string), ''))) as exterior_color,
        lower(trim(nullif(cast(interior_color as string), ''))) as interior_color,

        safe_cast(nullif(cast(owner_count as string), '') as int64) as owner_count,

        case 
            when lower(trim(cast(accident_history as string))) in ('', 'none', 'no', '0', 'n/a') then 'none'
            when regexp_contains(lower(cast(accident_history as string)), r'(yes|minor|major|reported|accident)') then 'reported'
            else lower(trim(cast(accident_history as string)))
        end as accident_history,

        lower(trim(nullif(cast(seller_type as string), ''))) as seller_type,
        lower(trim(nullif(cast(condition as string), ''))) as vehicle_condition,
        lower(trim(nullif(cast(trim as string), ''))) as vehicle_trim,

        safe_cast(nullif(cast(vehicle_age as string), '') as int64) as vehicle_age,

        case 
            when safe_cast(vehicle_age as int64) > 0 and safe_cast(mileage as int64) is not null
                 then round(safe_cast(mileage as float64) / safe_cast(vehicle_age as float64), 2)
            else null 
        end as mileage_per_year,

        safe_cast(nullif(cast(brand_popularity as string), '') as float64) as brand_popularity,
        safe_cast(nullif(cast(price as string), '') as numeric) as price,

        case when lower(cast(fuel_type as string)) = 'electric' then true else false end as is_electric,

        case 
            when mileage is not null and mileage > 0 and price is not null and price > 0
                 then round(price / cast(mileage as numeric), 4)
            else null 
        end as price_per_mile,

        case when mileage > 500000 then true else false end as is_outlier_mileage,
        case when price > 200000 then true else false end as is_outlier_price

    from source
)

select * from cleaned
