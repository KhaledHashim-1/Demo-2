with source as (
    select * from {{ source('khaled_projects', 'Demo-2-raw_vehicle_price_prediction') }}
),

cleaned as (
    select

        to_hex(md5(concat_ws('|',
            lower(trim(nullif(make, ''))),
            lower(trim(nullif(model, ''))),
            nullif(year, ''),
            nullif(mileage, ''),
            nullif(engine_hp, ''),
            lower(trim(nullif(transmission, ''))),
            lower(trim(nullif(fuel_type, '')))
        ))) as vehicle_id,


        row_number() over () as staging_id,

        lower(trim(nullif(make, ''))) as make,
        lower(trim(nullif(model, ''))) as model,
        cast(nullif(year, '') as int64) as year,
        cast(nullif(mileage, '') as int64) as mileage,
        cast(nullif(engine_hp, '') as float64) as engine_hp,
        lower(trim(nullif(transmission, ''))) as transmission,
        lower(trim(nullif(fuel_type, ''))) as fuel_type,
        lower(trim(nullif(drivetrain, ''))) as drivetrain,
        lower(trim(nullif(body_type, ''))) as body_type,
        lower(trim(nullif(exterior_color, ''))) as exterior_color,
        lower(trim(nullif(interior_color, ''))) as interior_color,
        cast(nullif(owner_count, '') as int64) as owner_count,

        case 
            when lower(trim(accident_history)) in ('', 'none', 'no', '0') then 'none'
            else lower(trim(accident_history))
        end as accident_history,

        lower(trim(nullif(seller_type, ''))) as seller_type,
        lower(trim(nullif(condition, ''))) as vehicle_condition,
        lower(trim(nullif(trim, ''))) as vehicle_trim,

        cast(nullif(vehicle_age, '') as int64) as vehicle_age,

        case 
            when cast(nullif(vehicle_age, '') as int64) > 0 
                 then cast(mileage as float64) / cast(vehicle_age as float64) 
            else null 
        end as mileage_per_year,

        cast(nullif(brand_popularity, '') as float64) as brand_popularity,
        cast(nullif(price, '') as numeric) as price,

        case when lower(fuel_type) = 'electric' then true else false end as is_electric,
        case when mileage is not null and mileage > 0 
             then cast(price as numeric) / mileage 
             else null 
        end as price_per_mile
    from source
)

select * from cleaned;
