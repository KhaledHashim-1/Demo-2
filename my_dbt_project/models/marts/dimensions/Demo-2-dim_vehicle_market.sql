with stg as (
    select * 
    from {{ ref('Demo-2-stg_vehicle_price_prediction') }}
)

select distinct
    vehicle_id,
    seller_type,
    brand_popularity,

    case 
        when brand_popularity < 0.2 then 'low'
        when brand_popularity < 0.6 then 'medium'
        else 'high'
    end as brand_popularity_bucket

from stg
where seller_type is not null
   or brand_popularity is not null;
