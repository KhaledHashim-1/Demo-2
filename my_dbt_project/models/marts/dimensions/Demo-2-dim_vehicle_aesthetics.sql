with stg as (
    select * 
    from {{ ref('Demo-2-stg_vehicle_price_prediction') }}
)

select distinct
    vehicle_id,
    exterior_color,
    interior_color
from stg
where exterior_color is not null
   or interior_color is not null
