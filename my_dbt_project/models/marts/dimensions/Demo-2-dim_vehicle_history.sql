with stg as (
    select * 
    from {{ ref('Demo-2-stg_vehicle_price_prediction') }}
)

select distinct
    vehicle_id,
    owner_count,
    accident_history,

    case 
        when owner_count = 1 then 'single-owner'
        when owner_count between 2 and 3 then 'multi-owner'
        when owner_count > 3 then 'fleet/other'
    end as owner_category,

    case 
        when accident_history = 'none' then false else true
    end as accident_flag

from stg
where owner_count is not null
   or accident_history is not null;
