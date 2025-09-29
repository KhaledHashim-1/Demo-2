select
    make,
    year,
    avg(price) as avg_price,
    count(distinct vehicle_id) as vehicle_count
from {{ ref('Demo-2-fact_vehicleprices') }}
group by make, year;
