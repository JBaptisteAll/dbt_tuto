WITH passengers AS (
    SELECT * FROM {{ ref('stg_passenger') }}
),

passenger_flights AS (
    SELECT 
        passenger_id,
        COUNT(flight_id) as total_flights
    FROM {{ ref('stg_passenger_flights') }}
    GROUP BY 1
)

SELECT
    p.passenger_id,
    p.first_name || ' ' || p.last_name AS full_name,
    p.nationality,
    COALESCE(pf.total_flights, 0) as total_flights
FROM passengers p
LEFT JOIN passenger_flights pf ON p.passenger_id = pf.passenger_id