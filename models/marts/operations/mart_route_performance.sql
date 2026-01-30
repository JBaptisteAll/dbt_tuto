WITH flight_perf AS (
    SELECT * FROM {{ ref('int_flights_performance') }}
),

airports AS (
    SELECT * FROM {{ ref('stg_airports') }}
),

route_metrics AS (
    SELECT
        -- On recrée les labels ici
        fp.airline_code, 
        fp.departure_airport_code,
        fp.destination_airport_code,
        fp.departure_airport_code || '-' || fp.destination_airport_code AS route_name,
        a1.airport_city AS departure_city,
        a2.airport_city AS destination_city,
        
        -- Agrégations
        COUNT(fp.flight_id) AS total_flights,
        SUM(fp.final_passenger_count) AS total_passengers,
        SUM(fp.calculated_rpm) AS total_rpm,
        SUM(fp.calculated_asm) AS total_asm,
        AVG(fp.distance_miles) AS avg_distance
    FROM flight_perf fp
    LEFT JOIN airports a1 ON fp.departure_airport_code = a1.airport_code
    LEFT JOIN airports a2 ON fp.destination_airport_code = a2.airport_code
    GROUP BY 1, 2, 3, 4, 5, 6
)

SELECT 
    *,
    CASE 
        WHEN total_asm > 0 
        THEN ROUND((total_rpm / total_asm)::numeric * 100, 2)
        ELSE 0 
    END AS route_load_factor_pct
FROM route_metrics