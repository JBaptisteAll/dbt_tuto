{{ config(materialized='table') }}

WITH passenger_flights AS (
    SELECT * FROM {{ ref('stg_passenger_flights') }}
),

passengers AS (
    SELECT * FROM {{ ref('stg_passenger') }}
),

flight_performance AS (
    SELECT * FROM {{ ref('int_flights_performance') }}
),

customer_stats AS (
    SELECT
        p.passenger_id,
        p.first_name || ' ' || p.last_name AS customer_name,
        p.nationality,
        COUNT(pf.flight_id) AS total_flights_taken,
        -- On utilise ici tes calculs de distance et de RPM de la couche Silver
        ROUND(SUM(fp.distance_miles)::numeric, 2) AS total_distance_traveled_miles,
        ROUND(AVG(fp.load_factor_pct)::numeric, 2) AS avg_flight_fill_rate
    FROM passengers p
    LEFT JOIN passenger_flights pf ON p.passenger_id = pf.passenger_id
    LEFT JOIN flight_performance fp ON pf.flight_id = fp.flight_id
    GROUP BY 1, 2, 3
)

SELECT 
    *,
    -- On crée une segmentation simple pour le marketing
    CASE 
        WHEN total_distance_traveled_miles > 20000 THEN 'Platinum'
        WHEN total_distance_traveled_miles > 10000 THEN 'Gold'
        WHEN total_distance_traveled_miles > 5000 THEN 'Silver'
        ELSE 'Bronze'
    END AS loyalty_tier
FROM customer_stats
ORDER BY total_distance_traveled_miles DESC NULLS LAST