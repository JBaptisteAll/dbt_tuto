WITH passenger_stats AS (
    SELECT 
        p.*,
        SUM(fp.distance_miles) AS total_distance
    FROM {{ ref('int_passengers_enriched') }} p
    LEFT JOIN {{ ref('stg_passenger_flights') }} pf ON p.passenger_id = pf.passenger_id
    LEFT JOIN {{ ref('int_flights_performance') }} fp ON pf.flight_id = fp.flight_id
    GROUP BY 1, 2, 3, 4 -- Ajusté selon les colonnes de ton intermediate enrichi
)

SELECT 
    *,
    CASE 
        WHEN total_distance > 20000 THEN 'Platinum'
        WHEN total_distance > 10000 THEN 'Gold'
        WHEN total_distance > 5000 THEN 'Silver'
        ELSE 'Bronze'
    END AS loyalty_tier
FROM passenger_stats
ORDER BY total_distance DESC NULLS LAST