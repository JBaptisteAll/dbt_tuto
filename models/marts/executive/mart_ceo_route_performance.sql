WITH route_analysis AS (
    SELECT 
        al.airline_name,
        rp.route_name,
        rp.departure_city,
        rp.destination_city,
        rp.total_passengers,
        rp.route_load_factor_pct,
        al.avg_airline_load_factor_pct,
        (rp.route_load_factor_pct - al.avg_airline_load_factor_pct) AS performance_vs_avg
    FROM {{ ref('mart_route_performance') }} rp
    -- JOINTURE SUR LE CODE ICI
    JOIN {{ ref('mart_airline_performance') }} al ON rp.airline_code = al.airline_code
)

SELECT 
    airline_name,
    route_name,
    departure_city || ' ✈️ ' || destination_city AS trip,
    total_passengers,
    route_load_factor_pct || '%' AS load_factor,
    CASE 
        WHEN performance_vs_avg > 10 THEN '💎 Route Étoile'
        WHEN performance_vs_avg < 0 THEN '⚠️ Sous-performance'
        ELSE '✅ Stable'
    END AS status,
    CASE 
        WHEN route_load_factor_pct >= 75 AND total_passengers > 200 THEN TRUE 
        ELSE FALSE 
    END AS is_priority_route
FROM route_analysis
ORDER BY airline_name, route_load_factor_pct DESC