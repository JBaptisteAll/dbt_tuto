WITH flight_perf AS (
    SELECT * FROM {{ ref('int_flights_performance') }}
),

airlines AS (
    SELECT * FROM {{ ref('stg_airlines') }}
),

airline_metrics AS (
    SELECT
        fp.airline_code,
        al.airline_name,
        COUNT(fp.flight_id) AS total_flights,
        SUM(fp.final_passenger_count) AS total_passengers,
        SUM(fp.calculated_rpm) AS total_rpm,
        SUM(fp.calculated_asm) AS total_asm,
        -- Calcul du Load Factor moyen pondéré par compagnie
        ROUND(
            (SUM(fp.calculated_rpm) / NULLIF(SUM(fp.calculated_asm), 0) * 100)::numeric, 
            2
        ) AS avg_airline_load_factor_pct
    FROM flight_perf fp
    INNER JOIN airlines al ON fp.airline_code = al.airline_code
    GROUP BY 1, 2
)

SELECT * FROM airline_metrics
ORDER BY avg_airline_load_factor_pct DESC