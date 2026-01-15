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
    
    flight_stats AS (
        SELECT
            *
        FROM passengers p
        LEFT JOIN passenger_flights pf ON p.passenger_id = pf.passenger_id
        LEFT JOIN flight_performance fp ON pf.flight_id = fp.flight_id
    )

SELECT 
    *
FROM flight_stats