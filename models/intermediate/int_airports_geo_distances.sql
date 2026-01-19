WITH airport_coords AS (
    SELECT 
        a.airport_code,
        c.latitude,
        c.longitude
    FROM {{ ref('stg_airports') }} a
    JOIN {{ ref('airports_coordinates') }} c ON a.airport_code = c.airport_code
),

-- On génère toutes les routes uniques présentes dans tes vols
distinct_routes AS (
    SELECT DISTINCT 
        departure_airport_code, 
        destination_airport_code
    FROM {{ ref('stg_flights') }}
)

SELECT 
    r.departure_airport_code,
    r.destination_airport_code,
    -- La formule Haversine centralisée ici
    3959 * acos(
        least(1.0, 
            cos(radians(orig.latitude)) * cos(radians(dest.latitude)) * cos(radians(dest.longitude) - radians(orig.longitude)) + 
            sin(radians(orig.latitude)) * sin(radians(dest.latitude))
        )
    ) AS distance_miles
FROM distinct_routes r
JOIN airport_coords orig ON r.departure_airport_code = orig.airport_code
JOIN airport_coords dest ON r.destination_airport_code = dest.airport_code