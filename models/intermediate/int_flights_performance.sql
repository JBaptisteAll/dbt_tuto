WITH flights AS (
    SELECT * FROM {{ ref('stg_flights') }}
),

bookings AS (
    SELECT 
        flight_id, 
        COUNT(passenger_id) AS ticket_booked_count
    FROM {{ ref('stg_passenger_flights') }}
    GROUP BY 1
),

aircraft AS (
    SELECT * FROM {{ ref('stg_aircraft') }}
),

-- On récupère les distances déjà calculées ou les coordonnées depuis notre nouvel intermediate
geo AS (
    SELECT 
        departure_airport_code, 
        destination_airport_code,
        distance_miles 
    FROM {{ ref('int_airports_geo_distances') }} 
),

joined AS (
    SELECT 
        f.flight_id,
        f.airline_code,
        f.departure_airport_code,
        f.destination_airport_code,
        a.passenger_capacity,
        COALESCE(b.ticket_booked_count, 1) AS final_passenger_count,
        dist.distance_miles
    FROM flights f
    LEFT JOIN bookings b ON f.flight_id = b.flight_id
    LEFT JOIN aircraft a ON f.aircraft_id = a.aircraft_id
    -- On joint sur notre nouveau référentiel de distances
    LEFT JOIN {{ ref('int_airports_geo_distances') }} dist 
        ON f.departure_airport_code = dist.departure_airport_code 
        AND f.destination_airport_code = dist.destination_airport_code
)

SELECT 
    *,
    ROUND((final_passenger_count * distance_miles)::numeric, 2) AS calculated_rpm,
    ROUND((passenger_capacity * distance_miles)::numeric, 2) AS calculated_asm,
    ROUND((final_passenger_count::numeric / NULLIF(passenger_capacity, 0) * 100), 2) AS load_factor_pct
FROM joined