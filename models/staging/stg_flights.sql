WITH source AS (
    SELECT * FROM {{ source('neon_raw', 'flights') }}
),

renamed AS (
    SELECT
        flight_id,
        departure_time,
        arrival_time,
        delay_minutes,
        departure_airport_code,
        destination_airport_code,
        flight_status,
        passenger_count,
        revenue AS flight_revenue,
        airline_code,
        aircraft_id
    FROM source
)

SELECT * FROM renamed