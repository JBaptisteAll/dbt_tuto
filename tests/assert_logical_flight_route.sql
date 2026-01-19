-- Le départ et l'arrivée doivent être différents
SELECT
    flight_id,
    departure_airport_code,
    destination_airport_code
FROM {{ ref('stg_flights') }}
WHERE departure_airport_code = destination_airport_code