-- Un vol ne peut pas avoir plus de passagers que la capacité de l'avion
SELECT
    flight_id,
    final_passenger_count,
    passenger_capacity
FROM {{ ref('int_flights_performance') }}
WHERE final_passenger_count > passenger_capacity