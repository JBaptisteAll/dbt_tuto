# dbt_tuto


Give "dbt" acces to Neon DB
```sql
-- Donne l'accès au schéma public
GRANT USAGE ON SCHEMA public TO dbt_user;

-- Donne le droit de lecture sur toutes les tables actuelles
GRANT SELECT ON ALL TABLES IN SCHEMA public TO dbt_user;

-- (Optionnel mais recommandé) Donne les droits pour les futures tables
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO dbt_user;
```


After importing the table into dbt, I renamed some columns for a better understanding
```sql
WITH source AS (
    select * from {{ source('neon_raw', 'aircraft') }}
),

renamed AS (
    select
        aircraft_id,
        manufacturer AS aircraft_manufacturer,
        aircraft_type,
        year_built,
        engine_type,
        mass,
        length_meters,
        cost,
        capacity AS passenger_capacity,
        fuel_capacity_liters,
        burn_rate_liters_per_minute,
        aircraft_status
    from source
)

select * from renamed
```

- creation of a seed in dbt to record all lat and long of all airport and calculate distance for all routes

- Now that the bronze layer is done, start working on the silver layer with the first view
```sql
WITH flights AS (
    SELECT * FROM {{ ref('stg_flights') }}
),

-- On compte les billets vendus par vol
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

coords AS (
    SELECT * FROM {{ ref('airports_coordinates') }}
),

-- Jointure globale
joined AS (
    SELECT 
        f.flight_id,
        f.airline_code,
        f.departure_airport_code,
        f.destination_airport_code,
        a.passenger_capacity,
        -- On privilégie les billets réservés, sinon on prend le count de la table vol
        COALESCE(b.ticket_booked_count, 1) AS final_passenger_count,
        f.passenger_count AS operational_passenger_count,
        b.ticket_booked_count
    FROM flights f
    LEFT JOIN bookings b ON f.flight_id = b.flight_id
    LEFT JOIN aircraft a ON f.aircraft_id = a.aircraft_id
),

-- Calcul avec les coordonnées (Haversine)
distances AS (
    SELECT 
        j.*,
        3959 * acos(
            least(1.0, 
            cos(radians(orig.latitude)) * cos(radians(dest.latitude)) * cos(radians(dest.longitude) - radians(orig.longitude)) + 
            sin(radians(orig.latitude)) * sin(radians(dest.latitude))
            )
        ) AS distance_miles
    FROM joined j
    LEFT JOIN coords orig ON j.departure_airport_code = orig.airport_code
    LEFT JOIN coords dest ON j.destination_airport_code = dest.airport_code
)

SELECT 
    flight_id,
    airline_code,
    departure_airport_code,
    destination_airport_code,
    passenger_capacity,
    final_passenger_count,
    -- Arrondi de la distance
    ROUND(distance_miles::numeric, 2) AS distance_miles,
    -- Arrondi du RPM
    ROUND((final_passenger_count * distance_miles)::numeric, 2) AS calculated_rpm,
    -- Arrondi de l'ASM
    ROUND((passenger_capacity * distance_miles)::numeric, 2) AS calculated_asm,
    -- Arrondi du Load Factor (déjà présent mais sécurisé)
    ROUND((final_passenger_count::numeric / NULLIF(passenger_capacity, 0) * 100), 2) AS load_factor_pct
FROM distances
```

| Metric | Logic | Definition |  
|--------|-------|------------|
|Distance Miles|Haversine SQL|The calculated flight path distance in miles.|
|Calculated RPM|passengers * distance|"Revenue Passenger Miles: Measures the volume of ""paid"" transportation actually performed."|
Calculated ASM|seats * distance|Available Seat Miles: Measures the total carrying capacity available to generate revenue.|
Load Factor %|(RPM / ASM) * 100|Occupancy Rate: The primary efficiency KPI showing how well flight capacity is utilized.|

- Creating the .yml in seeds and intermediate file, to keep doc up to date and start testing when building my dbt model
