WITH source AS (
    SELECT * FROM {{ source('neon_raw', 'aircraft') }}
),

renamed AS (
    SELECT
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
    FROM source
)

SELECT * FROM renamed