WITH source AS (
    SELECT * FROM {{ source('neon_raw', 'airlines') }}
),

renamed AS (
    SELECT
        airline_code,
        airline_name,
        headquarters AS airline_headquarter,
        date_founded,
        market_cap,
        employees AS airline_employees
    FROM source
)

SELECT * FROM renamed