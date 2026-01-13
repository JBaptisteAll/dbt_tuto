WITH source AS (
    SELECT * FROM {{ source('neon_raw', 'airports') }}
),

renamed AS (
    SELECT
        airport_code,
        airport_name,
        city AS airport_city,
        country AS airport_country,
        airport_employees,
        airport_size_m2
    FROM source
)

SELECT * FROM renamed