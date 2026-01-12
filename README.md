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