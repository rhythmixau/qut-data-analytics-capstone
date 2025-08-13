WITH locations AS (
    SELECT 
        DISTINCT
        latitude,
        longitude
    FROM {{ ref('stg_trap_data') }}
)
SELECT
    ROW_NUMBER() OVER() AS location_id,
    latitude,
    longitude
FROM locations