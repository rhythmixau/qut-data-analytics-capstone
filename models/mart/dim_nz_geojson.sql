WITH geo_data AS (
    SELECT * FROM {{ ref("stg_nz_geojson") }}
)
SELECT 
    DISTINCT
    REPLACE(GET(properties, 'id'), '"', '') AS id,
    REPLACE(GET(properties, 'name'), '"', '') AS suburb_locality,
    REPLACE(GET(properties, 'additional_name'), '"', '') AS additional_name,
    REPLACE(GET(properties, 'territorial_authority'), '"', '') AS territorial_authority,
    REPLACE(GET(properties, 'type'), '"', '') AS type
FROM geo_data
