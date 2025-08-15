WITH level_1 AS (
SELECT 
    GET(PARSE_JSON(RAW_GEOJSON), 'type') AS v_type,
    GET(PARSE_JSON(RAW_GEOJSON), 'features') AS v_features
FROM {{ source("uploaded_geo_data", "nz_geo_data") }}
)

SELECT 
    f.value:id AS id, 
    REPLACE(f.value:type, '"', '') AS type, 
    f.value:properties AS properties, 
    f.value:geometry AS geometry
FROM level_1,
LATERAL FLATTEN(INPUT => level_1.v_features) f