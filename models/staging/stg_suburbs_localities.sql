WITH suburbs AS (
    SELECT * FROM {{ source("uploaded_geo_data", "nz_suburbs_localities") }}
)
SELECT
    WKT, id, parent_id, name, type, start_date, name_ascii
FROM suburbs