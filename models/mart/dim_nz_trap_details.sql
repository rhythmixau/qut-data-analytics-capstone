WITH trap_details AS (
    SELECT
        LOCATION_ID,
        FULL_ADDRESS,
        DISTANCE_IN_METERS, -- distance from the trap geo location to the address geo location 
        UPLOADED_AT,
        ACTIVITY_TYPE,
        STRIKE_AT,
        BATCH_ID,
        CREATED_BY,
        TRAP_ID,
        LATITUDE,
        LONGITUDE,
        BATTERY_LEVEL,
        INSTALLED_AT
    FROM
        {{ ref('int_trap_details') }}
),

nz_addresses AS (
    SELECT
        TRAP_ADDRESS_ID,
        FULL_ADDRESS,
        ADDRESS_NUMBER,
        FULL_ROAD_NAME,
        TOWN_CITY,
        TERRITORIAL_AUTHORITY
    FROM {{ ref('dim_nz_trap_addresses') }}
)

SELECT 
    t.LOCATION_ID AS TRAP_LOCATION_ID,
    n.TRAP_ADDRESS_ID,
    t.UPLOADED_AT,
    t.ACTIVITY_TYPE,
    t.STRIKE_AT,
    t.BATCH_ID,
    t.CREATED_BY,
    t.TRAP_ID,
    t.LATITUDE,
    t.LONGITUDE,
    t.BATTERY_LEVEL,
    t.INSTALLED_AT,
    t.DISTANCE_IN_METERS
FROM trap_details t
JOIN nz_addresses n ON n.FULL_ADDRESS = T.FULL_ADDRESS
ORDER BY t.LOCATION_ID
