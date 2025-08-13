WITH trap_records AS (
    SELECT * FROM {{ ref('dim_nz_trap_details_with_expanded_time') }}
    WHERE ACTIVITY_TYPE = 'STRIKE'
), 

addresses AS (
    SELECT * FROM {{ ref('dim_nz_trap_addresses') }}
),

daily_kills AS (
SELECT 
    TRAP_LOCATION_ID, 
    TRAP_ADDRESS_ID, 
    ACTIVITY_TYPE, 
    STRIKED_DATE, 
    COUNT(STRIKE_AT) AS NUM_KILLS,
    TRAP_ID,
    CREATED_BY
FROM trap_records   
GROUP BY TRAP_LOCATION_ID, TRAP_ADDRESS_ID, ACTIVITY_TYPE, STRIKED_DATE, TRAP_ID, CREATED_BY
)

SELECT
    a.TRAP_ADDRESS_ID,
    a.FULL_ADDRESS,
    a.FULL_ROAD_NAME,
    a.TOWN_CITY,
    a.TERRITORIAL_AUTHORITY,
    k.TRAP_LOCATION_ID,
    k.ACTIVITY_TYPE,
    k.STRIKED_DATE,
    k.NUM_KILLS,
    k.TRAP_ID,
    k.CREATED_BY
FROM daily_kills k 
JOIN addresses a ON a.TRAP_ADDRESS_ID = k.TRAP_ADDRESS_ID
