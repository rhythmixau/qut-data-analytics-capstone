WITH traps AS (
    SELECT
        *
    FROM {{ source("gcp_qut_traps_data", "raw_traps")}}
)
SELECT
    id,
    `when` AS uploaded_at,
    activityType AS activity_type,
    strikeTime AS strike_at,
    batchId AS batch_id,
    createdBy AS created_by,
    trapId AS trap_id,
    latitude,
    longitude,
    batteryLevel AS battery_level,
    hasImage AS has_image,
    trapInstalledAt AS installed_at
FROM traps        