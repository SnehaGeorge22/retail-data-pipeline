WITH source AS (
    SELECT * FROM {{ source('raw', 'stores') }}
),

cleaned AS (
    SELECT
        store_id,
        TRIM(store_name) AS store_name,
        TRIM(store_type) AS store_type,
        TRIM(city) AS city,
        TRIM(state) AS state,
        TRIM(country) AS country,
        opened_date,
        size_sqft,
        DATEDIFF('day', opened_date, CURRENT_DATE()) AS days_operational,
        loaded_at
    FROM source
    WHERE store_id IS NOT NULL
)

SELECT * FROM cleaned