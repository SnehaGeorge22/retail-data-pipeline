WITH source AS (
    SELECT * FROM {{ source('raw', 'customers') }}
),

cleaned AS (
    SELECT
        customer_id,
        TRIM(first_name) AS first_name,
        TRIM(last_name) AS last_name,
        LOWER(TRIM(email)) AS email,
        phone,
        address,
        city,
        state,
        zip_code,
        signup_date,
        customer_segment,
        loyalty_member,
        loaded_at
    FROM source
    WHERE customer_id IS NOT NULL
)

SELECT * FROM cleaned