WITH source AS (
    SELECT * FROM {{ source('raw', 'transactions') }}
),

cleaned AS (
    SELECT
        transaction_id,
        transaction_date,
        transaction_time,
        store_id,
        customer_id,
        product_id,
        quantity,
        unit_price,
        discount_amount,
        total_amount,
        payment_method,
        loaded_at,
        CASE 
            WHEN DAYOFWEEK(transaction_date) IN (1, 7) THEN 'Weekend'
            ELSE 'Weekday'
        END AS day_type,
        CASE
            WHEN HOUR(transaction_time) BETWEEN 6 AND 11 THEN 'Morning'
            WHEN HOUR(transaction_time) BETWEEN 12 AND 17 THEN 'Afternoon'
            WHEN HOUR(transaction_time) BETWEEN 18 AND 21 THEN 'Evening'
            ELSE 'Night'
        END AS time_of_day
    FROM source
    WHERE transaction_id IS NOT NULL
        AND transaction_date IS NOT NULL
        AND quantity > 0
        AND total_amount >= 0
)

SELECT * FROM cleaned