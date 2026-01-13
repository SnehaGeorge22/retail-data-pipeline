WITH source AS (
    SELECT * FROM {{ source('raw', 'products') }}
),

cleaned AS (
    SELECT
        product_id,
        TRIM(product_name) AS product_name,
        TRIM(category) AS category,
        TRIM(subcategory) AS subcategory,
        TRIM(brand) AS brand,
        cost_price,
        retail_price,
        retail_price - cost_price AS profit_margin,
        ROUND((retail_price - cost_price) / NULLIF(retail_price, 0) * 100, 2) AS profit_margin_pct,
        supplier,
        created_date,
        loaded_at
    FROM source
    WHERE product_id IS NOT NULL
        AND retail_price > 0
)

SELECT * FROM cleaned