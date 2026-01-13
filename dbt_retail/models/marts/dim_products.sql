WITH products AS (
    SELECT * FROM {{ ref('stg_products') }}
),

product_performance AS (
    SELECT
        product_id,
        COUNT(DISTINCT transaction_id) AS total_transactions,
        SUM(quantity) AS total_units_sold,
        SUM(total_amount) AS total_revenue,
        AVG(unit_price) AS avg_selling_price
    FROM {{ ref('stg_transactions') }}
    GROUP BY product_id
)

SELECT
    p.product_id,
    p.product_name,
    p.category,
    p.subcategory,
    p.brand,
    p.cost_price,
    p.retail_price,
    p.profit_margin,
    p.profit_margin_pct,
    p.supplier,
    p.created_date,
    COALESCE(pp.total_transactions, 0) AS total_transactions,
    COALESCE(pp.total_units_sold, 0) AS total_units_sold,
    COALESCE(pp.total_revenue, 0) AS total_revenue,
    COALESCE(pp.avg_selling_price, 0) AS avg_selling_price,
    CASE
        WHEN pp.total_units_sold >= 100 THEN 'Best Seller'
        WHEN pp.total_units_sold >= 50 THEN 'Popular'
        WHEN pp.total_units_sold >= 10 THEN 'Average'
        ELSE 'Slow Moving'
    END AS performance_category,
    CURRENT_TIMESTAMP() AS updated_at
FROM products p
LEFT JOIN product_performance pp ON p.product_id = pp.product_id