WITH stores AS (
    SELECT * FROM {{ ref('stg_stores') }}
),

store_performance AS (
    SELECT
        store_id,
        COUNT(DISTINCT transaction_id) AS total_transactions,
        COUNT(DISTINCT customer_id) AS unique_customers,
        SUM(total_amount) AS total_revenue
    FROM {{ ref('stg_transactions') }}
    GROUP BY store_id
)

SELECT
    s.store_id,
    s.store_name,
    s.store_type,
    s.city,
    s.state,
    s.country,
    s.opened_date,
    s.size_sqft,
    s.days_operational,
    COALESCE(sp.total_transactions, 0) AS total_transactions,
    COALESCE(sp.unique_customers, 0) AS unique_customers,
    COALESCE(sp.total_revenue, 0) AS total_revenue,
    CASE
        WHEN sp.total_revenue >= 100000 THEN 'High Performing'
        WHEN sp.total_revenue >= 50000 THEN 'Medium Performing'
        ELSE 'Low Performing'
    END AS performance_tier,
    CURRENT_TIMESTAMP() AS updated_at
FROM stores s
LEFT JOIN store_performance sp ON s.store_id = sp.store_id