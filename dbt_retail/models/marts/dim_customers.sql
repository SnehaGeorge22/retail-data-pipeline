WITH customers AS (
    SELECT * FROM {{ ref('stg_customers') }}
),

customer_metrics AS (
    SELECT
        customer_id,
        COUNT(DISTINCT transaction_id) AS lifetime_transactions,
        SUM(total_amount) AS lifetime_value,
        MIN(transaction_date) AS first_purchase_date,
        MAX(transaction_date) AS last_purchase_date,
        DATEDIFF('day', MIN(transaction_date), MAX(transaction_date)) AS customer_tenure_days
    FROM {{ ref('stg_transactions') }}
    GROUP BY customer_id
)

SELECT
    c.customer_id,
    c.first_name,
    c.last_name,
    c.email,
    c.phone,
    c.city,
    c.state,
    c.zip_code,
    c.signup_date,
    c.customer_segment,
    c.loyalty_member,
    COALESCE(cm.lifetime_transactions, 0) AS lifetime_transactions,
    COALESCE(cm.lifetime_value, 0) AS lifetime_value,
    cm.first_purchase_date,
    cm.last_purchase_date,
    COALESCE(cm.customer_tenure_days, 0) AS customer_tenure_days,
    CASE
        WHEN cm.lifetime_value >= 5000 THEN 'VIP'
        WHEN cm.lifetime_value >= 2000 THEN 'High Value'
        WHEN cm.lifetime_value >= 500 THEN 'Medium Value'
        ELSE 'Low Value'
    END AS value_tier,
    CURRENT_TIMESTAMP() AS updated_at
FROM customers c
LEFT JOIN customer_metrics cm ON c.customer_id = cm.customer_id