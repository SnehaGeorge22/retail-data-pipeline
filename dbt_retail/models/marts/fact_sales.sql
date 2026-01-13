WITH transactions AS (
    SELECT * FROM {{ ref('stg_transactions') }}
),

enriched AS (
    SELECT
        t.transaction_id,
        t.transaction_date,
        t.transaction_time,
        t.day_type,
        t.time_of_day,
        t.store_id,
        t.customer_id,
        t.product_id,
        t.quantity,
        t.unit_price,
        t.discount_amount,
        t.total_amount,
        t.payment_method,
        p.category,
        p.subcategory,
        p.brand,
        p.cost_price,
        p.profit_margin,
        c.customer_segment,
        c.loyalty_member,
        s.store_type,
        s.city AS store_city,
        s.state AS store_state,
        t.quantity * p.cost_price AS total_cost,
        t.total_amount - (t.quantity * p.cost_price) AS gross_profit,
        YEAR(t.transaction_date) AS transaction_year,
        QUARTER(t.transaction_date) AS transaction_quarter,
        MONTH(t.transaction_date) AS transaction_month,
        DAYOFWEEK(t.transaction_date) AS day_of_week,
        DATE_TRUNC('week', t.transaction_date) AS transaction_week,
        DATE_TRUNC('month', t.transaction_date) AS transaction_month_start
    FROM transactions t
    LEFT JOIN {{ ref('stg_products') }} p ON t.product_id = p.product_id
    LEFT JOIN {{ ref('stg_customers') }} c ON t.customer_id = c.customer_id
    LEFT JOIN {{ ref('stg_stores') }} s ON t.store_id = s.store_id
)

SELECT * FROM enriched