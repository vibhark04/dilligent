WITH monthly_orders AS (
    SELECT
        strftime('%Y-%m', order_date) AS order_month,
        COUNT(*) AS total_orders,
        SUM(total_amount) AS revenue
    FROM orders
    GROUP BY order_month
)
SELECT
    order_month,
    total_orders,
    revenue,
    ROUND(revenue / NULLIF(total_orders, 0), 2) AS avg_order_value
FROM monthly_orders
ORDER BY order_month;

