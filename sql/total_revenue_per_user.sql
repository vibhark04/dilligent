SELECT
    u.user_id,
    u.first_name || ' ' || u.last_name AS user_name,
    COUNT(DISTINCT o.order_id) AS total_orders,
    SUM(o.total_amount) AS total_revenue
FROM users u
LEFT JOIN orders o ON o.user_id = u.user_id
GROUP BY u.user_id
ORDER BY total_revenue DESC
LIMIT 20;

