SELECT
    p.product_id,
    p.name,
    p.category,
    SUM(oi.quantity) AS units_sold,
    SUM(oi.line_total) AS revenue_generated
FROM products p
JOIN order_items oi ON oi.product_id = p.product_id
GROUP BY p.product_id
ORDER BY revenue_generated DESC
LIMIT 15;

