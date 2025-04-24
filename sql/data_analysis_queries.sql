/*
-- ============================================================================
-- Script Purpose: data_analysis_queries.sql
-- This script contains analytical queries to explore the retail order dataset:
-- - Top products
-- - Regional sales
-- - Delivery speed
-- - Payment behavior
-- ============================================================================
*/

-- Top 10 best-selling products
SELECT product_id, COUNT(*) AS total_sold
FROM orderitems
GROUP BY product_id
ORDER BY total_sold DESC
LIMIT 10;

-- Top 5 Revenue-Generating Products
SELECT product_id, ROUND(SUM(price), 2) AS total_revenue
FROM orderitems
GROUP BY product_id
ORDER BY total_revenue DESC
LIMIT 5;

-- Sales by state
SELECT customer_state, COUNT(*) AS total_orders
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
GROUP BY customer_state
ORDER BY total_orders DESC;

-- Product Category Performance
SELECT product_category_name, COUNT(*) AS items_sold, ROUND(SUM(price), 2) AS revenue
FROM orderitems oi
JOIN products p ON oi.product_id = p.product_id
GROUP BY product_category_name
ORDER BY revenue DESC
LIMIT 10;

--Payment Method Usage
SELECT payment_type, COUNT(*) AS num_payments, SUM(payment_value) AS total_paid
FROM payments
GROUP BY payment_type
ORDER BY total_paid DESC;
