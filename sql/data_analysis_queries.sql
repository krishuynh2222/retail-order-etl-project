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

-- Top 10 best-selling product 
SELECT 
	oi.product_id,
	p.product_category_name,
	COUNT(oi.order_id) AS total_orders,
	SUM(oi.price) AS total_revenue
FROM orderitems oi
JOIN products p ON oi.product_id = p.product_id
GROUP BY oi.product_id, p.product_category_name
ORDER BY total_orders DESC
LIMIT 10;

--  Top 5 Revenue-Generating Products
SELECT 
    p.product_category_name,
    SUM(oi.price) AS total_revenue
FROM orderitems oi
JOIN products p ON oi.product_id = p.product_id
GROUP BY p.product_category_name
ORDER BY total_revenue DESC
LIMIT 5;

-- Customer Orders by State
SELECT 
	c.customer_state,
	COUNT(o.order_id) AS total_orders
FROM orders o 
JOIN customers c ON o.customer_id = c.customer_id
GROUP BY c.customer_state
ORDER BY total_orders DESC;

-- Payment Method Usage
SELECT 
	payment_type,
	COUNT(*) AS payment_count,
	SUM(payment_value) AS total_payment_value
FROM payments
GROUP BY payment_type
ORDER BY payment_count DESC;

--  Delivery Performance (Delays)
SELECT
	order_id,
	order_purchase_timestamp,
	order_delivered_timestamp,
	DATE_PART('day', order_delivered_timestamp - order_purchase_timestamp) AS delivery_Days
FROM orders
WHERE order_delivered_timestamp IS NOT NULL
ORDER BY delivery_days DESC;
