/*
-- ============================================================================
-- Script Purpose: check_data_quality.sql
-- This script performs comprehensive data quality checks:
-- - Missing (NULL) values
-- - Duplicate primary keys
-- - Foreign key integrity
-- ============================================================================
*/

--  NULL VALUE CHECKS

-- Customers
SELECT 'customers' AS table, COUNT(*) AS null_rows
FROM customers
WHERE customer_id IS NULL OR customer_zip_code_prefix IS NULL
   OR customer_city IS NULL OR customer_state IS NULL;

-- Products
SELECT 'products' AS table, COUNT(*) AS null_rows
FROM products
WHERE product_id IS NULL OR product_category_name IS NULL;

-- Orders
SELECT 'orders' AS table, COUNT(*) AS null_rows
FROM orders
WHERE order_id IS NULL OR customer_id IS NULL;

-- OrderItems
SELECT 'orderitems' AS table, COUNT(*) AS null_rows
FROM orderitems
WHERE order_id IS NULL OR product_id IS NULL;

-- Payments
SELECT 'payments' AS table, COUNT(*) AS null_rows
FROM payments
WHERE order_id IS NULL OR payment_type IS NULL;

-- ============================================================================

--  DUPLICATE PRIMARY KEY CHECKS

-- Customers
SELECT 'customers' AS table, COUNT(*) AS duplicate_count
FROM (
    SELECT customer_id
    FROM customers
    GROUP BY customer_id
    HAVING COUNT(*) > 1
) AS dup;

-- Products
SELECT 'products', COUNT(*)
FROM (
    SELECT product_id
    FROM products
    GROUP BY product_id
    HAVING COUNT(*) > 1
) AS dup;

-- Orders
SELECT 'orders', COUNT(*)
FROM (
    SELECT order_id
    FROM orders
    GROUP BY order_id
    HAVING COUNT(*) > 1
) AS dup;

-- OrderItems (composite key)
SELECT 'orderitems', COUNT(*)
FROM (
    SELECT order_id, product_id
    FROM orderitems
    GROUP BY order_id, product_id
    HAVING COUNT(*) > 1
) AS dup;

-- Payments (composite key)
SELECT 'payments', COUNT(*)
FROM (
    SELECT order_id, payment_sequential
    FROM payments
    GROUP BY order_id, payment_sequential
    HAVING COUNT(*) > 1
) AS dup;

-- ============================================================================

--  FOREIGN KEY INTEGRITY CHECKS

-- Check Orders → Customers (FK: customer_id)
SELECT COUNT(*) AS missing_customers
FROM orders o
LEFT JOIN customers c ON o.customer_id = c.customer_id
WHERE c.customer_id IS NULL;

-- Check OrderItems → Orders (FK: order_id)
SELECT COUNT(*) AS missing_orders_in_orderitems
FROM orderitems oi
LEFT JOIN orders o ON oi.order_id = o.order_id
WHERE o.order_id IS NULL;

-- Check OrderItems → Products (FK: product_id)
SELECT COUNT(*) AS missing_products_in_orderitems
FROM orderitems oi
LEFT JOIN products p ON oi.product_id = p.product_id
WHERE p.product_id IS NULL;

-- Check Payments → Orders (FK: order_id)
SELECT COUNT(*) AS missing_orders_in_payments
FROM payments p
LEFT JOIN orders o ON p.order_id = o.order_id
WHERE o.order_id IS NULL;
