/*
-- 🎯 Script Purpose: create_tables.sql
========================================================================================
-- This script defines the SQL schema for the retail order data warehouse.
-- It creates five core tables: customers, products, orders, orderitems, and payments.
-- Each table includes primary keys, foreign keys, and appropriate data types.
========================================================================================
-- These tables represent the cleaned and transformed data and are
-- used for analytics, reporting, and business intelligence dashboards.
========================================================================================
-- Run this script using: execute directly in pgAdmin.
========================================================================================
-- WARNING: Ensure you are connected to the correct database before running.
========================================================================================
*/

-- 1. CUSTOMERS TABLE
CREATE TABLE customers (
    customer_id VARCHAR(50),
    customer_zip_code_prefix INT,
    customer_city VARCHAR(50),
    customer_state VARCHAR(20),
	PRIMARY KEY (customer_id)
);

-- 2. PRODUCTS TABLE
CREATE TABLE products (
    product_id VARCHAR(50),
    product_category_name VARCHAR(100),
    product_weight_g FLOAT,
    product_length_cm FLOAT,
    product_height_cm FLOAT,
    product_width_cm FLOAT,
	PRIMARY KEY (product_id)
);

-- 3. ORDERS TABLE
CREATE TABLE orders (
    order_id VARCHAR(50),
    customer_id VARCHAR(50),
    order_status VARCHAR(30),
    order_purchase_timestamp TIMESTAMP,
    order_approved_at TIMESTAMP,
    order_delivered_timestamp TIMESTAMP,
    order_estimated_delivery_date TIMESTAMP,
	PRIMARY KEY (order_id),
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);

-- 4. ORDERITEMS TABLE
CREATE TABLE orderitems (
    order_id VARCHAR(50),
    product_id VARCHAR(50),
    seller_id VARCHAR(50),
    price NUMERIC(10, 2),
    shipping_charges NUMERIC(10, 2),
    PRIMARY KEY (order_id, product_id),
    FOREIGN KEY (order_id) REFERENCES orders(order_id),
    FOREIGN KEY (product_id) REFERENCES products(product_id)
);

-- 5. PAYMENTS TABLE
CREATE TABLE payments (
    order_id VARCHAR(50),
    payment_sequential INT,
    payment_type VARCHAR(30),
    payment_installments INT,
    payment_value NUMERIC(10, 2),
    FOREIGN KEY (order_id) REFERENCES orders(order_id)
);
