-- 2. Dimension Table: dim_customers

USE WAREHOUSE ingest_wh;
USE DATABASE sales_analytics;
USE SCHEMA production;

CREATE OR REPLACE TABLE production.dim_customers AS
SELECT
    c.customer_id,
    c.customer_name,
    c.email,
    c.country,
    c.created_date,
    COUNT(t.transaction_id) AS lifetime_transactions,
    COALESCE(SUM(t.total_amount), 0) AS lifetime_value,
    MAX(t.transaction_date) AS last_purchase_date,
    CURRENT_TIMESTAMP AS load_ts
FROM staging.customers c
LEFT JOIN staging.transactions t
    ON TRIM(c.customer_id) = TRIM(t.customer_id)
GROUP BY
    c.customer_id, c.customer_name, c.email, c.country, c.created_date;


-- Explanation (simple):

-- One row per customer
-- Counts number of transactions (lifetime_transactions)
-- Sums total_amount to get lifetime value
-- Finds last purchase date
-- LEFT JOIN ensures even customers with no transactions appear

-- 2. Dimension Table: dim_products
CREATE OR REPLACE TABLE production.dim_products AS
SELECT
    p.product_id,
    p.product_name,
    p.category,
    p.supplier,
    COALESCE(AVG(t.unit_price), 0) AS avg_unit_price,
    CURRENT_TIMESTAMP AS load_ts
FROM staging.products p
LEFT JOIN staging.transactions t
    ON TRIM(p.product_id) = TRIM(t.product_id)
GROUP BY
    p.product_id, p.product_name, p.category, p.supplier;

-- Explanation (simple):

-- One row per product
-- Calculates average unit price from transactions
-- LEFT JOIN ensures all products appear, even if no sales
-- Ready for reporting and BI


-- Quick Checks:
-- Top customers by lifetime value
SELECT customer_id, lifetime_value
FROM production.dim_customers
ORDER BY lifetime_value DESC
LIMIT 10;

-- Top products by average price
SELECT product_id, avg_unit_price
FROM production.dim_products
ORDER BY avg_unit_price DESC
LIMIT 10;