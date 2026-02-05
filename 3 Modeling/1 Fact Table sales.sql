USE WAREHOUSE ingest_wh;
USE DATABASE sales_analytics;
USE SCHEMA production;


-- Grain: one row per customer × product × transaction_date

CREATE OR REPLACE TABLE production.fact_sales AS
SELECT
    t.transaction_id,
    t.customer_id,
    t.product_id,
    t.transaction_date,
    t.quantity,
    t.unit_price,
    t.total_amount,
    c.country AS customer_country,
    p.category AS product_category,
    CURRENT_TIMESTAMP AS load_ts
FROM staging.transactions t
LEFT JOIN staging.customers c
    ON TRIM(t.customer_id) = TRIM(c.customer_id)
LEFT JOIN staging.products p
    ON TRIM(t.product_id) = TRIM(p.product_id);

-- Cluster Keys for Performance
ALTER TABLE production.fact_sales
CLUSTER BY (transaction_date, product_id);



-- Total number of transactions loaded
SELECT COUNT(*) FROM production.fact_sales;

-- Check a few transactions
SELECT *
FROM production.fact_sales
LIMIT 10;

-- Total sales by product
SELECT product_id, SUM(total_amount) AS total_sales
FROM production.fact_sales
GROUP BY product_id
ORDER BY total_sales DESC
LIMIT 10;




-- We are creating a fact table of all sales transactions:

-- Each row = one transaction (customer × product × date)

-- Includes sales metrics (quantity, unit_price, total_amount)

-- Adds customer country and product category from staging tables

-- Uses LEFT JOIN so no transactions are lost

-- TRIM() fixes extra spaces in CSV IDs

-- CLUSTER BY helps queries run faster