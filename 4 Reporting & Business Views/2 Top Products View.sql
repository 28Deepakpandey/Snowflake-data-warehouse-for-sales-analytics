-- Notes:
-- Highlights top-performing products
-- Useful for inventory and marketing decisions

USE WAREHOUSE ingest_wh;
USE DATABASE sales_analytics;
USE SCHEMA production;

CREATE OR REPLACE VIEW production.vw_top_products AS
SELECT
    product_id,
    product_category,
    SUM(quantity) AS total_quantity_sold,
    SUM(total_amount) AS total_sales
FROM production.fact_sales
GROUP BY product_id, product_category
ORDER BY total_sales DESC
LIMIT 20;


-- Check top products
SELECT * FROM production.vw_top_products;
