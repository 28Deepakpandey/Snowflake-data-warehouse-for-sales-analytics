USE WAREHOUSE ingest_wh;
USE DATABASE sales_analytics;
USE SCHEMA production;

CREATE OR REPLACE VIEW production.vw_daily_sales AS
SELECT
    transaction_date,
    SUM(quantity) AS total_quantity,
    SUM(total_amount) AS total_sales,
    COUNT(DISTINCT customer_id) AS unique_customers
FROM production.fact_sales
GROUP BY transaction_date
ORDER BY transaction_date;



-- Check daily sales trends
SELECT * FROM production.vw_daily_sales LIMIT 10;