
USE WAREHOUSE ingest_wh;
USE DATABASE sales_analytics;
USE SCHEMA production;

CREATE OR REPLACE VIEW production.vw_customer_segments AS
SELECT
    customer_id,
    country,
    lifetime_transactions,
    lifetime_value,
    CASE
        WHEN lifetime_value >= 5000 THEN 'Platinum'
        WHEN lifetime_value >= 2000 THEN 'Gold'
        WHEN lifetime_value >= 500 THEN 'Silver'
        ELSE 'Bronze'
    END AS segment
FROM production.dim_customers;

-- Check customer segments
SELECT * FROM production.vw_customer_segments LIMIT 10;
