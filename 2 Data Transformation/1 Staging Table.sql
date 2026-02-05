USE WAREHOUSE ingest_wh;
USE DATABASE sales_analytics;



-- 1. Staging Table: Customers
CREATE OR REPLACE TABLE staging.customers AS
SELECT
    TRY_TO_NUMBER(customer_id)      AS customer_id,
    customer_name,
    email,
    country,
    created_date AS created_date,
    CURRENT_TIMESTAMP AS load_ts
FROM raw.customers;


-- Notes:
-- TRY_TO_NUMBER avoids failures on bad IDs
-- TRY_TO_DATE safely parses dates
-- load_ts tracks when data entered staging






-- 2. Staging Table: Products
CREATE OR REPLACE TABLE staging.products AS
SELECT
    TRY_TO_NUMBER(product_id)    AS product_id,
    product_name,
    category,
    supplier,
    unit_price    AS unit_price,
    CURRENT_TIMESTAMP AS load_ts
FROM raw.products;


-- Notes:
-- Keep unit_price as number for calculations
-- No joins yet — staging is flat and clean



-- 3. Staging Table: Transactions
CREATE OR REPLACE TABLE staging.transactions AS
SELECT
    transaction_id          AS transaction_id,
    customer_id             AS customer_id,
    product_id              AS product_id,
    quantity                AS quantity,
    unit_price              AS unit_price,
    transaction_date        AS transaction_date,
    quantity * unit_price   AS total_amount,
    CURRENT_TIMESTAMP       AS load_ts
FROM raw.transactions;


-- Notes:
-- Derived metric: total_amount = quantity × unit_price
-- All numeric conversions are safe with TRY_TO_NUMBER
-- transaction_date typed for future aggregation





--:::::::::::::::: Quick Quality Checks
-- Total rows per table
SELECT 'customers', COUNT(*) FROM staging.customers;
SELECT 'products', COUNT(*) FROM staging.products;
SELECT 'transactions', COUNT(*) FROM staging.transactions;

-- Check for negative totals
SELECT * FROM staging.transactions WHERE total_amount < 0;



SELECT * FROM staging.customers limit 2;
SELECT * FROM staging.products limit 2;
SELECT * FROM staging.transactions limit 2;