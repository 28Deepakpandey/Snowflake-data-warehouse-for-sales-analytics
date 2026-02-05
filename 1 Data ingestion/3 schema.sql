USE WAREHOUSE ingest_wh;
USE DATABASE sales_analytics;

-- raw.customers
CREATE OR REPLACE TABLE raw.customers (
    customer_id   NUMBER,            -- numeric ID
    customer_name STRING,
    email         STRING,
    country       STRING,
    created_date  DATE,              -- store as DATE
    source_file   STRING,
    load_ts       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP
);




-- raw.transactions
CREATE OR REPLACE TABLE raw.transactions (
    transaction_id    NUMBER,       -- numeric ID
    customer_id       NUMBER,       -- numeric ID
    product_id        NUMBER,       -- numeric ID
    quantity          NUMBER,       -- numeric
    unit_price        NUMBER(10,2), -- numeric
    total_amount      NUMBER(18,2), -- numeric
    transaction_date  DATE,         -- DATE type
    source_file       STRING,
    load_ts           TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP
);



-- raw.products
CREATE OR REPLACE TABLE raw.products (
    product_id    NUMBER,           -- numeric ID
    product_name  STRING,
    category      STRING,
    supplier      STRING,
    unit_price    NUMBER(10,2),     -- numeric price
    source_file   STRING,
    load_ts       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP
);
