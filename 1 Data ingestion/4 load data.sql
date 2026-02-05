USE WAREHOUSE ingest_wh;
USE DATABASE sales_analytics;


COPY INTO raw.customers
FROM (
    SELECT
        $1, $2, $3, $4, $5,
        METADATA$FILENAME, CURRENT_TIMESTAMP
    FROM @sales_csv_stage/customers.csv
)
FILE_FORMAT = csv_ff;


-- Load Transactions
COPY INTO raw.transactions
FROM (
    SELECT
        $1, $2, $3, $4, $5, $6,$7,
        METADATA$FILENAME, CURRENT_TIMESTAMP
    FROM @sales_csv_stage/transactions.csv
)
FILE_FORMAT = csv_ff;
-- transaction_id,customer_id,product_id,quantity,unit_price,total_amount,transaction_date,source_file

-- Load Products
COPY INTO raw.products
FROM (
    SELECT
        $1, $2, $3, $4, $5,
        METADATA$FILENAME, CURRENT_TIMESTAMP
    FROM @sales_csv_stage/products.csv
)
FILE_FORMAT = csv_ff;

-- Quick Validation
SELECT COUNT(*) FROM raw.customers;
SELECT * FROM raw.transactions;
SELECT COUNT(*) FROM raw.products;

SELECT * FROM raw.transactions LIMIT 5;
