USE WAREHOUSE ingest_wh;
USE DATABASE sales_analytics;


-- Option 1: Return a JSON string from the procedure
CREATE OR REPLACE PROCEDURE staging.validate_data()
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
var result = {};

var sql_commands = [
  {name: 'missing_customers', sql: `
    SELECT COUNT(*) AS cnt
    FROM staging.transactions t
    WHERE t.customer_id IS NOT NULL
      AND NOT EXISTS (
          SELECT 1 FROM staging.customers c
          WHERE c.customer_id = t.customer_id
      )`
  },
  {name: 'missing_products', sql: `
    SELECT COUNT(*) AS cnt
    FROM staging.transactions t
    WHERE t.product_id IS NOT NULL
      AND NOT EXISTS (
          SELECT 1 FROM staging.products p
          WHERE p.product_id = t.product_id
      )`
  },
  {name: 'negative_quantity', sql: `
    SELECT COUNT(*) AS cnt
    FROM staging.transactions
    WHERE quantity < 0`
  },
  {name: 'negative_unit_price', sql: `
    SELECT COUNT(*) AS cnt
    FROM staging.transactions
    WHERE unit_price < 0`
  },
  {name: 'missing_transaction_date', sql: `
    SELECT COUNT(*) AS cnt
    FROM staging.transactions
    WHERE transaction_date IS NULL`
  }
];

for (var i = 0; i < sql_commands.length; i++) {
    var stmt = snowflake.createStatement({sqlText: sql_commands[i].sql});
    var rs = stmt.execute();
    rs.next();
    result[sql_commands[i].name] = rs.getColumnValue('CNT');
}

return JSON.stringify(result);
$$;




-- Why this matters:
-- Prevents downstream errors
-- Centralized validation logic
-- Easy to automate


CALL staging.validate_data();









-- Option 2: Use a separate SQL query instead of a procedure

-- If you just want a table of results, you don’t need a procedure at all — just run this SQL:

SELECT 'missing_customers' AS check_name,
       COUNT(*) AS missing_count
FROM staging.transactions t
WHERE t.customer_id IS NOT NULL
  AND NOT EXISTS (SELECT 1 FROM staging.customers c WHERE c.customer_id = t.customer_id)
UNION ALL
SELECT 'missing_products',
       COUNT(*)
FROM staging.transactions t
WHERE t.product_id IS NOT NULL
  AND NOT EXISTS (SELECT 1 FROM staging.products p WHERE p.product_id = t.product_id)
UNION ALL
SELECT 'negative_quantity',
       COUNT(*)
FROM staging.transactions
WHERE quantity < 0
UNION ALL
SELECT 'negative_unit_price',
       COUNT(*)
FROM staging.transactions
WHERE unit_price < 0
UNION ALL
SELECT 'missing_transaction_date',
       COUNT(*)
FROM staging.transactions
WHERE transaction_date IS NULL;