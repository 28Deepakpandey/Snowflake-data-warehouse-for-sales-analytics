USE WAREHOUSE ingest_wh;
USE DATABASE sales_analytics;

-- 3. File Format (CSV)
CREATE OR REPLACE FILE FORMAT csv_ff
TYPE = 'CSV'
FIELD_DELIMITER = ','
SKIP_HEADER = 1
TRIM_SPACE = TRUE
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
NULL_IF = ('NULL', 'null', '');


CREATE OR REPLACE STAGE sales_csv_stage
FILE_FORMAT = csv_ff;


-- put data from ui to stage