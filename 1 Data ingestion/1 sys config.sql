
-- 1. Warehouse (Ingestion Compute)
CREATE OR REPLACE WAREHOUSE ingest_wh
WITH
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_RESUME = TRUE;

USE WAREHOUSE ingest_wh;


-- 2. Database & Schemas
CREATE OR REPLACE DATABASE sales_analytics;

CREATE OR REPLACE SCHEMA sales_analytics.raw;
CREATE OR REPLACE SCHEMA sales_analytics.staging;
CREATE OR REPLACE SCHEMA sales_analytics.production;


USE WAREHOUSE ingest_wh;
USE DATABASE sales_analytics;
