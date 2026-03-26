
---

# **Sales Analytics Project**

Here’s a **cleanly updated Project Overview** that incorporates your **continuous data pipeline using Streams, Tasks, and Stored Procedures** while keeping it professional and resume-ready:

---

## **Project Overview **

* Goal: Build a **Snowflake-based sales analytics data warehouse** with both **batch ingestion and continuous data processing**.
* Data: Customers, Products, Transactions (CSV files).
* Architecture: **Layered approach** → Raw → Staging → Production → Reporting.
* Enhancement: Implemented a **continuous data pipeline** using **Streams, Tasks, and Stored Procedures** for incremental data processing.
* Benefits:

  * Supports **real-time / near real-time data updates**
  * Eliminates full reloads with **incremental processing**
  * Ensures **automated, scalable, and low-latency pipelines**
  * Maintains **data consistency and auditability**
* Platform: **Snowflake**

---

### **Continuous Data Pipeline **

* Designed an **event-driven pipeline** to process newly ingested data automatically.

**Components:**

1. **Streams:**

   * Created streams on raw/staging tables to capture **change data (CDC)**.
   * Tracks inserts/updates without reprocessing entire datasets.

2. **Stored Procedure:**

   * Encapsulates transformation logic from **RAW → STAGING → PRODUCTION**.
   * Handles:

     * Incremental loads
     * Data validation
     * Error handling and logging

3. **Tasks:**

   * Scheduled tasks to **automatically trigger the stored procedure**.
   * Enables **continuous ingestion and transformation** without manual intervention.

---

### **Key Impact**

* Transitioned pipeline from **batch processing → incremental streaming pipeline**
* Improved performance by processing **only changed data**
* Achieved **automation with minimal operational overhead**
* Built a **production-grade ELT pipeline design** aligned with real-world data engineering practices

---

## **Layer 1 – RAW (Data Ingestion)**

**Purpose:** Land source data **as-is**, without transformations.

**Steps:**

1. **Warehouse:**

   * `ingest_wh` – lightweight, auto-suspend/resume for CSV loading.

2. **Database & Schemas:**

   * Database: `sales_analytics`
   * Schemas: `raw`, `staging`, `production`

3. **File Format:**

   * CSV (`csv_ff`) with headers, trimmed spaces, optional quotes, null handling.

4. **Stage:**

   * Internal stage `sales_csv_stage` for CSV files.
   * Upload: `customers.csv`, `transactions.csv`, `products.csv`.

5. **Raw Tables:**

   * `raw.customers` → customer info (all strings)
   * `raw.transactions` → transactions (all strings)
   * `raw.products` → product info (all strings)
   * **Load timestamp** added for traceability.

6. **Load Data (COPY INTO):**

   * Load CSV files into raw tables.
   * Preserve **source file info**.

**Key Point:**

* **No transformations or calculations**.
* Data is immutable and traceable.

---

## **Layer 2 – STAGING (Transformation & Clean Tables)**

**Purpose:** Clean, type-cast, and derive metrics. Prepare for analytics.

**Steps:**

1. **Staging Tables:**

   * `staging.customers` → type-cast IDs and dates
   * `staging.products` → type-cast IDs, unit_price
   * `staging.transactions` → type-cast IDs, quantity, unit_price, transaction_date

     * **Derived metric:** `total_amount = quantity × unit_price`

2. **Load Timestamp:**

   * Add `load_ts` to track staging load.

3. **Basic Validation:**

   * Check missing customer or product references.
   * Optional stored procedure: `staging.validate_data()`.

**Key Point:**

* Clean, typed tables ready for fact/dimension modeling.
* Derived metrics calculated in this layer.
* Prevents downstream errors in analytics.

---

## **Layer 3 – PRODUCTION (Fact & Dimension Modeling)**

**Purpose:** Build **analytics-ready tables** (star schema).

**Steps:**

1. **Fact Table – `fact_sales`:**

   * Grain: **one row per customer × product × transaction_date**
   * Columns: transaction_id, customer_id, product_id, quantity, unit_price, total_amount, customer_country, product_category, load_ts
   * Clustered by `(transaction_date, product_id)` for performance.

2. **Dimension Table – `dim_customers`:**

   * Columns: customer_id, customer_name, email, country, created_date
   * Derived metrics: lifetime_transactions, lifetime_value, last_purchase_date

3. **Optional Dimension – `dim_products`:**

   * Columns: product_id, product_name, category, supplier
   * Derived metric: avg_unit_price

**Key Point:**

* Optimized for BI queries and aggregation.
* Star schema: fact_sales joins dim_customers & dim_products.
* Supports customer-level and product-level analysis.

---

## **Layer 4 – REPORTING & BUSINESS VIEWS**

**Purpose:** Provide **ready-to-use analytics** for business / dashboards.

**Steps:**

1. **Views (Example):**

   * `vw_daily_sales` → daily revenue trends
   * `vw_top_products` → top-selling products
   * `vw_customer_segments` → customer segmentation / value

2. **Monitoring / Audit Table – `load_audit`:**

   * Tracks table/layer name, row count, load timestamp, success/failure
   * Ensures pipeline health and traceability.

**Key Point:**

* No raw data changes; read-only.
* Pre-aggregated metrics for fast BI access.
* Simplified, business-friendly columns.


---

##  Reporting Queries – Sample Outputs

The following screenshots demonstrate how **production fact and dimension tables**
are used to generate **business-ready reporting views** in Snowflake.

###  Daily Sales Revenue (`vw_daily_sales`)
<p align="center">
  <img src="image/daily_sales.png" width="750">
</p>

This view shows daily revenue trends aggregated from the `fact_sales` table,
supporting time-series analysis and KPI tracking.

---

###  Top Selling Products (`vw_top_products`)
<p align="center">
  <img src="image/top_products.png" width="750">
</p>

Highlights top-performing products based on total sales amount,
enabling product performance analysis and inventory planning.

---

###  Customer Segmentation (`vw_customer_segments`)
<p align="center">
  <img src="image/customer_segments.png" width="750">
</p>

Segments customers by purchasing behavior and lifetime value,
supporting targeted marketing and customer analytics.

---

## **Data Flow Summary**

| Step | Layer      | Action                           |
| ---- | ---------- | -------------------------------- |
| 1    | RAW        | Load CSV files into raw tables   |
| 2    | STAGING    | Parse, type-cast, derive metrics |
| 3    | PRODUCTION | Build fact & dimension tables    |
| 4    | REPORTING  | Create KPI views and audit table |

---

## **Relationships**

* Customers (1) → Transactions (many)
* Products (1) → Transactions (many)
* Supports: customer-level revenue, product performance, drill-down analysis.

---

## **Key Learning / Takeaways**

* Layered architecture ensures **clean, traceable data**
* RAW layer → preserves source data
* STAGING → typed and validated
* PRODUCTION → analytics-ready star schema
* REPORTING → business-friendly views + monitoring
* Derived metrics: `total_amount`, lifetime value, avg unit_price

---
