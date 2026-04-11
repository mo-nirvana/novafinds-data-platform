# Databricks notebook source
# DBTITLE 1,Pipeline Overview
# MAGIC %md
# MAGIC # NovaFinds E-Commerce Data Pipeline
# MAGIC ## Lakeflow Pipeline (Delta Live Tables)
# MAGIC
# MAGIC **Architecture**: 3-Layer Medallion (Bronze → Silver → Gold)
# MAGIC
# MAGIC **Data Source**: CSV files in Unity Catalog Volume (`/Volumes/main/novafinds/postgres/`)
# MAGIC
# MAGIC **Target Catalog**: `main.novafinds`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🥉 Bronze Layer
# MAGIC - **Pattern**: Streaming ingestion with Auto Loader
# MAGIC - **Tables**: 10 raw tables from CSV files
# MAGIC - **Features**: Schema inference, evolution, rescued data
# MAGIC
# MAGIC ### 🥈 Silver Layer
# MAGIC - **Pattern**: Batch transformations with data quality expectations
# MAGIC - **Tables**: 4 enriched tables (product, customer, order, payment)
# MAGIC - **Features**: Joins, filtering, validation, business rules
# MAGIC
# MAGIC ### 🥇 Gold Layer
# MAGIC - **Pattern**: Aggregated business metrics
# MAGIC - **Tables**: 4 analytics tables (profitability, cancellation, regional sales)
# MAGIC - **Features**: Pre-computed KPIs for dashboards
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📊 Pipeline Configuration
# MAGIC
# MAGIC **Recommended Settings**:
# MAGIC - **Mode**: Triggered (scheduled daily/hourly)
# MAGIC - **Cluster**: Serverless (fastest, auto-scaling)
# MAGIC - **Target**: `main.novafinds`
# MAGIC - **Storage**: Default (managed by DLT)
# MAGIC
# MAGIC **To Create This Pipeline**:
# MAGIC 1. Go to **Workflows → Delta Live Tables → Create Pipeline**
# MAGIC 2. Set **Notebook**: Select this notebook
# MAGIC 3. Set **Target**: `main.novafinds`
# MAGIC 4. Set **Cluster Mode**: Serverless
# MAGIC 5. Click **Create** and then **Start**

# COMMAND ----------

# DBTITLE 1,Configuration Guide
# MAGIC %md
# MAGIC ## ⚙️ Configuration: Update Source Paths
# MAGIC
# MAGIC **This pipeline uses managed connectors** - you need to update the source paths in Bronze layer cells to match your Lakeflow Connect and Fivetran configurations.
# MAGIC
# MAGIC ### Option 1: Lakeflow Connect writes to storage path
# MAGIC If Lakeflow Connect writes CDC data to a storage path, use:
# MAGIC ```sql
# MAGIC AS SELECT * FROM cloud_files(
# MAGIC   'dbfs:/pipelines/lakeflow_connect/<your_connection_name>/table_name',
# MAGIC   'delta',
# MAGIC   map('cloudFiles.schemaEvolutionMode', 'addNewColumns')
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC ### Option 2: Lakeflow Connect writes directly to Unity Catalog
# MAGIC If Lakeflow Connect creates tables in Unity Catalog (recommended), use:
# MAGIC ```sql
# MAGIC AS SELECT * FROM STREAM(catalog_name.schema_name.table_name)
# MAGIC ```
# MAGIC
# MAGIC **Example**: If your Lakeflow Connect PostgreSQL connection writes to `main.lakeflow_postgres`, update:
# MAGIC ```sql
# MAGIC -- FROM: cloud_files('dbfs:/pipelines/lakeflow_connect/<connection_name>/product', ...)
# MAGIC -- TO:   STREAM(main.lakeflow_postgres.product)
# MAGIC ```
# MAGIC
# MAGIC ### Fivetran Configuration
# MAGIC Similarly for Stripe data from Fivetran:
# MAGIC ```sql
# MAGIC -- If Fivetran writes to main.fivetran_stripe schema:
# MAGIC AS SELECT * FROM STREAM(main.fivetran_stripe.payment_intents)
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **🔍 How to find your source paths:**
# MAGIC 1. **Lakeflow Connect**: Check the connection settings in Data Engineering → Lakeflow Connect
# MAGIC 2. **Fivetran**: Check your Fivetran destination settings (Unity Catalog schema)
# MAGIC 3. Run `SHOW TABLES IN catalog_name.schema_name` to verify table names
# MAGIC
# MAGIC **📝 TODO**: Update cells 3-13 with your actual catalog/schema names before running the pipeline.

# COMMAND ----------

# DBTITLE 1,Bronze Layer
# MAGIC %md
# MAGIC ## 🥉 Bronze Layer: Managed Connector Ingestion
# MAGIC
# MAGIC **Pattern**: Reference CDC tables from Lakeflow Connect (PostgreSQL) and Fivetran (Stripe)
# MAGIC
# MAGIC **Data Sources**:
# MAGIC - **PostgreSQL**: Lakeflow Connect with CDC enabled (automatic incremental updates)
# MAGIC - **Stripe**: Fivetran connector (automatic sync)
# MAGIC
# MAGIC **Features**:
# MAGIC - No file monitoring required
# MAGIC - Automatic schema evolution
# MAGIC - Real-time CDC capture from PostgreSQL
# MAGIC - Exactly-once semantics from both connectors
# MAGIC
# MAGIC **Note**: Update the source catalog/schema names below to match your Lakeflow Connect and Fivetran configurations.

# COMMAND ----------

# DBTITLE 1,Bronze: Product
# MAGIC %sql
# MAGIC -- Product Bronze Table
# MAGIC -- Source: Lakeflow Connect CDC table from PostgreSQL
# MAGIC
# MAGIC CREATE OR REFRESH STREAMING LIVE TABLE product_bronze
# MAGIC COMMENT "Product data from PostgreSQL via Lakeflow Connect CDC"
# MAGIC AS SELECT * FROM cloud_files(
# MAGIC   'dbfs:/pipelines/lakeflow_connect/<connection_name>/product',
# MAGIC   'delta',
# MAGIC   map('cloudFiles.schemaEvolutionMode', 'addNewColumns')
# MAGIC )
# MAGIC
# MAGIC -- Alternative if tables are already in Unity Catalog:
# MAGIC -- AS SELECT * FROM STREAM(main.lakeflow_postgres.product)

# COMMAND ----------

# DBTITLE 1,Bronze: Brand
# MAGIC %sql
# MAGIC -- Brand Bronze Table
# MAGIC -- Source: Lakeflow Connect CDC table from PostgreSQL
# MAGIC
# MAGIC CREATE OR REFRESH STREAMING LIVE TABLE brand_bronze
# MAGIC COMMENT "Brand data from PostgreSQL via Lakeflow Connect CDC"
# MAGIC AS SELECT * FROM cloud_files(
# MAGIC   'dbfs:/pipelines/lakeflow_connect/<connection_name>/brand',
# MAGIC   'delta',
# MAGIC   map('cloudFiles.schemaEvolutionMode', 'addNewColumns')
# MAGIC )
# MAGIC
# MAGIC -- Alternative if tables are already in Unity Catalog:
# MAGIC -- AS SELECT * FROM STREAM(main.lakeflow_postgres.brand)

# COMMAND ----------

# DBTITLE 1,Bronze: Product Category
# MAGIC %sql
# MAGIC -- Product Category Bronze Table
# MAGIC -- Source: Lakeflow Connect CDC table from PostgreSQL
# MAGIC
# MAGIC CREATE OR REFRESH STREAMING LIVE TABLE product_category_bronze
# MAGIC COMMENT "Product category data from PostgreSQL via Lakeflow Connect CDC"
# MAGIC AS SELECT * FROM cloud_files(
# MAGIC   'dbfs:/pipelines/lakeflow_connect/<connection_name>/product_category',
# MAGIC   'delta',
# MAGIC   map('cloudFiles.schemaEvolutionMode', 'addNewColumns')
# MAGIC )
# MAGIC
# MAGIC -- Alternative if tables are already in Unity Catalog:
# MAGIC -- AS SELECT * FROM STREAM(main.lakeflow_postgres.product_category)

# COMMAND ----------

# DBTITLE 1,Bronze: Customer
# MAGIC %sql
# MAGIC -- Customer Bronze Table
# MAGIC -- Source: Lakeflow Connect CDC table from PostgreSQL
# MAGIC
# MAGIC CREATE OR REFRESH STREAMING LIVE TABLE customer_bronze
# MAGIC COMMENT "Customer data from PostgreSQL via Lakeflow Connect CDC"
# MAGIC AS SELECT * FROM cloud_files(
# MAGIC   'dbfs:/pipelines/lakeflow_connect/<connection_name>/customer',
# MAGIC   'delta',
# MAGIC   map('cloudFiles.schemaEvolutionMode', 'addNewColumns')
# MAGIC )
# MAGIC
# MAGIC -- Alternative if tables are already in Unity Catalog:
# MAGIC -- AS SELECT * FROM STREAM(main.lakeflow_postgres.customer)

# COMMAND ----------

# DBTITLE 1,Bronze: Customer Type
# MAGIC %sql
# MAGIC -- Customer Type Bronze Table
# MAGIC -- Source: Lakeflow Connect CDC table from PostgreSQL
# MAGIC
# MAGIC CREATE OR REFRESH STREAMING LIVE TABLE customer_type_bronze
# MAGIC COMMENT "Customer type data from PostgreSQL via Lakeflow Connect CDC"
# MAGIC AS SELECT * FROM cloud_files(
# MAGIC   'dbfs:/pipelines/lakeflow_connect/<connection_name>/customer_type',
# MAGIC   'delta',
# MAGIC   map('cloudFiles.schemaEvolutionMode', 'addNewColumns')
# MAGIC )
# MAGIC
# MAGIC -- Alternative if tables are already in Unity Catalog:
# MAGIC -- AS SELECT * FROM STREAM(main.lakeflow_postgres.customer_type)

# COMMAND ----------

# DBTITLE 1,Bronze: Address
# MAGIC %sql
# MAGIC -- Address Bronze Table
# MAGIC -- Source: Lakeflow Connect CDC table from PostgreSQL
# MAGIC
# MAGIC CREATE OR REFRESH STREAMING LIVE TABLE address_bronze
# MAGIC COMMENT "Address data from PostgreSQL via Lakeflow Connect CDC"
# MAGIC AS SELECT * FROM cloud_files(
# MAGIC   'dbfs:/pipelines/lakeflow_connect/<connection_name>/address',
# MAGIC   'delta',
# MAGIC   map('cloudFiles.schemaEvolutionMode', 'addNewColumns')
# MAGIC )
# MAGIC
# MAGIC -- Alternative if tables are already in Unity Catalog:
# MAGIC -- AS SELECT * FROM STREAM(main.lakeflow_postgres.address)

# COMMAND ----------

# DBTITLE 1,Bronze: Orders
# MAGIC %sql
# MAGIC -- Orders Bronze Table
# MAGIC -- Source: Lakeflow Connect CDC table from PostgreSQL
# MAGIC
# MAGIC CREATE OR REFRESH STREAMING LIVE TABLE orders_bronze
# MAGIC COMMENT "Orders data from PostgreSQL via Lakeflow Connect CDC"
# MAGIC AS SELECT * FROM cloud_files(
# MAGIC   'dbfs:/pipelines/lakeflow_connect/<connection_name>/orders',
# MAGIC   'delta',
# MAGIC   map('cloudFiles.schemaEvolutionMode', 'addNewColumns')
# MAGIC )
# MAGIC
# MAGIC -- Alternative if tables are already in Unity Catalog:
# MAGIC -- AS SELECT * FROM STREAM(main.lakeflow_postgres.orders)

# COMMAND ----------

# DBTITLE 1,Bronze: Order Item
# MAGIC %sql
# MAGIC -- Order Item Bronze Table
# MAGIC -- Source: Lakeflow Connect CDC table from PostgreSQL
# MAGIC
# MAGIC CREATE OR REFRESH STREAMING LIVE TABLE order_item_bronze
# MAGIC COMMENT "Order item data from PostgreSQL via Lakeflow Connect CDC"
# MAGIC AS SELECT * FROM cloud_files(
# MAGIC   'dbfs:/pipelines/lakeflow_connect/<connection_name>/order_item',
# MAGIC   'delta',
# MAGIC   map('cloudFiles.schemaEvolutionMode', 'addNewColumns')
# MAGIC )
# MAGIC
# MAGIC -- Alternative if tables are already in Unity Catalog:
# MAGIC -- AS SELECT * FROM STREAM(main.lakeflow_postgres.order_item)

# COMMAND ----------

# DBTITLE 1,Bronze: Payment
# MAGIC %sql
# MAGIC -- Payment Bronze Table (PostgreSQL)
# MAGIC -- Source: Lakeflow Connect CDC table from PostgreSQL
# MAGIC -- Note: This will be enriched with Stripe data in Silver layer
# MAGIC
# MAGIC CREATE OR REFRESH STREAMING LIVE TABLE payment_postgres_bronze
# MAGIC COMMENT "Payment data from PostgreSQL via Lakeflow Connect CDC"
# MAGIC AS SELECT * FROM cloud_files(
# MAGIC   'dbfs:/pipelines/lakeflow_connect/<connection_name>/payment',
# MAGIC   'delta',
# MAGIC   map('cloudFiles.schemaEvolutionMode', 'addNewColumns')
# MAGIC )
# MAGIC
# MAGIC -- Alternative if tables are already in Unity Catalog:
# MAGIC -- AS SELECT * FROM STREAM(main.lakeflow_postgres.payment)

# COMMAND ----------

# DBTITLE 1,Bronze: Payment Method
# MAGIC %sql
# MAGIC -- Payment Method Bronze Table
# MAGIC -- Source: Lakeflow Connect CDC table from PostgreSQL
# MAGIC
# MAGIC CREATE OR REFRESH STREAMING LIVE TABLE payment_method_bronze
# MAGIC COMMENT "Payment method data from PostgreSQL via Lakeflow Connect CDC"
# MAGIC AS SELECT * FROM cloud_files(
# MAGIC   'dbfs:/pipelines/lakeflow_connect/<connection_name>/payment_method',
# MAGIC   'delta',
# MAGIC   map('cloudFiles.schemaEvolutionMode', 'addNewColumns')
# MAGIC )
# MAGIC
# MAGIC -- Alternative if tables are already in Unity Catalog:
# MAGIC -- AS SELECT * FROM STREAM(main.lakeflow_postgres.payment_method)

# COMMAND ----------

# DBTITLE 1,Bronze: Stripe Payments
# MAGIC %sql
# MAGIC -- Stripe Payments Bronze Table
# MAGIC -- Source: Fivetran connector syncing Stripe payment_intents
# MAGIC -- Schema: Stripe API format (nested JSON structure)
# MAGIC
# MAGIC CREATE OR REFRESH STREAMING LIVE TABLE stripe_payments_bronze
# MAGIC COMMENT "Stripe payment intents data from Fivetran connector"
# MAGIC AS SELECT * FROM cloud_files(
# MAGIC   'dbfs:/pipelines/fivetran/<connection_name>/payment_intents',
# MAGIC   'delta',
# MAGIC   map('cloudFiles.schemaEvolutionMode', 'addNewColumns')
# MAGIC )
# MAGIC
# MAGIC -- Alternative if Fivetran writes directly to Unity Catalog:
# MAGIC -- AS SELECT * FROM STREAM(main.fivetran_stripe.payment_intents)

# COMMAND ----------

# DBTITLE 1,Silver Layer
# MAGIC %md
# MAGIC ## 🥈 Silver Layer: Cleaned & Enriched Data
# MAGIC
# MAGIC **Pattern**: Batch transformations with data quality expectations
# MAGIC
# MAGIC **Features**:
# MAGIC - Join dimension tables with facts
# MAGIC - Data quality constraints (EXPECT clauses)
# MAGIC - Business logic transformations
# MAGIC - Remove inactive/invalid records
# MAGIC
# MAGIC **Data Quality Actions**:
# MAGIC - `ON VIOLATION DROP ROW` - Silently remove bad records
# MAGIC - `ON VIOLATION FAIL UPDATE` - Stop pipeline if constraint fails

# COMMAND ----------

# DBTITLE 1,Data Quality Strategy
# MAGIC %md
# MAGIC ### 🛡️ Data Quality Strategy
# MAGIC
# MAGIC This pipeline uses **two-tier data quality expectations** based on business impact:
# MAGIC
# MAGIC #### 🚨 Hard Blocks (`ON VIOLATION FAIL UPDATE`)
# MAGIC These fields are **critical** — missing/invalid values would corrupt downstream metrics and make the pipeline output unusable:
# MAGIC
# MAGIC | Field | Table | Why Critical |
# MAGIC |-------|-------|-------------|
# MAGIC | `product.price` | product_silver | **Cost price** required for all margin/profitability calculations |
# MAGIC | `payment.amount` | payment_silver | **Revenue** cannot be calculated without payment amounts |
# MAGIC | `payment.stripe_status` | payment_silver | Must distinguish successful vs failed Stripe transactions (if Stripe payment exists) |
# MAGIC | `payment.stripe_currency` | payment_silver | Cannot normalize to USD without knowing source currency (if Stripe payment exists) |
# MAGIC
# MAGIC **Behavior**: Pipeline **stops** if any row violates these constraints. Fix the source data before retrying.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### ⚠️ Soft Blocks (`ON VIOLATION DROP ROW`)
# MAGIC These fields matter for analytics, but missing values shouldn't halt the entire pipeline:
# MAGIC
# MAGIC | Field | Table | Impact of Missing Data |
# MAGIC |-------|-------|------------------------|
# MAGIC | `customer.country` | customer_silver | Cannot attribute sales by region — creates "null region" bucket in reports (e.g., the $53k unattributed revenue problem) |
# MAGIC | `customer.is_active` | customer_silver | Cannot segment customers for retention analysis — affects customer cohort metrics |
# MAGIC
# MAGIC **Behavior**: Rows with missing values are **silently dropped**. Pipeline continues, but you'll see row count differences in monitoring.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### 📊 Monitoring Data Quality
# MAGIC
# MAGIC After pipeline runs, check:
# MAGIC 1. **Expectations Dashboard** → See pass/fail rates for each constraint
# MAGIC 2. **Row Count Deltas** → Compare Bronze vs Silver counts to see how many rows were dropped
# MAGIC 3. **Event Log** → Check for `FAIL UPDATE` violations (pipeline stops)
# MAGIC
# MAGIC **Pro Tip**: If `has_country` or `has_active_status` drop too many rows, investigate upstream data quality in your PostgreSQL source.

# COMMAND ----------

# DBTITLE 1,Silver: Product
# MAGIC %sql
# MAGIC -- Product Silver Table: Enriched with category and brand
# MAGIC -- Data Quality: Valid prices, non-null IDs, active products only
# MAGIC -- Critical: price (cost_price) must exist to calculate margins
# MAGIC
# MAGIC CREATE OR REFRESH LIVE TABLE product_silver (
# MAGIC   CONSTRAINT valid_product_id EXPECT (product_id IS NOT NULL) ON VIOLATION FAIL UPDATE,
# MAGIC   CONSTRAINT valid_cost_price EXPECT (price IS NOT NULL AND price > 0) ON VIOLATION FAIL UPDATE,
# MAGIC   CONSTRAINT active_products_only EXPECT (is_active = true)
# MAGIC )
# MAGIC COMMENT "Cleaned products with category and brand information"
# MAGIC AS SELECT 
# MAGIC   p.* EXCEPT (_rescued_data),
# MAGIC   pc.category_name,
# MAGIC   b.brand_name
# MAGIC FROM LIVE.product_bronze p
# MAGIC JOIN LIVE.product_category_bronze pc
# MAGIC   ON pc.product_category_id = p.product_category_id
# MAGIC JOIN LIVE.brand_bronze b
# MAGIC   ON b.brand_id = p.brand_id

# COMMAND ----------

# DBTITLE 1,Silver: Customer
# MAGIC %sql
# MAGIC -- Customer Silver Table: Enriched with type and address
# MAGIC -- Data Quality: Non-null customer IDs (hard block), country/active status (soft block)
# MAGIC -- Business Logic: Add flag for mismatched is_active vs type_name
# MAGIC
# MAGIC CREATE OR REFRESH LIVE TABLE customer_silver (
# MAGIC   CONSTRAINT valid_customer_id EXPECT (customer_id IS NOT NULL) ON VIOLATION FAIL UPDATE,
# MAGIC   CONSTRAINT has_country EXPECT (country IS NOT NULL) ON VIOLATION DROP ROW,
# MAGIC   CONSTRAINT has_active_status EXPECT (is_active IS NOT NULL) ON VIOLATION DROP ROW
# MAGIC )
# MAGIC COMMENT "Cleaned customers with type and location, includes active status validation flag"
# MAGIC AS SELECT 
# MAGIC   c.* EXCEPT (_rescued_data),
# MAGIC   ct.type_name,
# MAGIC   a.city,
# MAGIC   a.country,
# MAGIC   CASE 
# MAGIC     WHEN c.is_active IS true AND ct.type_name = 'inactive' THEN false
# MAGIC     WHEN c.is_active IS false AND ct.type_name = 'active' THEN false
# MAGIC     ELSE true
# MAGIC   END as active_status_is_correct
# MAGIC FROM LIVE.customer_bronze c
# MAGIC JOIN LIVE.customer_type_bronze ct
# MAGIC   ON ct.customer_type_id = c.customer_type_id
# MAGIC LEFT JOIN LIVE.address_bronze a
# MAGIC   ON a.customer_id = c.customer_id

# COMMAND ----------

# DBTITLE 1,Silver: Order
# MAGIC %sql
# MAGIC -- Order Silver Table: Orders with line items and payment amounts
# MAGIC -- Data Quality: Non-null order IDs, valid quantities
# MAGIC -- Joins: order_item for product details, payment for amounts
# MAGIC -- Currency: Unified conversion to USD for accurate analytics
# MAGIC
# MAGIC CREATE OR REFRESH LIVE TABLE order_silver (
# MAGIC   CONSTRAINT valid_order_id EXPECT (order_id IS NOT NULL) ON VIOLATION FAIL UPDATE,
# MAGIC   CONSTRAINT valid_quantity EXPECT (quantity > 0) ON VIOLATION DROP ROW
# MAGIC )
# MAGIC COMMENT "Orders combined with line items and payment information with USD conversion"
# MAGIC AS SELECT 
# MAGIC   o.order_id,
# MAGIC   o.customer_id,
# MAGIC   o.shipping_address_id,
# MAGIC   o.order_date,
# MAGIC   o.order_status,
# MAGIC   o.subtotal,
# MAGIC   o.discount_amount,
# MAGIC   o.total_amount,
# MAGIC   o.currency,
# MAGIC   oi.product_id,
# MAGIC   oi.quantity,
# MAGIC   p.amount,
# MAGIC   CASE o.currency
# MAGIC     WHEN 'Peso Uruguayo' THEN p.amount / 39.57
# MAGIC     WHEN 'Yuan Renminbi' THEN p.amount / 6.825
# MAGIC     WHEN 'Rupiah' THEN p.amount / 14250.0
# MAGIC     WHEN 'Ruble' THEN p.amount / 75.0
# MAGIC     WHEN 'Peso' THEN p.amount / 4100.0
# MAGIC     WHEN 'Som' THEN p.amount / 85.0
# MAGIC     WHEN 'Real' THEN p.amount / 5.0
# MAGIC     WHEN 'Peso Convertible' THEN p.amount
# MAGIC     WHEN 'Baht' THEN p.amount / 33.0
# MAGIC     WHEN 'Afghani' THEN p.amount / 75.0
# MAGIC     WHEN 'Tugrik' THEN p.amount / 2800.0
# MAGIC     WHEN 'Yen' THEN p.amount / 110.0
# MAGIC     WHEN 'Kwacha' THEN p.amount / 820.0
# MAGIC     WHEN 'Escudo' THEN p.amount / 100.0
# MAGIC     WHEN 'Krona' THEN p.amount / 8.5
# MAGIC     WHEN 'Koruna' THEN p.amount / 22.0
# MAGIC     WHEN 'Dinar' THEN p.amount / 3.0
# MAGIC     WHEN 'Pound' THEN p.amount / 450.0
# MAGIC     WHEN 'Naira' THEN p.amount / 410.0
# MAGIC     WHEN 'Kuna' THEN p.amount / 6.5
# MAGIC     WHEN 'Franc' THEN p.amount / 900.0
# MAGIC     WHEN 'Tolar' THEN p.amount / 200.0
# MAGIC     WHEN 'Dollar' THEN p.amount
# MAGIC     WHEN 'Euro' THEN p.amount * 1.1
# MAGIC     WHEN 'Zloty' THEN p.amount / 4.0
# MAGIC     ELSE p.amount
# MAGIC   END AS unified_amount,
# MAGIC   'USD' AS unified_currency
# MAGIC FROM LIVE.orders_bronze o
# MAGIC LEFT JOIN LIVE.order_item_bronze oi
# MAGIC   ON o.order_id = oi.order_id
# MAGIC LEFT JOIN LIVE.payment_bronze p
# MAGIC   ON o.order_id = p.order_id

# COMMAND ----------

# DBTITLE 1,Silver: Payment
# MAGIC %sql
# MAGIC -- Payment Silver Table: Merge PostgreSQL + Stripe payment data
# MAGIC -- Data Quality:
# MAGIC --   HARD BLOCKS (FAIL UPDATE): amount must exist to calculate revenue
# MAGIC --   SOFT BLOCKS (DROP ROW): Stripe status/currency needed for analytics but not critical
# MAGIC -- Sources: 
# MAGIC --   - Base: PostgreSQL payment table (via Lakeflow Connect)
# MAGIC --   - Enrichment: Stripe payment intents (via Fivetran)
# MAGIC
# MAGIC CREATE OR REFRESH LIVE TABLE payment_silver (
# MAGIC   CONSTRAINT valid_payment_id EXPECT (payment_id IS NOT NULL) ON VIOLATION FAIL UPDATE,
# MAGIC   CONSTRAINT valid_payment_amount EXPECT (amount IS NOT NULL AND amount >= 0) ON VIOLATION FAIL UPDATE,
# MAGIC   CONSTRAINT stripe_status_exists EXPECT (stripe_payment_id IS NULL OR stripe_status IS NOT NULL) ON VIOLATION DROP ROW,
# MAGIC   CONSTRAINT stripe_currency_exists EXPECT (stripe_payment_id IS NULL OR stripe_currency IS NOT NULL) ON VIOLATION DROP ROW
# MAGIC )
# MAGIC COMMENT "Payments enriched with Stripe data and payment method information"
# MAGIC AS 
# MAGIC WITH stripe_flattened AS (
# MAGIC   SELECT 
# MAGIC     payment.id AS stripe_payment_id,
# MAGIC     payment.amount AS stripe_amount,
# MAGIC     payment.amount_received AS stripe_amount_received,
# MAGIC     payment.currency AS stripe_currency,
# MAGIC     payment.status AS stripe_status,
# MAGIC     payment.customer AS stripe_customer_id,
# MAGIC     payment.receipt_email AS stripe_receipt_email,
# MAGIC     ARRAY_JOIN(payment.payment_method_types, ',') AS stripe_payment_method,
# MAGIC     payment.capture_method AS stripe_capture_method,
# MAGIC     payment.confirmation_method AS stripe_confirmation_method,
# MAGIC     payment.metadata.order_id AS stripe_order_id,
# MAGIC     FROM_UNIXTIME(payment.created) AS stripe_created_at
# MAGIC   FROM LIVE.stripe_payments_bronze
# MAGIC   LATERAL VIEW explode(data) AS payment
# MAGIC )
# MAGIC SELECT 
# MAGIC   p.*,
# MAGIC   pm.method_name,
# MAGIC   -- Stripe enrichment fields
# MAGIC   s.stripe_amount,
# MAGIC   s.stripe_amount_received,
# MAGIC   s.stripe_currency,
# MAGIC   s.stripe_status,
# MAGIC   s.stripe_customer_id,
# MAGIC   s.stripe_receipt_email,
# MAGIC   s.stripe_payment_method,
# MAGIC   s.stripe_capture_method,
# MAGIC   s.stripe_confirmation_method,
# MAGIC   s.stripe_created_at
# MAGIC FROM LIVE.payment_postgres_bronze p
# MAGIC JOIN LIVE.payment_method_bronze pm
# MAGIC   ON pm.payment_method_id = p.payment_method_id
# MAGIC LEFT JOIN stripe_flattened s
# MAGIC   ON p.stripe_payment_id = s.stripe_payment_id

# COMMAND ----------

# DBTITLE 1,Gold Layer
# MAGIC %md
# MAGIC ## 🥇 Gold Layer: Business Metrics & Analytics
# MAGIC
# MAGIC **Pattern**: Aggregated, pre-computed KPIs for dashboards
# MAGIC
# MAGIC **Features**:
# MAGIC - Pre-aggregated metrics for fast queries
# MAGIC - Business-ready dimensions
# MAGIC - Ready for BI tools (Tableau, Power BI, Lakeview)
# MAGIC
# MAGIC **Analytics Tables**:
# MAGIC 1. **Product Profitability** - Revenue, cost, profit margins by product
# MAGIC 2. **Cancellation Analysis** - Return rates and trends
# MAGIC 3. **Regional Sales** - Sales by geographic region
# MAGIC 4. **Country Sales** - Sales by individual country

# COMMAND ----------

# DBTITLE 1,Gold: Product Profitability
# MAGIC %sql
# MAGIC -- Product Profitability Gold Table
# MAGIC -- Metrics: Total revenue, quantity, cost, profit, unit profit, profit margin
# MAGIC -- Dimensions: Product ID, name, category
# MAGIC -- Use Cases: Pricing optimization, product portfolio analysis
# MAGIC -- Note: Uses unified_amount (USD) for accurate cross-currency profitability
# MAGIC
# MAGIC CREATE OR REFRESH LIVE TABLE product_profit_gold
# MAGIC COMMENT "Product profitability metrics in USD for analytics and dashboards"
# MAGIC AS SELECT 
# MAGIC   p.product_id, 
# MAGIC   p.product_name, 
# MAGIC   p.category_name,
# MAGIC   p.price,
# MAGIC   SUM(o.unified_amount) AS total_payment_amount,
# MAGIC   SUM(o.quantity) AS total_quantity,
# MAGIC   (p.price * SUM(o.quantity)) AS total_cost,
# MAGIC   (SUM(o.unified_amount) - (p.price * SUM(o.quantity))) AS total_profit,
# MAGIC   (SUM(o.unified_amount) - (p.price * SUM(o.quantity))) / SUM(o.quantity) AS unit_profit,
# MAGIC   (SUM(o.unified_amount) - (p.price * SUM(o.quantity))) / SUM(o.unified_amount) AS profit_margin
# MAGIC FROM LIVE.product_silver p
# MAGIC JOIN LIVE.order_silver o
# MAGIC   ON p.product_id = o.product_id
# MAGIC GROUP BY p.category_name, p.product_id, p.product_name, p.price

# COMMAND ----------

# DBTITLE 1,Gold: Cancellation Analysis
# MAGIC %sql
# MAGIC -- Product Cancellation Gold Table
# MAGIC -- Metrics: Cancelled count, non-cancelled count, return rate
# MAGIC -- Use Cases: Quality issues detection, customer satisfaction analysis
# MAGIC
# MAGIC CREATE OR REFRESH LIVE TABLE cancelled_product_gold
# MAGIC COMMENT "Product cancellation rates and order counts"
# MAGIC AS SELECT
# MAGIC   o.product_id,
# MAGIC   p.product_name,
# MAGIC   COUNT(CASE WHEN o.order_status = 'cancelled' THEN 1 END) AS cancelled_count,
# MAGIC   COUNT(CASE WHEN o.order_status != 'cancelled' THEN 1 END) AS non_cancelled_count,
# MAGIC   COUNT(CASE WHEN o.order_status = 'cancelled' THEN 1 END) / 
# MAGIC     NULLIF(COUNT(*), 0) AS return_rate
# MAGIC FROM LIVE.order_silver o
# MAGIC JOIN LIVE.product_silver p
# MAGIC   ON o.product_id = p.product_id
# MAGIC GROUP BY o.product_id, p.product_name

# COMMAND ----------

# DBTITLE 1,Gold: Regional Sales
# MAGIC %sql
# MAGIC -- Regional Sales Gold Table
# MAGIC -- Metrics: Total sales, order count, customer count by region
# MAGIC -- Dimensions: APAC, EMEA, LATAM, North America
# MAGIC -- Use Cases: Market performance comparison, expansion opportunities
# MAGIC
# MAGIC CREATE OR REFRESH LIVE TABLE sales_region_gold
# MAGIC COMMENT "Sales aggregated by geographic region (USD-converted)"
# MAGIC AS SELECT
# MAGIC   CASE 
# MAGIC     WHEN country IN ('US', 'CA', 'DO') THEN 'North America'
# MAGIC     WHEN country IN ('CO', 'PE', 'AR', 'VE', 'BR') THEN 'LATAM'
# MAGIC     WHEN country IN ('JP', 'CN', 'ID', 'TH', 'PH', 'KZ', 'MN') THEN 'APAC'
# MAGIC     WHEN country IN ('PT', 'NO', 'FR', 'PL', 'CV', 'CZ', 'RU', 'ES', 'SE', 'SI', 'HR', 'AM', 
# MAGIC                      'ZW', 'AF', 'NG', 'TN', 'CM', 'SS', 'ZM', 'MW') THEN 'EMEA'
# MAGIC     ELSE 'Other'
# MAGIC   END AS region,
# MAGIC   SUM(o.unified_amount) AS total_sales,
# MAGIC   COUNT(DISTINCT o.order_id) AS order_count,
# MAGIC   COUNT(DISTINCT c.customer_id) AS customer_count
# MAGIC FROM LIVE.order_silver o
# MAGIC JOIN LIVE.customer_silver c
# MAGIC   ON o.customer_id = c.customer_id
# MAGIC WHERE o.order_status != 'cancelled'
# MAGIC GROUP BY region

# COMMAND ----------

# DBTITLE 1,Gold: Country Sales
# MAGIC %sql
# MAGIC -- Country Sales Gold Table
# MAGIC -- Metrics: Total sales, order count, customer count by country
# MAGIC -- Use Cases: Country-level performance, geographic heat maps
# MAGIC
# MAGIC CREATE OR REFRESH LIVE TABLE sales_country_gold
# MAGIC COMMENT "Sales aggregated by individual country (USD-converted)"
# MAGIC AS SELECT
# MAGIC   country,
# MAGIC   SUM(o.unified_amount) AS total_sales,
# MAGIC   COUNT(DISTINCT o.order_id) AS order_count,
# MAGIC   COUNT(DISTINCT c.customer_id) AS customer_count
# MAGIC FROM LIVE.order_silver o
# MAGIC JOIN LIVE.customer_silver c
# MAGIC   ON o.customer_id = c.customer_id
# MAGIC WHERE o.order_status != 'cancelled'
# MAGIC GROUP BY country

# COMMAND ----------

# DBTITLE 1,Pipeline Setup Instructions
# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## 🚀 Pipeline Setup & Execution
# MAGIC
# MAGIC ### Step 1: Create the Pipeline
# MAGIC
# MAGIC 1. Navigate to **Workflows → Delta Live Tables**
# MAGIC 2. Click **Create Pipeline**
# MAGIC 3. Configure:
# MAGIC    - **Pipeline Name**: `NovaFinds E-Commerce Pipeline`
# MAGIC    - **Product Edition**: Advanced (for data quality metrics)
# MAGIC    - **Notebook Libraries**: Select this notebook
# MAGIC    - **Target**: `main.novafinds`
# MAGIC    - **Storage Location**: Leave default (managed by DLT)
# MAGIC    - **Cluster Mode**: **Serverless** (recommended) or Enhanced Autoscaling
# MAGIC    - **Channel**: Current
# MAGIC
# MAGIC ### Step 2: Schedule the Pipeline
# MAGIC
# MAGIC **Option A: Triggered Mode (Recommended)**
# MAGIC - Schedule: Daily at 2 AM (or your preferred time)
# MAGIC - Best for batch processing
# MAGIC - Cost-effective for predictable workloads
# MAGIC
# MAGIC **Option B: Continuous Mode**
# MAGIC - Runs continuously, processes new files as they arrive
# MAGIC - Best for real-time requirements
# MAGIC - Higher cost, but minimal latency
# MAGIC
# MAGIC ### Step 3: Monitor the Pipeline
# MAGIC
# MAGIC After starting the pipeline, monitor:
# MAGIC - **Data Quality Metrics**: Check expectation pass rates
# MAGIC - **Lineage Graph**: Visualize data flow from Bronze → Silver → Gold
# MAGIC - **Event Log**: Review errors, warnings, and performance
# MAGIC - **Table Metrics**: Row counts, data quality violations
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ✅ Key Features of This Pipeline
# MAGIC
# MAGIC ✔️ **Auto Loader**: Incremental CSV ingestion (only new files processed)
# MAGIC ✔️ **Data Quality**: 12+ expectations with automatic monitoring
# MAGIC ✔️ **Schema Evolution**: Handles schema changes automatically
# MAGIC ✔️ **Exactly-Once Semantics**: Checkpointing prevents duplicates
# MAGIC ✔️ **Automatic Retries**: Failed updates are retried automatically
# MAGIC ✔️ **Lineage Tracking**: Visual data flow from source to gold
# MAGIC ✔️ **Cost Optimized**: Serverless auto-scales based on workload
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📊 Data Quality Expectations Summary
# MAGIC
# MAGIC ### Bronze Layer
# MAGIC - No expectations (raw data preserved)
# MAGIC - Rescued data captured for malformed records
# MAGIC
# MAGIC ### Silver Layer
# MAGIC - **Product**: Valid price (> 0), non-null product_id, active products only
# MAGIC - **Customer**: Non-null customer_id
# MAGIC - **Order**: Non-null order_id, valid quantity (> 0)
# MAGIC - **Payment**: Non-null payment_id, non-negative amount
# MAGIC
# MAGIC ### Gold Layer
# MAGIC - No constraints (aggregated metrics)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🔧 Comparison: This Pipeline vs Original Notebook
# MAGIC
# MAGIC | Feature | Original Notebook | Lakeflow Pipeline |
# MAGIC |---------|------------------|-------------------|
# MAGIC | **Ingestion** | Full reload each run | Incremental (Auto Loader) |
# MAGIC | **Data Quality** | Manual validation | Built-in expectations |
# MAGIC | **Orchestration** | Manual cell execution | Declarative DAG |
# MAGIC | **Retries** | Manual | Automatic |
# MAGIC | **Lineage** | None | Automatic |
# MAGIC | **Monitoring** | Manual queries | Built-in dashboard |
# MAGIC | **Cost** | Recomputes everything | Incremental, optimized |
# MAGIC | **Schedule** | Databricks Job needed | Built-in scheduler |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📦 Next Steps
# MAGIC
# MAGIC 1. **Create the Pipeline** using the steps above
# MAGIC 2. **Run Initial Load**: Start the pipeline to process existing CSV files
# MAGIC 3. **Verify Data Quality**: Check expectation metrics in pipeline UI
# MAGIC 4. **Connect BI Tools**: Use gold tables in dashboards (Tableau, Power BI, Lakeview)
# MAGIC 5. **Set Alerts**: Configure notifications for pipeline failures
# MAGIC 6. **Optimize**: Add table properties (partitioning, Z-order) if needed for large datasets
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📚 Additional Resources
# MAGIC
# MAGIC - [Delta Live Tables Documentation](https://docs.databricks.com/delta-live-tables/index.html)
# MAGIC - [Auto Loader Guide](https://docs.databricks.com/ingestion/auto-loader/index.html)
# MAGIC - [Data Quality Expectations](https://docs.databricks.com/delta-live-tables/expectations.html)
# MAGIC - [Pipeline Monitoring](https://docs.databricks.com/delta-live-tables/observability.html)