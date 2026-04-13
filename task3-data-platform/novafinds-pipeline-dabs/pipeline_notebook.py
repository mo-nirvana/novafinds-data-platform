# Databricks notebook source
# DBTITLE 1,NovaFinds Lakeflow Pipeline
# MAGIC %md
# MAGIC # NovaFinds E-Commerce Data Pipeline
# MAGIC ## Lakeflow Declarative Pipeline (Bronze → Silver → Gold)
# MAGIC
# MAGIC This notebook defines the complete medallion architecture for NovaFinds using Delta Live Tables.
# MAGIC
# MAGIC **Environment Configuration**: Catalog and data source paths are passed via pipeline configuration parameters.

# COMMAND ----------

# DBTITLE 1,Configuration
# Get configuration from pipeline parameters
catalog = spark.conf.get("catalog", "novafinds_dev")
data_source = spark.conf.get("data_source", "/Volumes/novafinds_dev/raw/fixtures")
environment = spark.conf.get("environment", "dev")

print(f"Pipeline Configuration:")
print(f"  Catalog: {catalog}")
print(f"  Data Source: {data_source}")
print(f"  Environment: {environment}")

# COMMAND ----------

# DBTITLE 1,Bronze Layer
# MAGIC %md
# MAGIC ## 🥉 Bronze Layer: Raw Data Ingestion
# MAGIC
# MAGIC **Sources**:
# MAGIC - PostgreSQL via Lakeflow Connect (CDC)
# MAGIC - Stripe via Fivetran
# MAGIC
# MAGIC **Features**: Schema evolution, rescued data capture

# COMMAND ----------

# DBTITLE 1,Bronze: Product
# MAGIC %sql
# MAGIC CREATE OR REFRESH STREAMING LIVE TABLE product_bronze
# MAGIC COMMENT "Product data from PostgreSQL via Lakeflow Connect CDC"
# MAGIC AS SELECT * FROM cloud_files(
# MAGIC   '${data_source}/product',
# MAGIC   'delta',
# MAGIC   map('cloudFiles.schemaEvolutionMode', 'addNewColumns')
# MAGIC )

# COMMAND ----------

# DBTITLE 1,Bronze: Customer
# MAGIC %sql
# MAGIC CREATE OR REFRESH STREAMING LIVE TABLE customer_bronze
# MAGIC COMMENT "Customer data from PostgreSQL via Lakeflow Connect CDC"
# MAGIC AS SELECT * FROM cloud_files(
# MAGIC   '${data_source}/customer',
# MAGIC   'delta',
# MAGIC   map('cloudFiles.schemaEvolutionMode', 'addNewColumns')
# MAGIC )

# COMMAND ----------

# DBTITLE 1,Bronze: Orders
# MAGIC %sql
# MAGIC CREATE OR REFRESH STREAMING LIVE TABLE orders_bronze
# MAGIC COMMENT "Orders data from PostgreSQL via Lakeflow Connect CDC"
# MAGIC AS SELECT * FROM cloud_files(
# MAGIC   '${data_source}/orders',
# MAGIC   'delta',
# MAGIC   map('cloudFiles.schemaEvolutionMode', 'addNewColumns')
# MAGIC )

# COMMAND ----------

# DBTITLE 1,Bronze: Payment
# MAGIC %sql
# MAGIC CREATE OR REFRESH STREAMING LIVE TABLE payment_postgres_bronze
# MAGIC COMMENT "Payment data from PostgreSQL via Lakeflow Connect CDC"
# MAGIC AS SELECT * FROM cloud_files(
# MAGIC   '${data_source}/payment',
# MAGIC   'delta',
# MAGIC   map('cloudFiles.schemaEvolutionMode', 'addNewColumns')
# MAGIC )

# COMMAND ----------

# DBTITLE 1,Silver Layer
# MAGIC %md
# MAGIC ## 🥈 Silver Layer: Cleaned & Enriched
# MAGIC
# MAGIC **Features**: Dimensional joins, data quality expectations, USD currency normalization

# COMMAND ----------

# DBTITLE 1,Silver: Product
# MAGIC %sql
# MAGIC CREATE OR REFRESH LIVE TABLE product_silver (
# MAGIC   CONSTRAINT valid_product_id EXPECT (product_id IS NOT NULL) ON VIOLATION FAIL UPDATE,
# MAGIC   CONSTRAINT valid_cost_price EXPECT (price IS NOT NULL AND price > 0) ON VIOLATION FAIL UPDATE
# MAGIC )
# MAGIC COMMENT "Cleaned products with validation"
# MAGIC AS SELECT 
# MAGIC   product_id,
# MAGIC   product_name,
# MAGIC   price,
# MAGIC   category_name,
# MAGIC   brand_name
# MAGIC FROM LIVE.product_bronze

# COMMAND ----------

# DBTITLE 1,Silver: Order
# MAGIC %sql
# MAGIC CREATE OR REFRESH LIVE TABLE order_silver (
# MAGIC   CONSTRAINT valid_order_id EXPECT (order_id IS NOT NULL) ON VIOLATION FAIL UPDATE,
# MAGIC   CONSTRAINT valid_quantity EXPECT (quantity > 0) ON VIOLATION DROP ROW
# MAGIC )
# MAGIC COMMENT "Orders with USD conversion"
# MAGIC AS SELECT 
# MAGIC   o.order_id,
# MAGIC   o.customer_id,
# MAGIC   o.order_date,
# MAGIC   o.order_status,
# MAGIC   o.quantity,
# MAGIC   o.product_id,
# MAGIC   CASE o.currency
# MAGIC     WHEN 'Yuan Renminbi' THEN p.amount / 6.825
# MAGIC     WHEN 'Euro' THEN p.amount * 1.1
# MAGIC     WHEN 'Pound' THEN p.amount / 450.0
# MAGIC     WHEN 'Yen' THEN p.amount / 110.0
# MAGIC     ELSE p.amount
# MAGIC   END AS unified_amount,
# MAGIC   'USD' AS unified_currency
# MAGIC FROM LIVE.orders_bronze o
# MAGIC LEFT JOIN LIVE.payment_postgres_bronze p
# MAGIC   ON o.order_id = p.order_id

# COMMAND ----------

# DBTITLE 1,Gold Layer
# MAGIC %md
# MAGIC ## 🥇 Gold Layer: Business Analytics
# MAGIC
# MAGIC **Tables**: Product profitability, regional sales, cancellation analysis

# COMMAND ----------

# DBTITLE 1,Gold: Product Profitability
# MAGIC %sql
# MAGIC CREATE OR REFRESH LIVE TABLE product_profit_gold
# MAGIC COMMENT "Product profitability metrics in USD"
# MAGIC AS SELECT 
# MAGIC   p.product_id, 
# MAGIC   p.product_name, 
# MAGIC   p.category_name,
# MAGIC   SUM(o.unified_amount) AS total_payment_amount,
# MAGIC   SUM(o.quantity) AS total_quantity,
# MAGIC   (p.price * SUM(o.quantity)) AS total_cost,
# MAGIC   (SUM(o.unified_amount) - (p.price * SUM(o.quantity))) AS total_profit,
# MAGIC   (SUM(o.unified_amount) - (p.price * SUM(o.quantity))) / SUM(o.unified_amount) AS profit_margin
# MAGIC FROM LIVE.product_silver p
# MAGIC JOIN LIVE.order_silver o
# MAGIC   ON p.product_id = o.product_id
# MAGIC GROUP BY p.category_name, p.product_id, p.product_name, p.price