"""
NovaFinds Data Pipeline - PostgreSQL Version
Medallion Architecture (Bronze -> Silver -> Gold)

Extension for simple_stripe_loader.py
Creates analytics tables from existing PostgreSQL data
"""

import psycopg2
import pandas as pd

DB_CONFIG = {
    "host": "localhost",
    "port": 5431,
    "database": "novafinds_db",
    "user": "novafinds_user",
    "password": "novafinds_password"
}

def connect_db():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except Exception as e:
        print(f"ERROR: Could not connect - {e}")
        return None

def run_query(conn, query):
    try:
        cur = conn.cursor()
        cur.execute(query)
        conn.commit()
        cur.close()
        return True
    except Exception as e:
        print(f"ERROR: {str(e)[:100]}")
        conn.rollback()
        return False

def query_to_dataframe(conn, query):
    try:
        return pd.read_sql(query, conn)
    except Exception as e:
        print(f"ERROR: {e}")
        return None

print("\n" + "="*70)
print("BRONZE LAYER - Raw Tables (Already in PostgreSQL)")
print("="*70)

conn = connect_db()
if not conn:
    exit(1)

print("\nChecking existing tables...")
existing_tables = query_to_dataframe(conn, """
    SELECT table_name 
    FROM information_schema.tables 
    WHERE table_schema = 'public'
    ORDER BY table_name
""")

print(f"Found {len(existing_tables)} tables:")
for table in existing_tables['table_name']:
    row_count = query_to_dataframe(conn, f"SELECT COUNT(*) as cnt FROM {table}")
    print(f"  [OK] {table:<25} {row_count['cnt'][0]:>6} rows")

print("\n" + "="*70)
print("SILVER LAYER - Cleaned & Enriched Tables")
print("="*70)

# 1. Product Silver
print("\n[1] Creating product_silver...")
sql_product_silver = """
DROP TABLE IF EXISTS product_silver;
CREATE TABLE product_silver AS
SELECT 
  p.product_id,
  p.product_name,
  p.price,
  p.is_active,
  pc.category_name,
  b.brand_name
FROM product p
JOIN product_category pc ON pc.product_category_id = p.product_category_id
JOIN brand b ON b.brand_id = p.brand_id
WHERE p.is_active = true;
"""
if run_query(conn, sql_product_silver):
    count = query_to_dataframe(conn, "SELECT COUNT(*) as cnt FROM product_silver")['cnt'][0]
    print(f"   [OK] Created product_silver: {count} rows")

# 2. Customer Silver
print("\n[2] Creating customer_silver...")
sql_customer_silver = """
DROP TABLE IF EXISTS customer_silver;
CREATE TABLE customer_silver AS
SELECT 
  c.customer_id,
  CONCAT(c.first_name, ' ', c.last_name) as customer_name,
  c.email,
  c.is_active,
  ct.type_name,
  a.city,
  a.country,
  CASE 
    WHEN c.is_active IS true AND ct.type_name = 'inactive' THEN false
    WHEN c.is_active IS false AND ct.type_name = 'active' THEN false
    ELSE true
  END as active_status_is_correct
FROM customer c
JOIN customer_type ct ON ct.customer_type_id = c.customer_type_id
LEFT JOIN address a ON a.customer_id = c.customer_id;
"""
if run_query(conn, sql_customer_silver):
    count = query_to_dataframe(conn, "SELECT COUNT(*) as cnt FROM customer_silver")['cnt'][0]
    print(f"   [OK] Created customer_silver: {count} rows")

# 3. Payment Silver
print("\n[3] Creating payment_silver...")
sql_payment_silver = """
DROP TABLE IF EXISTS payment_silver;
CREATE TABLE payment_silver AS
SELECT 
  p.payment_id,
  p.amount,
  p.payment_date,
  p.order_id,
  pm.method_name,
  p.stripe_payment_id,
  p.stripe_status,
  p.stripe_currency
FROM payment p
JOIN payment_method pm ON pm.payment_method_id = p.payment_method_id;
"""
if run_query(conn, sql_payment_silver):
    count = query_to_dataframe(conn, "SELECT COUNT(*) as cnt FROM payment_silver")['cnt'][0]
    print(f"   [OK] Created payment_silver: {count} rows")

# 4. Order Silver
print("\n[4] Creating order_silver...")
sql_order_silver = """
DROP TABLE IF EXISTS order_silver;
CREATE TABLE order_silver AS
SELECT 
  o.order_id,
  o.customer_id,
  o.order_date,
  o.total_amount,
  o.currency,
  o.order_status,
  o.discount_amount,
  oi.order_item_id,
  oi.product_id,
  oi.quantity
FROM orders o
LEFT JOIN order_item oi ON o.order_id = oi.order_id;
"""
if run_query(conn, sql_order_silver):
    count = query_to_dataframe(conn, "SELECT COUNT(*) as cnt FROM order_silver")['cnt'][0]
    print(f"   [OK] Created order_silver: {count} rows")

print("\n" + "="*70)
print("GOLD LAYER - Business Analytics & Metrics")
print("="*70)

# 1. Product Revenue Analysis
print("\n[1] Creating product_revenue_gold...")
sql_product_revenue = """
DROP TABLE IF EXISTS product_revenue_gold;
CREATE TABLE product_revenue_gold AS
SELECT 
  p.product_id,
  p.product_name,
  p.category_name,
  p.price,
  COUNT(DISTINCT os.order_id) as total_orders,
  COALESCE(SUM(os.quantity), 0) as total_quantity,
  COALESCE(SUM(os.quantity * p.price), 0) as total_cost,
  COALESCE(SUM(os.total_amount), 0) as total_revenue
FROM product_silver p
LEFT JOIN order_silver os ON p.product_id = os.product_id
GROUP BY p.product_id, p.product_name, p.category_name, p.price;
"""
if run_query(conn, sql_product_revenue):
    count = query_to_dataframe(conn, "SELECT COUNT(*) as cnt FROM product_revenue_gold")['cnt'][0]
    print(f"   [OK] Created product_revenue_gold: {count} rows")

# 2. Product Cancellation Analysis
print("\n[2] Creating cancelled_product_gold...")
sql_cancelled_product = """
DROP TABLE IF EXISTS cancelled_product_gold;
CREATE TABLE cancelled_product_gold AS
SELECT
  os.product_id,
  ps.product_name,
  COUNT(CASE WHEN os.order_status = 'cancelled' THEN 1 END) as cancelled_count,
  COUNT(CASE WHEN os.order_status != 'cancelled' THEN 1 END) as non_cancelled_count,
  CAST(COUNT(CASE WHEN os.order_status = 'cancelled' THEN 1 END) AS FLOAT) / 
    NULLIF(COUNT(*), 0) as return_rate
FROM order_silver os
JOIN product_silver ps ON os.product_id = ps.product_id
GROUP BY os.product_id, ps.product_name;
"""
if run_query(conn, sql_cancelled_product):
    count = query_to_dataframe(conn, "SELECT COUNT(*) as cnt FROM cancelled_product_gold")['cnt'][0]
    print(f"   [OK] Created cancelled_product_gold: {count} rows")

# 3. Sales by Region
print("\n[3] Creating sales_region_gold...")
sql_sales_region = """
DROP TABLE IF EXISTS sales_region_gold;
CREATE TABLE sales_region_gold AS
SELECT
  CASE 
    WHEN COALESCE(cs.country, '') IN ('US', 'CA', 'DO') THEN 'North America'
    WHEN COALESCE(cs.country, '') IN ('CO', 'PE', 'AR', 'VE', 'BR') THEN 'LATAM'
    WHEN COALESCE(cs.country, '') IN ('JP', 'CN', 'ID', 'TH', 'PH', 'KZ', 'MN') THEN 'APAC'
    WHEN COALESCE(cs.country, '') IN ('PT', 'NO', 'FR', 'PL', 'CV', 'CZ', 'RU', 'ES', 'SE', 'SI', 'HR', 'AM') 
         THEN 'EMEA'
    ELSE 'Other'
  END as region,
  SUM(os.total_amount) as total_sales,
  COUNT(DISTINCT os.order_id) as order_count,
  COUNT(DISTINCT cs.customer_id) as customer_count
FROM order_silver os
JOIN customer_silver cs ON os.customer_id = cs.customer_id
WHERE os.order_status != 'cancelled'
GROUP BY region;
"""
if run_query(conn, sql_sales_region):
    count = query_to_dataframe(conn, "SELECT COUNT(*) as cnt FROM sales_region_gold")['cnt'][0]
    print(f"   [OK] Created sales_region_gold: {count} rows")

# 4. Sales by Country
print("\n[4] Creating sales_country_gold...")
sql_sales_country = """
DROP TABLE IF EXISTS sales_country_gold;
CREATE TABLE sales_country_gold AS
SELECT
  cs.country,
  SUM(os.total_amount) as total_sales,
  COUNT(DISTINCT os.order_id) as order_count,
  COUNT(DISTINCT cs.customer_id) as customer_count
FROM order_silver os
JOIN customer_silver cs ON os.customer_id = cs.customer_id
WHERE os.order_status != 'cancelled'
GROUP BY cs.country;
"""
if run_query(conn, sql_sales_country):
    count = query_to_dataframe(conn, "SELECT COUNT(*) as cnt FROM sales_country_gold WHERE country IS NOT NULL")['cnt'][0]
    print(f"   [OK] Created sales_country_gold: {count} rows")

print("\n" + "="*70)
print("DATA QUALITY CHECKS")
print("="*70)

print("\n[1] Geographic Coverage:")
countries = query_to_dataframe(conn, 
    "SELECT DISTINCT country FROM sales_country_gold WHERE country IS NOT NULL ORDER BY country")
if countries is not None:
    print(f"    Total countries: {len(countries)}")

print("\n[2] Date Range:")
date_info = query_to_dataframe(conn, """
    SELECT 
        MIN(order_date) as oldest_date,
        MAX(order_date) as newest_date
    FROM orders
""")
if date_info is not None:
    print(f"    Oldest order: {date_info['oldest_date'][0]}")
    print(f"    Newest order: {date_info['newest_date'][0]}")

print("\n[3] Data Completeness:")
missing = query_to_dataframe(conn, 
    "SELECT COUNT(*) as cnt FROM customer_silver WHERE country IS NULL")
total = query_to_dataframe(conn, "SELECT COUNT(*) as cnt FROM customer_silver")
if missing is not None and total is not None:
    missing_pct = (missing['cnt'][0] / total['cnt'][0] * 100) if total['cnt'][0] > 0 else 0
    print(f"    Customers with missing country: {missing['cnt'][0]} ({missing_pct:.1f}%)")

print("\n[4] Stripe Integration:")
stripe_loaded = query_to_dataframe(conn, 
    "SELECT COUNT(*) as cnt FROM payment WHERE stripe_payment_id IS NOT NULL")
if stripe_loaded is not None:
    print(f"    Stripe payments loaded: {stripe_loaded['cnt'][0]}")

print("\n" + "="*70)
print("ANALYTICS SUMMARY")
print("="*70)

print("\n[RESULTS] Top 5 Products by Revenue:")
top_products = query_to_dataframe(conn, """
    SELECT product_name, total_revenue, total_quantity
    FROM product_revenue_gold
    ORDER BY total_revenue DESC
    LIMIT 5
""")
if top_products is not None:
    for idx, row in top_products.iterrows():
        qty = int(row['total_quantity']) if row['total_quantity'] else 0
        print(f"    {idx+1}. {row['product_name']:<30} ${row['total_revenue']:>10.2f} ({qty} units)")

print("\n[RESULTS] Sales by Region:")
regions = query_to_dataframe(conn, 
    "SELECT region, total_sales, order_count FROM sales_region_gold ORDER BY total_sales DESC")
if regions is not None:
    for idx, row in regions.iterrows():
        print(f"    {row['region']:<20} ${row['total_sales']:>12.2f} ({int(row['order_count'])} orders)")

print("\n[RESULTS] Top 5 Products by Cancellation Rate:")
high_cancel = query_to_dataframe(conn, """
    SELECT product_name, return_rate, cancelled_count, non_cancelled_count
    FROM cancelled_product_gold
    WHERE (cancelled_count + non_cancelled_count) >= 5
    ORDER BY return_rate DESC
    LIMIT 5
""")
if high_cancel is not None:
    for idx, row in high_cancel.iterrows():
        print(f"    {idx+1}. {row['product_name']:<30} {row['return_rate']*100:>6.1f}% " +
              f"(cancelled: {int(row['cancelled_count'])}, successful: {int(row['non_cancelled_count'])})")

print("\n" + "="*70)
print("PIPELINE COMPLETE!")
print("="*70)
print("""
[OK] Medallion Architecture Created:
    - BRONZE LAYER: 10 raw tables
    - SILVER LAYER: 4 cleaned & enriched tables
    - GOLD LAYER: 4 analytics tables

[READY] You can now:
    1. Query gold layer tables for dashboards
    2. Run: python export_all_tables.py (to export analytics)
    3. Build visualizations from gold tables
    4. Monitor data quality

Next steps:
    - Connect gold tables to BI tools (Tableau, Power BI)
    - Schedule this script to run daily
    - Build custom dashboards from gold layer
""")

conn.close()
print("[OK] Done!")
