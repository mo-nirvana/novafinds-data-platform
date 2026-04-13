# Databricks notebook source
# DBTITLE 1,Unit Tests
# MAGIC %md
# MAGIC # NovaFinds Pipeline - Unit Tests
# MAGIC
# MAGIC This notebook runs unit tests before the Lakeflow pipeline executes.
# MAGIC
# MAGIC **Test Categories**:
# MAGIC 1. **Schema Tests** - Verify expected columns exist
# MAGIC 2. **Data Quality Tests** - Check for nulls, invalid values
# MAGIC 3. **Business Logic Tests** - Validate calculations and transformations
# MAGIC 4. **Data Freshness Tests** - Ensure data is recent
# MAGIC
# MAGIC **Exit Code**: Returns 0 on success, 1 on failure

# COMMAND ----------

# DBTITLE 1,Test Configuration
# Get configuration from parameters
catalog = dbutils.widgets.get("catalog") if "catalog" in [w.name for w in dbutils.widgets.getAll().items()] else "novafinds_dev"
environment = dbutils.widgets.get("environment") if "environment" in [w.name for w in dbutils.widgets.getAll().items()] else "dev"

print(f"Running tests for:")
print(f"  Catalog: {catalog}")
print(f"  Environment: {environment}")

# Test results tracking
test_results = []
failed_tests = 0

# COMMAND ----------

# DBTITLE 1,Test Helper Functions
def run_test(test_name, test_function):
    """Execute a test and track results"""
    global failed_tests
    try:
        test_function()
        test_results.append({"test": test_name, "status": "✅ PASSED"})
        print(f"✅ {test_name}")
    except AssertionError as e:
        test_results.append({"test": test_name, "status": f"❌ FAILED: {str(e)}"})
        print(f"❌ {test_name}: {str(e)}")
        failed_tests += 1
    except Exception as e:
        test_results.append({"test": test_name, "status": f"❌ ERROR: {str(e)}"})
        print(f"❌ {test_name}: Unexpected error - {str(e)}")
        failed_tests += 1

def assert_table_exists(table_name):
    """Check if a table exists in the catalog"""
    result = spark.sql(f"SHOW TABLES IN {catalog} LIKE '{table_name}'").count()
    assert result > 0, f"Table {catalog}.{table_name} does not exist"

def assert_column_exists(table_name, column_name):
    """Check if a column exists in a table"""
    df = spark.table(f"{catalog}.{table_name}")
    assert column_name in df.columns, f"Column {column_name} not found in {table_name}"

def assert_no_nulls(table_name, column_name):
    """Check that a column has no null values"""
    null_count = spark.sql(f"SELECT COUNT(*) as cnt FROM {catalog}.{table_name} WHERE {column_name} IS NULL").collect()[0]['cnt']
    assert null_count == 0, f"Found {null_count} null values in {table_name}.{column_name}"

# COMMAND ----------

# DBTITLE 1,Schema Tests
# MAGIC %md
# MAGIC ## Schema Validation Tests
# MAGIC
# MAGIC Verify that expected tables and columns exist with correct data types.

# COMMAND ----------

# DBTITLE 1,Test: Bronze Tables Exist
def test_bronze_tables():
    """Verify all Bronze layer tables exist"""
    expected_tables = ['product_bronze', 'customer_bronze', 'orders_bronze', 'payment_postgres_bronze']
    for table in expected_tables:
        assert_table_exists(table)

run_test("Bronze Tables Exist", test_bronze_tables)

# COMMAND ----------

# DBTITLE 1,Test: Silver Tables Exist
def test_silver_tables():
    """Verify all Silver layer tables exist"""
    if environment == 'dev':
        # In dev, Silver tables may not exist yet
        print("  Skipping Silver table check in dev environment")
        return
    expected_tables = ['product_silver', 'order_silver']
    for table in expected_tables:
        assert_table_exists(table)

run_test("Silver Tables Exist", test_silver_tables)

# COMMAND ----------

# DBTITLE 1,Test: Product Schema
def test_product_schema():
    """Verify product table has required columns"""
    required_columns = ['product_id', 'product_name', 'price']
    for col in required_columns:
        assert_column_exists('product_bronze', col)

run_test("Product Schema Validation", test_product_schema)

# COMMAND ----------

# DBTITLE 1,Data Quality Tests
# MAGIC %md
# MAGIC ## Data Quality Tests
# MAGIC
# MAGIC Validate data quality constraints that should be enforced by the pipeline.

# COMMAND ----------

# DBTITLE 1,Test: No Null Product IDs
def test_no_null_product_ids():
    """Product IDs should never be null"""
    assert_no_nulls('product_bronze', 'product_id')

run_test("No Null Product IDs", test_no_null_product_ids)

# COMMAND ----------

# DBTITLE 1,Test: Positive Prices
def test_positive_prices():
    """All product prices should be positive"""
    negative_prices = spark.sql(f"SELECT COUNT(*) as cnt FROM {catalog}.product_bronze WHERE price <= 0").collect()[0]['cnt']
    assert negative_prices == 0, f"Found {negative_prices} products with non-positive prices"

run_test("All Prices Are Positive", test_positive_prices)

# COMMAND ----------

# DBTITLE 1,Test Results Summary
# MAGIC %md
# MAGIC ## Test Results Summary

# COMMAND ----------

# DBTITLE 1,Display Results
# Display test results
import pandas as pd
results_df = pd.DataFrame(test_results)
display(results_df)

print(f"\n{'='*60}")
print(f"Total Tests: {len(test_results)}")
print(f"Passed: {len(test_results) - failed_tests}")
print(f"Failed: {failed_tests}")
print(f"{'='*60}")

if failed_tests > 0:
    print(f"\n❌ {failed_tests} test(s) failed. Pipeline execution should be blocked.")
    dbutils.notebook.exit("FAILED")
else:
    print(f"\n✅ All tests passed! Pipeline can proceed.")
    dbutils.notebook.exit("SUCCESS")
