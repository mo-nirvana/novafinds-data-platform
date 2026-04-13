# Databricks notebook source
# DBTITLE 1,NovaFinds Analytics Dashboard
# MAGIC %md
# MAGIC # NovaFinds E-Commerce Analytics Dashboard
# MAGIC ## Data Visualization Deliverable
# MAGIC
# MAGIC This notebook generates business visualizations from the Gold layer tables.
# MAGIC
# MAGIC **Charts**:
# MAGIC 1. Top 10 Profitable Products
# MAGIC 2. Sales by Region
# MAGIC 3. Product Profitability Distribution
# MAGIC 4. Payment Trends (if Stripe data available)
# MAGIC 5. Order Status Distribution

# COMMAND ----------

# DBTITLE 1,Configuration
# Get configuration from parameters
catalog = dbutils.widgets.get("catalog") if "catalog" in [w.name for w in dbutils.widgets.getAll().items()] else "novafinds_dev"
target_schema = dbutils.widgets.get("target_schema") if "target_schema" in [w.name for w in dbutils.widgets.getAll().items()] else f"{catalog}_dlt"
environment = dbutils.widgets.get("environment") if "environment" in [w.name for w in dbutils.widgets.getAll().items()] else "dev"

print(f"Dashboard Configuration:")
print(f"  Catalog: {catalog}")
print(f"  Target Schema: {target_schema}")
print(f"  Environment: {environment}")

import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd

sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (12, 6)

# COMMAND ----------

# DBTITLE 1,Load Data
# MAGIC %md
# MAGIC ## Load Gold Layer Tables

# COMMAND ----------

# DBTITLE 1,Query Gold Tables
# Load gold layer data
try:
    product_profit_df = spark.sql(f"""
        SELECT * FROM {target_schema}.product_profit_gold
        ORDER BY total_profit DESC
        LIMIT 20
    """).toPandas()
    
    print(f"✅ Loaded product profitability data: {len(product_profit_df)} rows")
except Exception as e:
    print(f"❌ Error loading product_profit_gold: {str(e)}")
    product_profit_df = pd.DataFrame()

try:
    regional_sales_df = spark.sql(f"""
        SELECT * FROM {target_schema}.sales_region_gold
        ORDER BY total_sales DESC
    """).toPandas()
    
    print(f"✅ Loaded regional sales data: {len(regional_sales_df)} rows")
except Exception as e:
    print(f"❌ Error loading sales_region_gold: {str(e)}")
    regional_sales_df = pd.DataFrame()

# COMMAND ----------

# DBTITLE 1,Chart 1: Top Products
# MAGIC %md
# MAGIC ## Chart 1: Top 10 Profitable Products

# COMMAND ----------

# DBTITLE 1,Top Products Bar Chart
if not product_profit_df.empty:
    fig, ax = plt.subplots(figsize=(14, 8))
    
    top_10 = product_profit_df.head(10)
    bars = ax.barh(top_10['product_name'], top_10['total_profit'], color='#1f77b4')
    
    ax.set_xlabel('Total Profit (USD)', fontsize=12)
    ax.set_ylabel('Product Name', fontsize=12)
    ax.set_title('Top 10 Profitable Products', fontsize=14, fontweight='bold')
    ax.invert_yaxis()
    
    # Add value labels
    for i, bar in enumerate(bars):
        width = bar.get_width()
        ax.text(width, bar.get_y() + bar.get_height()/2, 
                f'${width:,.0f}', 
                ha='left', va='center', fontsize=10)
    
    plt.tight_layout()
    plt.show()
    
    print(f"✅ Generated Top 10 Profitable Products chart")
else:
    print("❌ No product profit data available")

# COMMAND ----------

# DBTITLE 1,Chart 2: Regional Sales
# MAGIC %md
# MAGIC ## Chart 2: Sales by Geographic Region

# COMMAND ----------

# DBTITLE 1,Regional Sales Chart
if not regional_sales_df.empty:
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))
    
    # Bar chart
    ax1.bar(regional_sales_df['region'], regional_sales_df['total_sales'], color='#2ca02c')
    ax1.set_xlabel('Region', fontsize=12)
    ax1.set_ylabel('Total Sales (USD)', fontsize=12)
    ax1.set_title('Sales by Region', fontsize=14, fontweight='bold')
    ax1.tick_params(axis='x', rotation=45)
    
    # Pie chart
    colors = ['#ff7f0e', '#2ca02c', '#d62728', '#9467bd', '#8c564b']
    ax2.pie(regional_sales_df['total_sales'], 
            labels=regional_sales_df['region'], 
            autopct='%1.1f%%',
            colors=colors,
            startangle=90)
    ax2.set_title('Sales Distribution by Region', fontsize=14, fontweight='bold')
    
    plt.tight_layout()
    plt.show()
    
    print(f"✅ Generated Regional Sales charts")
else:
    print("❌ No regional sales data available")

# COMMAND ----------

# DBTITLE 1,Chart 3: Profit Margins
# MAGIC %md
# MAGIC ## Chart 3: Product Profitability Distribution

# COMMAND ----------

# DBTITLE 1,Profit Margin Distribution
if not product_profit_df.empty:
    fig, ax = plt.subplots(figsize=(12, 6))
    
    ax.scatter(product_profit_df['total_quantity'], 
               product_profit_df['total_profit'],
               s=product_profit_df['profit_margin']*500,
               alpha=0.6,
               c=product_profit_df['profit_margin'],
               cmap='viridis')
    
    ax.set_xlabel('Total Quantity Sold', fontsize=12)
    ax.set_ylabel('Total Profit (USD)', fontsize=12)
    ax.set_title('Product Profitability: Quantity vs Profit (Bubble size = Margin)', 
                 fontsize=14, fontweight='bold')
    
    cbar = plt.colorbar(ax.collections[0], ax=ax)
    cbar.set_label('Profit Margin', fontsize=10)
    
    plt.tight_layout()
    plt.show()
    
    print(f"✅ Generated Profitability Distribution chart")
else:
    print("❌ No product profit data available")

# COMMAND ----------

# DBTITLE 1,Summary Statistics
# MAGIC %md
# MAGIC ## Summary Statistics

# COMMAND ----------

# DBTITLE 1,Display Summary
print("\n" + "="*60)
print("NOVAFINDS E-COMMERCE ANALYTICS SUMMARY")
print("="*60)

if not product_profit_df.empty:
    print(f"\nProduct Metrics:")
    print(f"  Total Products Analyzed: {len(product_profit_df)}")
    print(f"  Total Profit: ${product_profit_df['total_profit'].sum():,.2f}")
    print(f"  Average Profit Margin: {product_profit_df['profit_margin'].mean()*100:.1f}%")
    print(f"  Top Product: {product_profit_df.iloc[0]['product_name']}")

if not regional_sales_df.empty:
    print(f"\nRegional Metrics:")
    print(f"  Total Regions: {len(regional_sales_df)}")
    print(f"  Total Sales: ${regional_sales_df['total_sales'].sum():,.2f}")
    print(f"  Top Region: {regional_sales_df.iloc[0]['region']} (${regional_sales_df.iloc[0]['total_sales']:,.2f})")

print("\n" + "="*60)
print("✅ Dashboard generation complete!")
print("="*60)
